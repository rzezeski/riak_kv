-module(entropy_manager).
-compile(export_all).
-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-type index() :: non_neg_integer().
-type index_n() :: {index(), pos_integer()}.

-record(state, {trees,
                tree_queue,
                locks,
                exchange_queue :: [{index(), index(), index_n()}],
                exchanges}).

-define(CONCURRENCY, 2).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_lock(Type) ->
    get_lock(Type, self()).

get_lock(Type, Pid) ->
    gen_server:call(?MODULE, {get_lock, Type, Pid}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    schedule_tick(),
    State = #state{trees=[],
                   tree_queue=[],
                   locks=[],
                   exchanges=[],
                   exchange_queue=[]},
    {ok, State}.

handle_call({get_lock, Type, Pid}, _From, State) ->
    {Reply, State2} = do_get_lock(Type, Pid, State),
    {reply, Reply, State2};
handle_call(Request, From, State) ->
    lager:warning("Unexpected message: ~p from ~p", [Request, From]),
    {reply, ok, State}.

handle_cast({requeue_poke, Index}, State) ->
    State2 = requeue_poke(Index, State),
    {noreply, State2};
handle_cast({exchange_status, Pid, LocalVN, RemoteVN, IndexN, Reply}, State) ->
    State2 = do_exchange_status(Pid, LocalVN, RemoteVN, IndexN, Reply, State),
    {noreply, State2};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    State2 = maybe_reload_hashtrees(State),
    State3 = tick(State2),
    {noreply, State3};
handle_info({'DOWN', Ref, _, Obj, _}, State) ->
    State2 = maybe_release_lock(Ref, State),
    State3 = maybe_clear_exchange(Ref, State2),
    State4 = maybe_clear_registered_tree(Obj, State3),
    {noreply, State4};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

maybe_reload_hashtrees(State) ->
    case lists:member(riak_kv, riak_core_node_watcher:services(node())) of
        true ->
            reload_hashtrees(State);
        false ->
            State
    end.

reload_hashtrees(State=#state{trees=Trees}) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Indices = riak_core_ring:my_indices(Ring),
    Existing = dict:from_list(Trees),
    MissingIdx = [Idx || Idx <- Indices,
                         not dict:is_key(Idx, Existing)],
    L = [{Idx, Pid} || Idx <- MissingIdx,
                       {ok,Pid} <- [riak_kv_vnode:hashtree_pid(Idx)],
                       is_pid(Pid) andalso is_process_alive(Pid)],
    Trees2 = orddict:from_list(Trees ++ L),
    State2 = State#state{trees=Trees2},
    State3 = lists:foldl(fun({Idx,Pid}, StateAcc) ->
                                 monitor(process, Pid),
                                 add_index_exchanges(Idx, StateAcc)
                         end, State2, L),
    State3.

do_get_lock(_Type, Pid, State=#state{locks=Locks}) ->
    case length(Locks) >= ?CONCURRENCY of
        true ->
            {max_concurrency, State};
        false ->
            Ref = monitor(process, Pid),
            State2 = State#state{locks=[{Pid,Ref}|Locks]},
            {ok, State2}
    end.

maybe_release_lock(Ref, State) ->
    Locks = lists:keydelete(Ref, 2, State#state.locks),
    State#state{locks=Locks}.

maybe_clear_exchange(Ref, State) ->
    case lists:keyfind(Ref, 2, State#state.exchanges) of
        false ->
            ok;
        {Idx,Ref} ->
            lager:info("Untracking exchange: ~p", [Idx])
    end,
    Exchanges = lists:keydelete(Ref, 2, State#state.exchanges),
    State#state{exchanges=Exchanges}.

maybe_clear_registered_tree(Pid, State) when is_pid(Pid) ->
    Trees = lists:keydelete(Pid, 2, State#state.trees),
    State#state{trees=Trees};
maybe_clear_registered_tree(_, State) ->
    State.

next_tree(#state{trees=[]}) ->
    throw(no_trees_registered);
next_tree(State=#state{tree_queue=[], trees=Trees}) ->
    State2 = State#state{tree_queue=Trees},
    next_tree(State2);
next_tree(State=#state{tree_queue=Queue}) ->
    %% TODO: Handle dead pids and remove them
    [{_Index,Pid}|Rest] = Queue,
    State2 = State#state{tree_queue=Rest},
    {Pid, State2}.

schedule_tick() ->
    DefaultTick = 1000,
    Tick = app_helper:get_env(riak_kv,
                              entropy_tick,
                              DefaultTick),
    erlang:send_after(Tick, ?MODULE, tick).

tick(State) ->
    State2 = lists:foldl(fun(_,S) ->
                                 maybe_poke_tree(S)
                         end, State, lists:seq(1,10)),
    State3 = maybe_exchange(State2),
    schedule_tick(),
    State3.

maybe_poke_tree(State=#state{trees=[]}) ->
    State;
maybe_poke_tree(State) ->
    {Tree, State2} = next_tree(State),
    gen_server:cast(Tree, poke),
    State2.

%%%===================================================================
%%% Exchanging
%%%===================================================================

do_exchange_status(_Pid, LocalVN, RemoteVN, IndexN, Reply, State) ->
    {LocalIdx, _} = LocalVN,
    {RemoteIdx, _} = RemoteVN,
    lager:info("S: ~p", [Reply]),
    case Reply of
        ok ->
            State;
        _ ->
            %% lager:info("Requeuing rate limited exchange"),
            State2 = requeue_exchange(LocalIdx, RemoteIdx, IndexN, State),
            State2
    end.

start_exchange(LocalVN, {RemoteIdx, IndexN}, Ring, State) ->
    Owner = riak_core_ring:index_owner(Ring, RemoteIdx),
    RemoteVN = {RemoteIdx, Owner},
    case exchange_fsm:start(LocalVN, RemoteVN, IndexN) of
        {ok, FsmPid} ->
            %% Make this happen automatically as part of init in exchange_fsm
            lager:info("Starting exchange: ~p", [LocalVN]),
            %% exchange_fsm handles locking: tries to get concurrency lock, then index_ht lock
            {LocalIdx, _} = LocalVN,
            Tree = orddict:fetch(LocalIdx, State#state.trees),
            exchange_fsm:start_exchange(FsmPid, Tree, self()),
            %% Do we want to monitor exchange FSMs?
            %% Do we want to track exchange FSMs?
            Ref = monitor(process, FsmPid),
            E = State#state.exchanges,
            {ok, State#state{exchanges=[{LocalIdx,Ref}|E]}};
        {error, Reason} ->
            {Reason, State}
    end.

all_pairwise_exchanges(Index, Ring) ->
    LocalIndexN = riak_kv_vnode:responsible_preflists(Index, Ring),
    Sibs = riak_kv_vnode:preflist_siblings(Index),
    lists:flatmap(
      fun(RemoteIdx) when RemoteIdx == Index ->
              [];
         (RemoteIdx) ->
              RemoteIndexN = riak_kv_vnode:responsible_preflists(RemoteIdx, Ring),
              SharedIndexN = ordsets:intersection(ordsets:from_list(LocalIndexN),
                                                  ordsets:from_list(RemoteIndexN)),
              [{Index, RemoteIdx, IndexN} || IndexN <- SharedIndexN]
      end, Sibs).

all_exchanges(_Node, Ring, #state{trees=Trees}) ->
    Indices = orddict:fetch_keys(Trees),
    lists:flatmap(fun(Index) ->
                          all_pairwise_exchanges(Index, Ring)
                  end, Indices).

add_index_exchanges(Index, State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Exchanges = all_pairwise_exchanges(Index, Ring),
    EQ = State#state.exchange_queue ++ Exchanges,
    EQ2 = prune_exchanges(EQ),
    State#state{exchange_queue=EQ2}.

prune_exchanges(Exchanges) ->
    L = [if A < B ->
                 {A, B, IndexN};
            true ->
                 {B, A, IndexN}
         end || {A, B, IndexN} <- Exchanges],
    lists:usort(L).

already_exchanging(Index, #state{exchanges=E}) ->
    case lists:keyfind(Index, 1, E) of
        false ->
            false;
        {Index,_} ->
            true
    end.

maybe_exchange(State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    case next_exchange(Ring, State) of
        {none, State2} ->
            State2;
        {NextExchange, State2} ->
            {LocalIdx, RemoteIdx, IndexN} = NextExchange,
            case already_exchanging(LocalIdx, State) of
                true ->
                    requeue_exchange(LocalIdx, RemoteIdx, IndexN, State2);
                false ->
                    LocalVN = {LocalIdx, node()},
                    io:format("SE: ~p~n", [[LocalVN, {RemoteIdx, IndexN}]]),
                    case start_exchange(LocalVN, {RemoteIdx, IndexN}, Ring, State2) of
                        {ok, State3} ->
                            State3;
                        {_Reason, State3} ->
                            %% lager:info("ExchangeQ: ~p", [Reason]),
                            State3
                    end
            end
    end.

init_next_exchange(State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Exchanges = prune_exchanges(all_exchanges(node(), Ring, State)),
    State#state{exchange_queue=Exchanges}.

next_exchange(_Ring, State=#state{exchange_queue=[], trees=[]}) ->
    {none, State};
next_exchange(Ring, State=#state{exchange_queue=[]}) ->
    %% %% [Exchange|Rest] = all_pairwise_exchanges(State#state.index, Ring),
    [Exchange|Rest] = prune_exchanges(all_exchanges(node(), Ring, State)),
    State2 = State#state{exchange_queue=Rest},
    {Exchange, State2};
    %% _ = Ring,
    %% {none, State};
next_exchange(_Ring, State=#state{exchange_queue=Exchanges}) ->
    [Exchange|Rest] = Exchanges,
    State2 = State#state{exchange_queue=Rest},
    {Exchange, State2}.

requeue_poke(Index, State=#state{trees=Trees}) ->
    case orddict:find(Index, Trees) of
        {ok, Tree} ->
            Queue = State#state.tree_queue ++ [{Index,Tree}],
            State#state{tree_queue=Queue};
        _ ->
            State
    end.

requeue_exchange(LocalIdx, RemoteIdx, IndexN, State) ->
    Exchange = {LocalIdx, RemoteIdx, IndexN},
    case lists:member(Exchange, State#state.exchange_queue) of
        true ->
            State;
        false ->
            lager:info("Requeue: ~p", [{LocalIdx, RemoteIdx, IndexN}]),
            Exchanges = State#state.exchange_queue ++ [Exchange],
            State#state{exchange_queue=Exchanges}
    end.
