-module(exchange_fsm).
-behaviour(gen_fsm).

%% API
-export([start_link/3, start/3]).

%% gen_fsm callbacks
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
         terminate/3, code_change/4]).
-compile(export_all).

-record(state, {local,
                remote,
                index_n,
                local_tree,
                remote_tree,
                built,
                missing,
                from}).

%% Per state transition timeout used by certain transitions
-define(DEFAULT_ACTION_TIMEOUT, 20000).

%%%===================================================================
%%% API
%%%===================================================================

start_link(LocalVN, RemoteVN, IndexN) ->
    gen_fsm:start_link(?MODULE, [LocalVN, RemoteVN, IndexN], []).

start(LocalVN, RemoteVN, IndexN) ->
    gen_fsm:start(?MODULE, [LocalVN, RemoteVN, IndexN], []).

sync_start_exchange(Fsm) ->
    gen_fsm:sync_send_event(Fsm, start_exchange, infinity).

start_exchange(Fsm) ->
    start_exchange(Fsm, undefined, undefined).

start_exchange(Fsm, Tree, Pid) ->
    gen_fsm:send_event(Fsm, {start_exchange, Tree, Pid}).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([LocalVN, RemoteVN, IndexN]) ->
    State = #state{local=LocalVN,
                   remote=RemoteVN,
                   index_n=IndexN,
                   built=0},
    {ok, prepare_exchange, State}.

prepare_exchange(start_exchange, From, State) ->
    prepare_exchange(start_exchange, State#state{from=From}).

prepare_exchange({start_exchange, Tree, From}, State) ->
    monitor(process, From),
    monitor(process, Tree),
    prepare_exchange(start_exchange, State#state{local_tree=Tree, from=From});

prepare_exchange(start_exchange, State=#state{local=_LocalVN,
                                              remote=RemoteVN,
                                              index_n=IndexN}) ->
    case entropy_manager:get_lock(exchange) of
        max_concurrency ->
            %% lager:info("Exchange: max_concurrency"),
            maybe_reply(max_concurrency, State),
            {stop, normal, State};
        ok ->
            case index_hashtree:get_lock(State#state.local_tree, local_fsm) of
                ok ->
                    Sender = {fsm, undefined, self()},
                    riak_core_vnode_master:command(RemoteVN,
                                                   {start_exchange_remote, self(), IndexN},
                                                   Sender,
                                                   riak_kv_vnode_master),
                    next_state_with_timeout(prepare_exchange, State);
                _ ->
                    maybe_reply(already_locked, State),
                    {stop, normal, State}
            end
    end;
prepare_exchange(timeout, State) ->
    do_timeout(State);
prepare_exchange({remote_exchange, Pid}, State) when is_pid(Pid) ->
    State2 = maybe_reply(ok, State),
    monitor(process, Pid),
    State3 = State2#state{remote_tree=Pid},
    update_trees(start_exchange, State3);
prepare_exchange({remote_exchange, Error}, State) ->
    %% lager:info("Exchange: {remote, ~p}", [Error]),
    maybe_reply({remote, Error}, State),
    {stop, normal, State}.

update_trees(start_exchange, State=#state{local=LocalVN,
                                          remote=RemoteVN,
                                          local_tree=LocalTree,
                                          remote_tree=RemoteTree,
                                          index_n=IndexN}) ->
    lager:info("Sending to ~p", [LocalVN]),
    lager:info("Sending to ~p", [RemoteVN]),

    update_request(LocalTree, LocalVN, IndexN),
    update_request(RemoteTree, RemoteVN, IndexN),
    next_state_with_timeout(update_trees, State);

update_trees(timeout, State) ->
    do_timeout(State);
update_trees({not_responsible, VNodeIdx, IndexN}, State) ->
    lager:info("VNode ~p does not cover preflist ~p", [VNodeIdx, IndexN]),
    maybe_reply({not_responsible, VNodeIdx, IndexN}, State),
    {stop, normal, State};
update_trees({tree_built, _, _}, State) ->
    Built = State#state.built + 1,
    case Built of
        2 ->
            lager:info("Moving to key exchange"),
            {next_state, key_exchange, State, 0};
        _ ->
            next_state_with_timeout(update_trees, State#state{built=Built})
    end.

key_exchange(timeout, State=#state{local=LocalVN,
                                   remote=RemoteVN,
                                   local_tree=LocalTree,
                                   remote_tree=RemoteTree,
                                   index_n=IndexN}) ->
    lager:info("Starting key exchange between ~p and ~p", [LocalVN, RemoteVN]),
    lager:info("Exchanging hashes for preflist ~p", [IndexN]),

    Remote = fun(get_bucket, {L, B}) ->
                     exchange_bucket(RemoteTree, IndexN, L, B);
                (key_hashes, Segment) ->
                     exchange_segment(RemoteTree, IndexN, Segment)
             end,

    %% TODO: Do we want this to be sync? Do we want FSM to be able to timeout?
    %%       Perhaps this should be sync but we have timeout tick?
    {ok, RC} = riak:local_client(),
    AccFun = fun(KeyDiff, Acc) ->
                     lists:foreach(fun(Diff) ->
                                           read_repair_keydiff(RC, Diff)
                                   end, KeyDiff),
                     Acc
             end,
    index_hashtree:compare(IndexN, Remote, AccFun, LocalTree),
    {stop, normal, State}.
%%     [riak_kv_vnode:get([LocalVN, RemoteVN], BKey, make_ref()) || BKey <- Missing],
%%     {next_state, test, State#state{missing=Missing}, 1000}.

read_repair_keydiff(RC, {_, KeyBin}) ->
    {Bucket, Key} = binary_to_term(KeyBin),
    lager:info("Anti-entropy forced read repair: ~p/~p", [Bucket, Key]),
    RC:get(Bucket, Key),
    ok.

%% test(timeout, State=#state{missing=Missing}) ->
%%     {ok, RC} = riak:local_client(),
%%     [begin
%%          lager:info("Anti-entropy forced read repair: ~p/~p", [Bucket, Key]),
%%          RC:get(Bucket, Key)
%%      end || {Bucket, Key} <- Missing],
%%     maybe_reply(ok, State),
%%     {stop, normal, State};
%% test(Request, State) ->
%%     lager:info("Req: ~p", [Request]),
%%     {next_state, test, State, 500}.
   
exchange_bucket(Tree, IndexN, Level, Bucket) ->
    index_hashtree:exchange_bucket(IndexN, Level, Bucket, Tree).

exchange_segment(Tree, IndexN, Segment) ->
    index_hashtree:exchange_segment(IndexN, Segment, Tree).

handle_event(_Event, _StateName, State) ->
    %% {next_state, StateName, State}.
    {stop, badmsg, State}.

handle_sync_event(_Event, _From, _StateName, State) ->
    %% {reply, ok, StateName, State}.
    {stop, badmsg, State}.

handle_info({'DOWN', _, _, _, _}, _StateName, State) ->
    %% Either the entropy manager, local hashtree, or remote hashtree has
    %% exited. Stop exchange.
    {stop, normal, State};
handle_info(_Info, _StateName, State) ->
    %% {next_state, StateName, State}.
    {stop, badmsg, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

update_request(Tree, {Index, _}, IndexN) ->
    as_event(fun() ->
                     case index_hashtree:update(IndexN, Tree) of
                         ok ->
                             {tree_built, Index, IndexN};
                         not_responsible ->
                             {not_responsible, Index, IndexN}
                     end
             end).

as_event(F) ->
    Self = self(),
    spawn(fun() ->
                  Result = F(),
                  gen_fsm:send_event(Self, Result)
          end),
    ok.

do_timeout(State=#state{local=LocalVN,
                        remote=RemoteVN,
                        index_n=IndexN}) ->
    lager:info("Timeout during exchange between (local) ~p and (remote) ~p, "
               "(preflist) ~p", [LocalVN, RemoteVN, IndexN]),
    maybe_reply({timeout, RemoteVN, IndexN}, State),
    {stop, normal, State}.

maybe_reply(_, State=#state{from=undefined}) ->
    ok,
    State;
maybe_reply(Reply, State=#state{from=Pid,
                                local=LocalVN,
                                remote=RemoteVN,
                                index_n=IndexN}) when is_pid(Pid) ->
    gen_server:cast(Pid, {exchange_status, self(), LocalVN, RemoteVN, IndexN, Reply}),
    State#state{from=undefined};
maybe_reply(Reply, State=#state{from=From}) ->
    gen_fsm:reply(From, Reply),
    State#state{from=undefined}.

next_state_with_timeout(StateName, State) ->
    next_state_with_timeout(StateName, State, ?DEFAULT_ACTION_TIMEOUT).
next_state_with_timeout(StateName, State, Timeout) ->
    {next_state, StateName, State, Timeout}.
