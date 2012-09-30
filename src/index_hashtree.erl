-module(index_hashtree).
-behaviour(gen_server).

-include_lib("riak_kv_vnode.hrl").

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {index,
                built,
                lock :: undefined | reference(),
                path,
                build_time,
                trees}).

-compile(export_all).

%% Time from build to expiration of tree, in microseconds.
-define(EXPIRE, 10000000).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Index) ->
    gen_server:start_link(?MODULE, [Index], []).

start_link(Index, IndexN) ->
    gen_server:start_link(?MODULE, [Index, IndexN], []).

new_tree(Id, Tree) ->
    put(calling, ?LINE),
    gen_server:call(Tree, {new_tree, Id}, infinity).

insert(Id, Key, Hash, Tree) ->
    insert(Id, Key, Hash, Tree, []).

insert(Id, Key, Hash, Tree, Options) ->
    gen_server:cast(Tree, {insert, Id, Key, Hash, Options}).

insert_object(BKey, RObj, Tree) ->
    gen_server:cast(Tree, {insert_object, BKey, RObj}).

delete(BKey, Tree) ->
    gen_server:cast(Tree, {delete, BKey}).

start_exchange_remote(FsmPid, IndexN, Tree) ->
    put(calling, ?LINE),
    gen_server:call(Tree, {start_exchange_remote, FsmPid, IndexN}, infinity).

update(Id, Tree) ->
    put(calling, ?LINE),
    gen_server:call(Tree, {update_tree, Id}, infinity).

build(Tree) ->
    gen_server:cast(Tree, build).

exchange_bucket(Id, Level, Bucket, Tree) ->
    put(calling, ?LINE),
    gen_server:call(Tree, {exchange_bucket, Id, Level, Bucket}, infinity).

exchange_segment(Id, Segment, Tree) ->
    put(calling, ?LINE),
    gen_server:call(Tree, {exchange_segment, Id, Segment}, infinity).

compare(Id, Remote, Tree) ->
    compare(Id, Remote, undefined, Tree).

compare(Id, Remote, AccFun, Tree) ->
    put(calling, ?LINE),
    gen_server:call(Tree, {compare, Id, Remote, AccFun}, infinity).

get_lock(Tree, Type) ->
    get_lock(Tree, Type, self()).

get_lock(Tree, Type, Pid) ->
    put(tree, Tree),
    put(calling, ?LINE),
    gen_server:call(Tree, {get_lock, Type, Pid}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Index]) ->
    Root = "data/anti",
    Path = filename:join(Root, integer_to_list(Index)),

    {ok, #state{index=Index,
                trees=orddict:new(),
                built=false,
                path=Path}};

init([Index, IndexN]) ->
    Root = "data/anti",
    Path = filename:join(Root, integer_to_list(Index)),

    State = #state{index=Index,
                   trees=orddict:new(),
                   built=false,
                   path=Path},
    State2 = init_trees(IndexN, State),
    {ok, State2}.

init_trees(IndexN, State) ->
    State2 = lists:foldl(fun(Id, StateAcc) ->
                                 do_new_tree(Id, StateAcc)
                         end, State, IndexN),
    State2#state{built=false}.
   
load_built(#state{trees=Trees}) ->
    {_,Tree0} = hd(Trees),
    case hashtree:read_meta(<<"built">>, Tree0) of
        {ok, <<1>>} ->
            true;
        _ ->
            false
    end.

hash_object(RObjBin) ->
    RObj = binary_to_term(RObjBin),
    Vclock = riak_object:vclock(RObj),
    UpdObj = riak_object:set_vclock(RObj, lists:sort(Vclock)),
    Hash = erlang:phash2(term_to_binary(UpdObj)),
    term_to_binary(Hash).

fold_keys(Partition, Tree) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Req = ?FOLD_REQ{foldfun=fun(BKey={Bucket,Key}, RObj, _) ->
                                    IndexN = get_index_n({Bucket, Key}, Ring),
                                    insert(IndexN, term_to_binary(BKey), hash_object(RObj),
                                           Tree, [if_missing]),
                                    ok
                            end,
                    acc0=ok},
    riak_core_vnode_master:sync_command({Partition, node()},
                                        Req,
                                        riak_kv_vnode_master, infinity),
    ok.

handle_call({new_tree, Id}, _From, State) ->
    State2 = do_new_tree(Id, State),
    {reply, ok, State2};

handle_call({get_lock, Type, Pid}, _From, State) ->
    {Reply, State2} = do_get_lock(Type, Pid, State),
    {reply, Reply, State2};

handle_call({start_exchange_remote, FsmPid, _IndexN}, _From, State) ->
    case entropy_manager:get_lock(exchange_remote, FsmPid) of
        max_concurrency ->
            {reply, max_concurrency, State};
        ok ->
            case do_get_lock(remote_fsm, FsmPid, State) of
                {ok, State2} ->
                    {reply, ok, State2};
                {Reply, State2} ->
                    {reply, Reply, State2}
            end
    end;

handle_call({update_tree, Id}, From, State) ->
    lager:info("Updating tree: (vnode)=~p (preflist)=~p", [State#state.index, Id]),
    apply_tree(Id,
               fun(Tree) ->
                       {SnapTree, Tree2} = hashtree:update_snapshot(Tree),
                       spawn_link(fun() ->
                                          hashtree:update_perform(SnapTree),
                                          gen_server:reply(From, ok)
                                  end),
                       {noreply, Tree2}
               end,
               State);

handle_call({exchange_bucket, Id, Level, Bucket}, _From, State) ->
    apply_tree(Id,
               fun(Tree) ->
                       Result = hashtree:get_bucket(Level, Bucket, Tree),
                       {Result, Tree}
               end,
               State);

handle_call({exchange_segment, Id, Segment}, _From, State) ->
    apply_tree(Id,
               fun(Tree) ->
                       [{_, Result}] = hashtree:key_hashes(Tree, Segment),
                       {Result, Tree}
               end,
               State);

handle_call({compare, Id, Remote, AccFun}, From, State) ->
    Tree = orddict:fetch(Id, State#state.trees),
    spawn(fun() ->
                  Result = case AccFun of
                               undefined ->
                                   hashtree:compare(Tree, Remote);
                               _ ->
                                   hashtree:compare(Tree, Remote, AccFun)
                           end,
                  gen_server:reply(From, Result)
          end),
    {noreply, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(poke, State) ->
    State2 = do_poke(State),
    {noreply, State2};

handle_cast(build, State) ->
    State2 = maybe_build(State),
    {noreply, State2};

handle_cast(build_failed, State) ->
    gen_server:cast(entropy_manager, {requeue_poke, State#state.index}),
    State2 = State#state{built=false},
    {noreply, State2};
handle_cast(build_finished, State) ->
    State2 = do_build_finished(State),
    {noreply, State2};

handle_cast({insert, Id, Key, Hash, Options}, State) ->
    State2 = do_insert(Id, Key, Hash, Options, State),
    {noreply, State2};
handle_cast({insert_object, BKey, RObj}, State) ->
    %% lager:info("Inserting object ~p", [BKey]),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    IndexN = get_index_n(BKey, Ring),
    State2 = do_insert(IndexN, term_to_binary(BKey), hash_object(RObj), [], State),
    {noreply, State2};
handle_cast({delete, BKey}, State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    IndexN = get_index_n(BKey, Ring),
    State2 = do_delete(IndexN, term_to_binary(BKey), State),
    {noreply, State2};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, _, _, _}, State) ->
    State2 = maybe_release_lock(Ref, State),
    {noreply, State2};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_new_tree(Id, State=#state{trees=Trees, path=Path}) ->
    Index = State#state.index,
    IdBin = tree_id(Id),
    NewTree = case Trees of
                  [] ->
                      hashtree:new({Index,IdBin}, [{segment_path, Path}]);
                  [{_,Other}|_] ->
                      hashtree:new({Index,IdBin}, Other)
              end,
    Trees2 = orddict:store(Id, NewTree, Trees),
    State#state{trees=Trees2}.

do_get_lock(_, _, State) when State#state.built /= true ->
    lager:info("Not built: ~p :: ~p", [State#state.index, State#state.built]),
    {not_built, State};
do_get_lock(_Type, Pid, State=#state{lock=undefined}) ->
    Ref = monitor(process, Pid),
    State2 = State#state{lock=Ref},
    %% lager:info("Locked: ~p", [State#state.index]),
    {ok, State2};
do_get_lock(_, _, State) ->
    lager:info("Already locked: ~p", [State#state.index]),
    {already_locked, State}.

maybe_release_lock(Ref, State) ->
    case State#state.lock of
        Ref ->
            %% lager:info("Unlocked: ~p", [State#state.index]),
            State#state{lock=undefined};
        _ ->
            State
    end.

apply_tree(Id, Fun, State=#state{trees=Trees}) ->
    case orddict:find(Id, Trees) of
        error ->
            {reply, not_responsible, State};
        {ok, Tree} ->
            {Result, Tree2} = Fun(Tree),
            Trees2 = orddict:store(Id, Tree2, Trees),
            State2 = State#state{trees=Trees2},
            case Result of
                noreply ->
                    {noreply, State2};
                _ ->
                    {reply, Result, State2}
            end
    end.

do_build_finished(State=#state{index=Index, built=_Pid}) ->
    lager:info("Finished build (b): ~p", [Index]),
    {_,Tree0} = hd(State#state.trees),
    hashtree:write_meta(<<"built">>, <<1>>, Tree0),
    State#state{built=true, build_time=os:timestamp()}.

do_insert(Id, Key, Hash, Opts, State=#state{trees=Trees}) ->
    %% lager:info("Insert into ~p/~p :: ~p / ~p", [State#state.index, Id, Key, Hash]),
    case orddict:find(Id, Trees) of
        {ok, Tree} ->
            Tree2 = hashtree:insert(Key, Hash, Tree, Opts),
            Trees2 = orddict:store(Id, Tree2, Trees),
            State#state{trees=Trees2};
        _ ->
            State2 = handle_unexpected_key(Id, Key, State),
            State2
    end.

do_delete(Id, Key, State=#state{trees=Trees}) ->
    case orddict:find(Id, Trees) of
        {ok, Tree} ->
            Tree2 = hashtree:delete(Key, Tree),
            Trees2 = orddict:store(Id, Tree2, Trees),
            State#state{trees=Trees2};
        _ ->
            State2 = handle_unexpected_key(Id, Key, State),
            State2
    end.

handle_unexpected_key(Id, Key, State=#state{index=Partition}) ->
    RP = riak_kv_vnode:responsible_preflists(Partition),
    case lists:member(Id, RP) of
        false ->
            lager:warning("Object ~p encountered during fold over partition "
                          "~p, but key does not hash to an index handled by "
                          "this partition", [Key, Partition]),
            State;
        true ->
            lager:info("Partition/tree ~p/~p does not exist to hold object ~p",
                       [Partition, Id, Key]),
            case State#state.built of
                true ->
                    lager:info("Clearing tree to trigger future rebuild"),
                    clear_tree(State);
                _ ->
                    %% Initialize new index_n tree to prevent future errors,
                    %% but state may be inconsistent until future rebuild
                    State2 = do_new_tree(Id, State),
                    State2
            end
    end.

tree_id({Index, N}) ->
    %% hashtree is hardcoded for 22-byte (176-bit) tree id
    <<Index:160/integer,N:16/integer>>;
tree_id(_) ->
    erlang:error(badarg).

get_index_n(BKey) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    get_index_n(BKey, Ring).

get_index_n({Bucket, Key}, Ring) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket, Ring),
    N = proplists:get_value(n_val, BucketProps),
    ChashKey = riak_core_util:chash_key({Bucket, Key}),
    Index = riak_core_ring:responsible_index(ChashKey, Ring),
    {Index, N}.

do_poke(State) ->
    State1 = maybe_clear(State),
    State2 = maybe_build(State1),
    State2.

maybe_clear(State=#state{lock=undefined, built=true}) ->
    Diff = timer:now_diff(os:timestamp(), State#state.build_time),
    case Diff > ?EXPIRE of
        true ->
            clear_tree(State);
        false ->
            State
    end;
maybe_clear(State) ->
    State.

clear_tree(State=#state{index=Index, trees=Trees}) ->
    lager:info("Clearing tree ~p", [State#state.index]),
    {_,Tree0} = hd(Trees),
    hashtree:destroy(Tree0),
    IndexN = riak_kv_vnode:responsible_preflists(Index),
    State2 = init_trees(IndexN, State#state{trees=orddict:new()}),
    State2#state{built=false}.

maybe_build(State=#state{built=false}) ->
    Self = self(),
    Pid = spawn_link(fun() ->
                             case entropy_manager:get_lock(build) of
                                 max_concurrency ->
                                     gen_server:cast(Self, build_failed);
                                 ok ->
                                     build_or_rehash(Self, State)
                             end
                     end),
    State#state{built=Pid};
maybe_build(State) ->
    %% Already built or build in progress
    State.

build_or_rehash(Self, State=#state{index=Index, trees=Trees}) ->
    case load_built(State) of
        false ->
            lager:info("Starting build: ~p", [Index]),
            fold_keys(Index, Self),
            lager:info("Finished build (a): ~p", [Index]), 
            gen_server:cast(Self, build_finished);
        true ->
            lager:info("Starting rehash: ~p", [Index]),
            _ = [hashtree:rehash_tree(T) || {_,T} <- Trees],
            lager:info("Finished rehash (a): ~p", [Index]),
            gen_server:cast(Self, build_finished)
    end.
