%% @doc Coordinator for range operations.
-module(riak_kv_range_fsm).
-behavior(riak_core_coverage_fsm).
-include("riak_kv.hrl").
-include("riak_kv_vnode.hrl").

-export([init/2,
         process_results/2,
         finish/2]).

-record(ctx, {from}).

init(From, [Bucket, StartKey, EndKey, Limit, Timeout]) ->
    BProps = riak_core_bucket:get_bucket(Bucket),
    N = proplists:get_value(n_val, BProps),
    Req = ?KV_RANGE_REQ{bucket=Bucket,
                        limit=Limit,
                        start_key=StartKey,
                        end_key=EndKey},
    {Req, all, N, 1, riak_kv, riak_kv_vnode_master, Timeout, #ctx{from=From}}.

process_results({final_results, Res}, Ctx=#ctx{from=From}) ->
    riak_core_vnode:reply(From, {?RANGE_RESULTS, Res}),
    {done, Ctx}.

finish(clean, Ctx=#ctx{from=From}) ->
    riak_core_vnode:reply(From, ?RANGE_COMPLETE),
    {stop, normal, Ctx};

finish({error, _}=Err, Ctx=#ctx{from=From}) ->
    riak_core_vnode:reply(From, Err),
    {stop, normal, Ctx}.
