-module(riak_kv_range_fsm_sup).
-behavior(supervisor).

-export([start_range_fsm/2]).
-export([start_link/0,
         init/1]).

start_range_fsm(Node, Args) -> supervisor:start_child({?MODULE, Node}, Args).

start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Spec = {undefined,
            {riak_core_coverage_fsm, start_link, [riak_kv_range_fsm]},
            temporary, 5000, worker, [riak_kv_range_fsm]},
    {ok, {{simple_one_for_one, 10, 10}, [Spec]}}.
