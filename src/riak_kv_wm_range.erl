-module(riak_kv_wm_range).
-export([
         init/1,
         service_available/2,
         allowed_methods/2,
         malformed_request/2,
         content_types_provided/2,
         %% Content
         range/2,
         %% HOFs
         add_nl/1
        ]).
-include_lib("webmachine/include/webmachine.hrl").
-include("riak_kv_wm_raw.hrl").

-record(ctx, {
          api_version,
          bucket,
          client,
          'end',
          keys_only,
          limit,
          prefix,
          riak,
          start
         }).

init(Props) ->
    {ok, #ctx{api_version=proplists:get_value(api_version, Props),
              prefix=proplists:get_value(prefix, Props),
              riak=proplists:get_value(riak, Props)}}.

service_available(RD, Ctx=#ctx{riak=RiakProps}) ->
    case riak_kv_wm_utils:get_riak_client(RiakProps, riak_kv_wm_utils:get_client_id(RD)) of
        {ok, C} ->
            {true,
             RD,
             Ctx#ctx{
               client=C,
               bucket=get_path_param(bucket, RD),
               start=get_path_param(start, RD),
               'end'=get_path_param('end', RD),
               %% TODO probably put coversion in malformed_request
               limit=list_to_integer(wrq:get_qs_value(?Q_LIMIT, "10", RD)),
               keys_only=list_to_existing_atom(wrq:get_qs_value(?Q_KEYS_ONLY, "false", RD))
              }};
        Error ->
            {false,
             wrq:set_resp_body(
               io_lib:format("Unable to connect to Riak: ~p~n", [Error]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}
    end.

allowed_methods(RD, Ctx) ->
    {['GET'], RD, Ctx}.

malformed_request(RD, Ctx) ->
    %% TODO always good for now
    {false, RD, Ctx}.

content_types_provided(RD, Ctx=#ctx{keys_only=KO}) ->
    if KO -> CT = "text/plain";
       true -> CT = "multipart/mixed"
    end,
    {[{CT, range}], RD, Ctx}.

%% -------------------------------------------------------------------
%% Content
%% -------------------------------------------------------------------

range(RD, Ctx=#ctx{keys_only=false}) ->
    #ctx{api_version=APIVersion, bucket=B, client=C, 'end'=E, limit=L,
         prefix=P, start=S}=Ctx,
    lager:info("range ~p ~p ~p ~p", [B, S, E, L]),
    {ok, Res} = C:range(B, S, E, [{limit,L}]),
    Boundary = riak_core_util:unique_id_62(),
    Res2 = multipart_encode(Res, P, B, Boundary, APIVersion),
    RD2 = wrq:set_resp_header(?HEAD_CTYPE,
                              "multipart/mixed; boundry="++Boundary,
                              RD),
    {Res2, RD2, Ctx};

range(RD, Ctx=#ctx{keys_only=true}) ->
    #ctx{bucket=B, client=C, 'end'=E, limit=L, start=S}=Ctx,
    lager:info("range ~p ~p ~p ~p", [B, S, E, L]),
    {ok, Res} = C:range(B, S, E, [keys_only, {limit,L}]),
    Res2 = lists:map(fun ?MODULE:add_nl/1, Res),
    RD2 = wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD),
    {Res2, RD2, Ctx}.

%% -------------------------------------------------------------------
%% Private
%% -------------------------------------------------------------------

get_path_param(Name, RD) ->
    case wrq:path_info(Name, RD) of
        undefined -> undefined;
        Val -> list_to_binary(riak_kv_wm_utils:maybe_decode_uri(RD, Val))
    end.

add_nl(B) ->
    <<B/binary,"\n">>.

multipart_encode(Res, P, B, Boundary, APIVersion) ->
    [[[["\r\n--",Boundary,"\r\n",
        "X-Riak-Key: ", riak_object:key(O), "\r\n",
        riak_kv_wm_utils:multipart_encode_body(P, B, Content, APIVersion)]
       || Content <- riak_object:get_contents(O)]
      || O <- Res],
     "\r\n--",Boundary,"--\r\n"].
