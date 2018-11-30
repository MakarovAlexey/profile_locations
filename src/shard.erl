%% -*- coding: utf-8 -*-

-module(shard).

-behaviour(gen_server).

-export([
	 start_link/1,
	 init/1,
	 handle_call/3,
	 handle_cast/2,
	 terminate/2
	]).

-export([
	 child_spec/1,
	 registry_spec/0,
	 query/3
	]).

-export([
	 register_name/2,
	 unregister_name/1,
	 whereis_name/1,
	 send/2
	]).

%%====================================================================
%% Registry functions
%%====================================================================

registry_spec() ->
    registry:child_spec(?MODULE, ?MODULE).

register_name(Name, Pid) ->
    registry:register_name(?MODULE, Name, Pid).

unregister_name(Name) ->
    registry:unregister_name(?MODULE, Name).

whereis_name(Name) ->
    registry:whereis_name(?MODULE, Name).

send(Name, Msg) ->
    registry:send(?MODULE, Name, Msg).

%%====================================================================
%% API functions
%%====================================================================

child_spec(ShardNumber) ->
    #{
       id => {?MODULE, ShardNumber},
       start => {?MODULE, start_link, [ShardNumber]},
       type => worker
     }.

query(ShardNumber, Query, TypedParameters) ->
    gen_server:call({via, ?MODULE, ShardNumber}, {query, Query, TypedParameters}).

%%====================================================================
%% Callback functions
%%====================================================================

start_link(ShardNumber) ->
    gen_server:start_link({via, ?MODULE, ShardNumber}, ?MODULE, #{shard_number => ShardNumber}, []).

init(#{shard_number := ShardNumber} = Args) ->
    Hostname = lists:concat(["r", round(rand:uniform() * 1000000), ".rshard-", ShardNumber, ".pgc.kribrum"]),
    Ip = case inet:getaddr(Hostname, inet) of
	     {error, nxdomain} -> throw({nxdomain, Hostname});
	     {ok, Addr} -> Addr
	 end,
    {ok, {hostent, ServerHostname, _, _, _, _}} = inet:gethostbyaddr(Ip),
    pgc_server:ensure_started(ServerHostname),
    ShardName = lists:concat(["shard_", ShardNumber]),
    State = Args#{
	      shard_name => list_to_binary(ShardName),
	      server => ServerHostname
	     },
    {ok, State}.

handle_call({query, QueryString, Parameters}, {Pid, Ref}, State) ->
    do_query(Pid, Ref, QueryString, Parameters, State),
    {reply, Ref, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

do_query(From, Ref, Query, Parameters, #{shard_name := ShardName, server := Hostname}) ->
    QueryString = string:replace(Query, "{shard}", ShardName, all),
    pgc_server:query(Hostname, ShardName, From, Ref, QueryString, Parameters).
