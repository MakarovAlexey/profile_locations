%% -*- coding: utf-8 -*-

-module(shard_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

-export([start_child/1, terminate_child/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
    ChildSpecs = [shard:child_spec(ShardNumber) || ShardNumber <- shards:list()],
    {ok, {SupFlags, [pgc_query_sup:child_spec(),shard:registry_spec()|ChildSpecs]}}.

start_child(ChildSpec) ->
    supervisor:start_child(?MODULE, ChildSpec).

terminate_child(Pid) ->
    supervisor:terminate_child(?MODULE, Pid).
