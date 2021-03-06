%% -*- coding: utf-8 -*-

-module(ng_queue_sup).

-behaviour(supervisor).

-export([start_link/0, init/1, start_child/1, terminate_child/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
    ChildSpecs = [ng_queue:child_spec(), ng_query:registry_spec()],
    {ok, {SupFlags, ChildSpecs}}.

start_child(ChildSpec) ->
    supervisor:start_child(?MODULE, ChildSpec).

terminate_child(Pid) ->
    supervisor:terminate_child(?MODULE, Pid).
