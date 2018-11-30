%% -*- coding: utf-8 -*-

-module(profile_locations_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).
-export([start_child/1, terminate_child/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
    ChildSpecs = [sharded:registry_spec(),
		  profile_id:registry_spec(),
		  authors_loader:registry_spec(),
		  #{
		     id => pgc_server_sup,
		     start => {pgc_server_sup, start_link, []},
		     type => supervisor
		   }, #{
		     id => shard_sup,
		     start => {shard_sup, start_link, []},
		     type => supervisor
		   }, #{
		     id => ng_queue_sup,
		     start => {ng_queue_sup, start_link, []},
		     type => supervisor
		   }, #{
		     id => parallel_sup,
		     start => {parallel_sup, start_link, []},
		     type => supervisor
		   }, #{
		     id => cache,
		     start => {cache, start_link, [cache, [{n, 10}, {ttl, 60}]]},
		     type => worker
		   }, #{
		     id => postgresql_files,
		     start => {postgresql_file_sup, start_link, []},
		     type => supervisor
		   }
		 ],
    {ok, {SupFlags, ChildSpecs}}.

start_child(ChildSpec) ->
    supervisor:start_child(?MODULE, ChildSpec).

terminate_child(Pid) ->
    supervisor:terminate_child(?MODULE, Pid).
