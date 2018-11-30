%% -*- coding: utf-8 -*-

-module(pgc_server).

-behaviour(gen_server).

-export([
	 start_link/1,
	 init/1,
	 handle_call/3,
	 handle_cast/2,
	 terminate/2
	]).

-export([
	 ensure_started/1,
	 child_spec/1,
	 registry_spec/0,
	 close/1,
	 query/6,
	 result/3
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

child_spec(Hostname) ->
    #{
       id => {?MODULE, list_to_binary(Hostname)},
       start => {?MODULE, start_link, [Hostname]},
       type => worker
     }.

ensure_started(Hostname) ->
    ChildSpec = child_spec(Hostname),
    pgc_server_sup:start_child(ChildSpec).

query(Hostname, ShardName, From, Ref, Query, TypedParameters) ->
    gen_server:cast({via, ?MODULE, Hostname}, {query, ShardName, From, Ref, Query, TypedParameters}).

result(Hostname, Connection, Result) ->
    gen_server:cast({via, ?MODULE, Hostname}, {result, Connection, Result}).

close(Pid) ->
    pgc_server_sup:terminate_child(Pid).

%%====================================================================
%% Callback functions
%%====================================================================

start_link(Hostname) ->
    {ok, Args} = application:get_env(?MODULE),
    gen_server:start_link({via, ?MODULE, Hostname}, ?MODULE, Args#{hostname => Hostname}, []).

init(#{hostname := Hostname} = Args) ->
    {ok, Connection} = pgc_connection:new(Hostname),
    Query = "SELECT DISTINCT nspname, spcname FROM pg_class INNER JOIN pg_tablespace ON pg_class.reltablespace = pg_tablespace.oid INNER JOIN pg_namespace ON relnamespace = pg_namespace.oid",
    {ok, _, Result} = pgc_connection:query(Connection, Query, []),
    State = Args#{
	      tablespaces_by_schemas => dict:from_list(Result),
	      query_queue => query_queue(),
	      connections => [Connection],
	      executing => dict:new()
	     },
    lager:log(info, "console", "using shards from ~p...~n", [Hostname]),
    {ok, State}.

handle_call(_Msg, _From, State) ->
    {reply, error, State}.

handle_cast({query, ShardName, From, Ref, QueryString, Parameters}, State) ->
    NewState = do_query(ShardName, QueryString, Parameters, Ref, From, State),
    {noreply, NewState};
handle_cast({result, Connection, Result}, State) ->
    NewState = do_result(Connection, Result, State),
    {noreply, NewState}.

terminate(_Reason, #{connections := Connections}) ->
    lists:foreach(fun pgc_connection:close/1, Connections).

%%====================================================================
%% Internal functions
%%====================================================================

do_query(ShardName, QueryString, Parameters, Ref, From, #{
						    tablespaces_by_schemas := Tablespaces,
						    query_queue := Queue
						   } = State) ->
    Tablespace = dict:fetch(ShardName, Tablespaces),
    do_execute(State#{
		 query_queue := schedule_query(Tablespace, {QueryString, Parameters, From, Ref}, Queue)
		}).

do_execute(#{
	      max_connections_count := MaxConnectionsCount,
	      connections := Connections,
	      executing := Executing,
	      hostname := Hostname
	    } = State) ->
    ExecutingCount = dict:size(Executing),
    ConnectionsCount = length(Connections),
    if ConnectionsCount < MaxConnectionsCount andalso ConnectionsCount == ExecutingCount ->
	    {ok, Connection} = pgc_connection:new(Hostname),
	    execute_query(Connection, State#{connections := [Connection|Connections]});
       ConnectionsCount =< MaxConnectionsCount andalso ConnectionsCount > ExecutingCount ->
	    [Connection|_] = list_free_connections(Connections, Executing),
	    execute_query(Connection, State);
       ConnectionsCount == MaxConnectionsCount andalso ConnectionsCount == ExecutingCount ->
	    State
    end.

list_free_connections(Connections, Processing) ->
    lists:dropwhile(fun (Connection) ->
			    dict:is_key(Connection, Processing)
		    end, Connections).

execute_query(Connection, #{
		query_queue := Queue,
		executing := Executing
	       } = State) ->
    {QueryString, Parameters, From, Ref} = queue_head(Queue),
    pgc_connection:execute(Connection, QueryString, Parameters),
    State#{
      executing := dict:store(Connection, {From, Ref}, Executing),
      query_queue := queue_tail(Queue)
     }.

do_result(Connection, Result, #{executing := Executing} = State) ->
    {From, Ref} = dict:fetch(Connection, Executing),
    From ! {ok, Ref, Result},
    execute_next(Connection, State#{executing := dict:erase(Connection, Executing)}).

execute_next(Connection, #{
	       query_queue := Queue,
	       connections := Connections
	      } = State) ->
    QueueCount = queue_count(Queue),
    ConnectionsCount = length(Connections),
    if QueueCount >= ConnectionsCount ->
	    %%timer:sleep(3000),
	    execute_query(Connection, State);
       QueueCount < ConnectionsCount ->
	    pgc_connection:close(Connection),
	    State#{connections := lists:delete(Connection, Connections)}
    end.

query_queue() ->
    #{
       queries_by_tablespaces => maps:new(),
       tablespaces_order => queue:new()
     }.

schedule_query(Tablespace, Query, #{
			     queries_by_tablespaces := Queries,
			     tablespaces_order := Order
			    } = Queue) ->
    Queue#{
      queries_by_tablespaces := append_query(Tablespace, Query, Queries),
      tablespaces_order := schedule_tablespace(Tablespace, Order)
     }.

append_query(Tablespace, Query, Queries) ->
    Fun = fun(Queue) -> queue:snoc(Queue, Query) end,
    Init = queue:from_list([Query]),
    maps:update_with(Tablespace, Fun, Init, Queries).

schedule_tablespace(Tablespace, Queue) ->
    case queue:member(Tablespace, Queue) of
	false -> queue:snoc(Queue, Tablespace);
	true -> Queue
    end.

queue_head(#{
	      queries_by_tablespaces := Queries,
	      tablespaces_order := Order
	    }) ->
    Tablespace = queue:head(Order),
    #{Tablespace := Queue} = Queries,
    queue:head(Queue).

queue_tail(#{
	      queries_by_tablespaces := Queries,
	      tablespaces_order := Order
	    }) ->
    Tablespace = queue:head(Order),
    #{Tablespace := Queue} = Queries,
    Tail = queue:tail(Queue),
    case queue:is_empty(Tail) of
	false -> #{
	  tablespaces_order => rotate_queue(Order),
	  queries_by_tablespaces => maps:put(Tablespace, Tail, Queries)
	 };
	true -> #{
	  tablespaces_order => queue:tail(Order),
	  queries_by_tablespaces => maps:remove(Tablespace, Queries)
	 }
    end.

queue_count(#{queries_by_tablespaces := Queries}) ->
    maps:size(Queries).

rotate_queue(Queue) ->
    Head = queue:head(Queue),
    Tail = queue:tail(Queue),
    queue:snoc(Tail, Head).
