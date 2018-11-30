%% -*- coding: utf-8 -*-

-module(ng_queue).

-behaviour(gen_server).

-export([
	 start_link/0,
	 init/1,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 terminate/2
	]).

-export([
	 child_spec/0,
	 query/2
	]).

%%====================================================================
%% API functions
%%====================================================================

child_spec() ->
    #{
       id => ?MODULE,
       start => {?MODULE, start_link, []},
       type => worker
     }.

query(Query, TypedParameters) ->
    gen_server:call(?MODULE, {query, Query, TypedParameters}).

%%====================================================================
%% Callback functions
%%====================================================================

start_link() ->
    Args = case application:get_env(?MODULE) of
    	       {ok, Env} -> Env;
    	       undefined -> #{}
    	   end,
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

init(Args) ->
    case maps:find(prepared_statements, Args) of
	{ok, PreparedStatements} ->
	    maps:map(fun(Identifier, QueryString) ->
			     erlcass:add_prepare_statement(Identifier, QueryString)
		     end, PreparedStatements);
	error ->
	    ok
    end,
    State = Args#{
	      query_queue => query_queue(),
	      executing => dict:new()
	     },
    {ok, State}.

handle_call({query, QueryName, Parameters}, {From, Ref}, State) ->
    Query = #{
      query_name => QueryName,
      parameters => Parameters,
      from => From,
      ref => Ref
     },
    NewState = do_query(Query, State),
    {reply, Ref, NewState}.

handle_cast(Msg, _State) ->
    lager:log(error, "console",
	      "~p received message to handle cast, but not support handle cast (message ~p)~n",
	      [?MODULE, Msg]).

handle_info({execute_statement_result, Tag, {ok, _, Result}}, State) ->
    NewState = do_result(Tag, Result, State),
    {noreply, NewState};
handle_info({execute_statement_result, Tag, Error}, State) ->
    lager:log(error, "console", "(~p, retrying) ~p~n", [?MODULE, Error]),
    NewData = retry_request(Tag, State),
    {noreply, NewData}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

do_query(#{from := From} = Query, #{query_queue := Queue} = State) ->
    execute_next(State#{
		   query_queue := schedule_query(From, Query, Queue)
		  }).

execute_next(#{
		max_simultaneous_requests := MaxSimultaneousRequests,
		query_queue := Queue,
		executing := Executing
	      } = State) ->
    ExecutingCount = dict:size(Executing),
    Count = queue_count(Queue),
    if ExecutingCount < MaxSimultaneousRequests andalso Count > 0 ->
	    Query = queue_head(Queue),
	    %lager:log(info, "console", "queries in executing: ~p~n", [ExecutingCount]),
	    execute_query(Query, State);
       ExecutingCount == MaxSimultaneousRequests orelse Count == 0 ->
	    State
    end.

execute_query(#{query_name := QueryName, parameters := Parameters} = Query,
	      #{query_queue := Queue, executing := Executing} = State) ->
    {ok, Tag} = erlcass:async_execute(QueryName, Parameters),
    State#{
      executing := dict:store(Tag, Query, Executing),
      query_queue := queue_tail(Queue)
     }.

do_result(Tag, Result, #{executing := Executing} = State) ->
    #{from := From, ref := Ref} = dict:fetch(Tag, Executing),
    From ! {ok, Ref, Result},
    execute_next(State#{executing := dict:erase(Tag, Executing)}).

retry_request(Tag, #{executing := Executing} = State) ->
    Query = dict:fetch(Tag, Executing),
    do_query(Query, State#{executing => dict:erase(Tag, Executing)}).

%%% query_queue

query_queue() ->
    #{
       queries_by_pids => maps:new(),
       pids_order => queue:new()
     }.

schedule_query(Pid, Query, #{
		      queries_by_pids := Queries,
		      pids_order := Order
		     } = Queue) ->
    Queue#{
      queries_by_pids := append_query(Pid, Query, Queries),
      pids_order := schedule_pid(Pid, Order)
     }.

append_query(Pid, Query, Queries) ->
    Fun = fun(Queue) -> queue:snoc(Queue, Query) end,
    Init = queue:from_list([Query]),
    maps:update_with(Pid, Fun, Init, Queries).

schedule_pid(Pid, Queue) ->
    case queue:member(Pid, Queue) of
	false -> queue:snoc(Queue, Pid);
	true -> Queue
    end.

queue_head(#{
	      queries_by_pids := Queries,
	      pids_order := Order
	    }) ->
    Pid = queue:head(Order),
    #{Pid := Queue} = Queries,
    queue:head(Queue).

queue_tail(#{
	      queries_by_pids := Queries,
	      pids_order := Order
	    }) ->
    Pid = queue:head(Order),
    #{Pid := Queue} = Queries,
    Tail = queue:tail(Queue),
    case queue:is_empty(Tail) of
	false -> #{
	  queries_by_pids => maps:put(Pid, Tail, Queries),
	  pids_order => rotate_queue(Order)
	 };
	true -> #{
	  queries_by_pids => maps:remove(Pid, Queries),
	  pids_order => queue:tail(Order)
	 }
    end.

rotate_queue(Queue) ->
    Head = queue:head(Queue),
    Tail = queue:tail(Queue),
    queue:snoc(Tail, Head).

queue_count(#{queries_by_pids := Queries}) ->
    maps:size(Queries).
