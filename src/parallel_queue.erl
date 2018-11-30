%% -*- coding: utf-8 -*-

-module(parallel_queue).

-behaviour(gen_server).

-export([
	 start_link/0,
	 init/1,
	 handle_call/3,
	 handle_cast/2,
	 terminate/2
	]).

-export([
	 child_spec/0,
	 execute/2,
	 result/2
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

execute(Fun, Args) ->
    gen_server:call(?MODULE, {execute, Fun, Args}, infinity).

result(Worker, Result) ->
    gen_server:cast(?MODULE, {result, Worker, Result}).

%%====================================================================
%% Callback functions
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, #{}, []).

init(Args) ->
    State = Args#{
	      max_workers_count => erlang:system_info(schedulers),
	      executing => dict:new(),
	      workers => [],
	      delayed => []
	     },
    {ok, State}.

handle_call({execute, Fun, Args}, {From, Ref}, State) ->
    NewState = do_job(Ref, Fun, Args, From, State),
    {reply, Ref, NewState}.

handle_cast({result, Worker, Result}, State) ->
    NewState = do_result(Worker, Result, State),
    {noreply, NewState}.

terminate(_Reason, State) ->
    stop_workers(State).

%%====================================================================
%% Internal functions
%%====================================================================

do_job(Ref, Fun, Args, From, #{
			 max_workers_count := MaxWorkersCount,
			 workers := Workers
			} = State) when length(Workers) < MaxWorkersCount ->
    {ok, Worker} = parallel_worker:new(),
    execute_job(Worker, Ref, Fun, Args, From, State#{workers := [Worker|Workers]});
do_job(Ref, Fun, Args, From, #{
			 max_workers_count := MaxWorkersCount,
			 executing := Executing,
			 workers := Workers
			} = State) when length(Workers) == MaxWorkersCount ->
    Size = dict:size(Executing),
    if Size < MaxWorkersCount ->
	    [Worker|_] = list_free_connections(Workers, Executing),
	    execute_job(Worker, Ref, Fun, Args, From, State);
       Size == MaxWorkersCount ->
	    delay_job(Ref, Fun, Args, From, State)
    end.

list_free_connections(Workers, Executing) ->
    lists:dropwhile(fun (Worker) ->
			    dict:is_key(Worker, Executing)
		    end, Workers).

execute_job(Worker, Ref, Fun, Args, From, State) ->
    parallel_worker:execute(Worker, Fun, Args),
    maps:update_with(executing, fun(Jobs) ->
					dict:store(Worker, {Ref, From}, Jobs)
				end, State).

delay_job(Ref, Fun, Args, From, State) ->
    maps:update_with(delayed, fun(Jobs) ->
				      [{Ref, Fun, Args, From}|Jobs]
			      end, State).

do_result(Worker, Result, #{executing := Executing} = State) ->
    {Ref, From} = dict:fetch(Worker, Executing),
    From ! {ok, Ref, Result},
    execute_delayed(Worker, State).

execute_delayed(Worker, #{delayed := [{Ref, Fun, Args, From}|Rest]} = State) ->
    execute_job(Worker, Ref, Fun, Args, From, State#{delayed => Rest});
execute_delayed(Worker, #{delayed := []} = State) ->
    maps:update_with(executing, fun(Jobs) ->
					dict:erase(Worker, Jobs)
				end, State).

stop_workers(#{workers := Workers}) ->
    lists:foreach(fun parallel_worker:stop/1, Workers).
