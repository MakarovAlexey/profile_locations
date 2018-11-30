%% -*- coding: utf-8 -*-

-module(pgc_query).

-behaviour(gen_statem).

-export([
	 new/1,
	 execute/3,
	 execute/4,
	 close/1,
	 close/2,
	 close/3
	]).

-export([
	 callback_mode/0,
	 init/1,
	 start_link/2
	]).

-export([
	 ready/3,
	 closing/3
	]).

-export([
	 registry_spec/0,
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

new(QueryString) ->
    Ref = make_ref(),
    ChildSpec = #{
      id => Ref,
      start => {?MODULE, start_link, [Ref, QueryString]},
      restart => transient
     },
    pgc_query_sup:start_child(ChildSpec),
    {ok, Ref}.

execute(Ref, ShardNumber, Parameters) ->
    gen_statem:cast({via, ?MODULE, Ref}, {execute, ShardNumber, Parameters, fun identity/1}).

execute(Ref, ShardNumber, Parameters, Fun) ->
    gen_statem:cast({via, ?MODULE, Ref}, {execute, ShardNumber, Parameters, Fun}).

close(Ref) ->
    gen_statem:call({via, ?MODULE, Ref}, {close, fun identity/1}).

close(Ref, Fun) ->
    gen_statem:call({via, ?MODULE, Ref}, {close, Fun}).

close(Ref, Fun, Acc) ->
    gen_statem:call({via, ?MODULE, Ref}, {close, fun(Results) -> lists:foldl(Fun, Acc, Results) end}).

%% callback functions

start_link(Ref, QueryString) ->
    gen_statem:start_link({via, ?MODULE, Ref}, ?MODULE, #{query => QueryString}, []).

init(Args) ->
    State = ready,
    Data = Args#{refs => maps:new(), queue => [], results => [], avg_execution_time => 0, avg_query_time => 0, query_start_time => 0},
    {ok, State, Data}.

callback_mode() ->
    state_functions.

%%% state callback(s)

ready(cast, {execute, ShardNumber, Parameters, Fun}, #{refs := Refs} = Data) when map_size(Refs) == 0 ->
    NewData = send_query(ShardNumber, Parameters, Fun, Data),
    {keep_state, NewData};
ready(cast, {execute, ShardNumber, Parameters, Fun}, #{refs := Refs} = Data) when map_size(Refs) > 0 ->
    NewData = queue_query(ShardNumber, Parameters, Fun, Data),
    {keep_state, NewData};
ready(info, {ok, Ref, Result}, Data) ->
    NewData = update_with_result(Ref, Result, Data),
    {keep_state, NewData};
ready({call, From}, {close, Fun}, #{refs := Refs, queue := Queue} = Data) when map_size(Refs) > 0 orelse length(Queue) >= 0 ->
    NewData = Data#{from => From, function => Fun},
    {next_state, closing, NewData};
ready({call, From}, {close, Fun}, #{refs := Refs, queue := Queue, results := Results}) when map_size(Refs) == 0 andalso length(Queue) == 0  ->
    {stop_and_reply, normal, [{reply, From, apply(Fun, [Results])}]}.

closing(info, {ok, Ref, Result}, #{refs := Refs, queue := Queue, from := From, function := Fun} = Data)
  when map_size(Refs) == 1 andalso length(Queue)  == 0 ->
    #{results := Results} = update_with_result(Ref, Result, Data),
    Reply = apply(Fun, [Results]),
    lager:log(
      info, "console", "closing, received last query result, (~p rows), session closed~n", [length(Result)]),
    gen_statem:reply(From, Reply),
    {stop_and_reply, normal, {reply, From, Reply}};
closing(info, {ok, Ref, Result}, #{refs := Refs, queue := Queue} = Data) when map_size(Refs) > 1 orelse length(Queue) > 1 ->
    lager:log(info, "console", "closing, received query result ~p rows (~p left)~n", [length(Result), maps:size(Refs) - 1]),
    NewData = update_with_result(Ref, Result, Data),
    {keep_state, NewData}.

%%====================================================================
%% Internal functions
%%====================================================================

update_with_result(Ref, Result, #{query_start_time := QueryStartTime, refs := Refs, results := Results} = Data) ->
    QueryEndTime = erlang:monotonic_time(),
    #{Ref := Fun} = Refs,
    {Time, Value} = timer:tc(Fun, [Result]),
    lager:log(info, "console", "время выполнения: ~p~n", [Time]),
    AvgExecutionTime = calc_avg_execution_time(Time, Data),
    AvgQueryTime = calc_avg_query_time(QueryEndTime, Data),
    SimultaneosQueriesCount = round(AvgQueryTime / AvgExecutionTime), %% кол-во одновременных запросов к базам
    QueriesCount = SimultaneosQueriesCount - maps:size(Refs), %% коль-во, которое можно запустить дополнительно
    lager:log(
      info, "console", "QueryStartTime: ~p, QueryEndTime: ~p, Среднее время запроса: ~p, среднее время выполнения: ~p, длина очереди: ~p, количество возможных дополнительных запросов: ~p~n",
      [QueryEndTime, QueryStartTime, AvgQueryTime, AvgExecutionTime, SimultaneosQueriesCount, QueriesCount]),
    send_queries(QueriesCount, Data#{
				 avg_execution_time := AvgExecutionTime,
				 avg_query_time := AvgQueryTime,
				 results := [Value|Results],
				 refs := maps:remove(Ref, Refs)
				}).

send_queries(QueriesCount, #{queue := [{ShardNumber, Parameters, Fun}|Queue]} = Data) when QueriesCount > 0 ->
    NewData = send_query(ShardNumber, Parameters, Fun, Data#{queue := Queue}),
    send_queries(QueriesCount - 1, NewData);
send_queries(QueriesCount, #{queue := []} = Data) when QueriesCount > 0 ->
    Data;
send_queries(QueriesCount, Data) when QueriesCount =< 0 ->
    Data.

calc_avg_execution_time(ExecutionTime, #{avg_execution_time := AvgExecutionTime}) ->
    (ExecutionTime + AvgExecutionTime) / 2.

calc_avg_query_time(QueryEndTime, #{
		      query_start_time := QueryStartTime,
		      avg_query_time := AvgQueryTime
		     }) ->
    ((QueryEndTime - QueryStartTime) + AvgQueryTime) / 2.

%% тут отправляем запросы

send_query(ShardNumber, Parameters, Fun, #{refs := Refs, query := Query} = Data) ->
    QueryStartTime = erlang:monotonic_time(),
    Ref = shard:query(ShardNumber, Query, Parameters),
    Data#{refs := maps:put(Ref, Fun, Refs), query_start_time := QueryStartTime}.

queue_query(ShardNumber, Parameters, Fun, #{queue := Queue} = Data) ->
    Data#{queue := [{ShardNumber, Parameters, Fun}|Queue]}.

identity(Arg) ->
    Arg.
