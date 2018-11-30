%% -*- coding: utf-8 -*-

-module(ng_query).

-behaviour(gen_statem).

-export([
	 new/1,
	 execute/2,
	 execute/3,
	 close/1,
	 close/2,
	 close/3
	]).

-export([
	 start_link/2,
	 init/1,
	 callback_mode/0
	]).

-export([
	 opened/3,
	 syncing/3
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

new(QueryName) ->
    Ref = make_ref(),
    ChildSpec = #{
      id => Ref,
      start => {?MODULE, start_link, [Ref, QueryName]},
      restart => transient
     },
    ng_queue_sup:start_child(ChildSpec),
    {ok, Ref}.

execute(Ref, Parameters) ->
    gen_statem:cast({via, ?MODULE, Ref}, {execute, fun identity/1, Parameters}).

execute(Ref, Parameters, Fun) ->
    gen_statem:cast({via, ?MODULE, Ref}, {execute, Fun, Parameters}).

close(Ref) ->
    gen_statem:call({via, ?MODULE, Ref}, {close, fun identity/1}).

close(Ref, Fun) ->
    gen_statem:call({via, ?MODULE, Ref}, {close, Fun}).

close(Ref, Fun, Acc) ->
    gen_statem:call({via, ?MODULE, Ref}, {close, fun(Results) -> lists:foldl(Fun, Acc, Results) end}).

%%% callback functions

start_link(Ref, QueryName) ->
    gen_statem:start_link({via, ?MODULE, Ref}, ?MODULE, #{query => QueryName}, []).

init(Args) ->
    State = opened,
    Data = Args#{refs => dict:new(), results => []},
    {ok, State, Data}.

callback_mode() ->
    state_functions.

%%% state callback(s)

opened(cast, {execute, Fun, Parameters}, Data) ->
    NewData = execute_query(Fun, Parameters, Data),
    {keep_state, NewData};
opened(info, {ok, Tag, Result}, Data) ->
    %#{refs := Refs} = Data,
    %%io:fwrite("(ng_queue, open) received query result ~p rows (~p left)~n",
    %%[length(Result), dict:size(Refs) - 1]),
    NewData = update_with_result(Tag, Result, Data),
    {keep_state, NewData};
opened({call, From}, {close, Fun}, #{refs := Refs, results := Results} = Data) ->
    Size = dict:size(Refs),
    if Size > 0 ->
	    NewData = Data#{from => From, function => Fun},
	    %%io:fwrite("(ng_queue) session opened, syncing (closing)...~n", []),
	    {next_state, syncing, NewData};
       Size == 0 ->
	    {stop_and_reply, normal, {reply, From, apply(Fun, [Results])}}
    end.

syncing(info, {ok, Tag, Result}, #{from := From} = Data) ->
    #{refs := Refs} = NewData = update_with_result(Tag, Result, Data),
    Size = dict:size(Refs),
    if Size == 0 ->
	    #{function := Fun, results := Results} = NewData,
	    Reply = apply(Fun, [Results]),
	    %%io:fwrite("(ng_queue, syncing) received last query result (~p rows), session closed~n",
	    %%	      [length(Result)]),
	    gen_statem:reply(From, Reply),
	    {stop, normal};
       Size > 0 ->
	    %%io:fwrite("(ng_queue, syncing) received query result ~p rows (~p left)~n",
	    %%	      [length(Result), Size - 1]),
	    {keep_state, NewData}
    end.

%%====================================================================
%% Internal functions
%%====================================================================

execute_query(Fun, Parameters, #{query := QueryName, refs := Refs} = Data) ->
    Ref = ng_queue:query(QueryName, Parameters),
    Data#{refs => dict:store(Ref, Fun, Refs)}.

update_with_result(Ref, Result, #{refs := Refs, results := Results} = Data) ->
    Fun = dict:fetch(Ref, Refs),
    Data#{refs := dict:erase(Ref, Refs), results := [apply_to_result(Fun, Result)|Results]}.

apply_to_result(Fun, Result) ->
    apply(Fun, [Result]).

identity(X) ->
    X.
