%% -*- coding: utf-8 -*-

-module(parallel).

-behaviour(gen_statem).

-export([
	 new/1,
	 execute/2,
	 close/3,
	 close/2,
	 close/1
	]).

-export([
	 callback_mode/0,
	 init/1,
	 start_link/2
	]).

-export([
	 ready/3,
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

new(Fun) ->
    Ref = make_ref(),
    ChildSpec = #{
      id => Ref,
      start => {?MODULE, start_link, [Ref, Fun]},
      restart => transient
     },
    parallel_sup:start_child(ChildSpec),
    {ok, Ref}.

execute(Ref, Parameters) ->
    gen_statem:cast({via, ?MODULE, Ref}, {execute, Parameters}).

close(Ref, Fun) ->
    gen_statem:call({via, ?MODULE, Ref}, {close, Fun}).

close(Ref) ->
    gen_statem:call({via, ?MODULE, Ref}, {close, fun(Results) -> Results end}).

close(Ref, Fun, Acc0) ->
    gen_statem:call({via, ?MODULE, Ref}, {close, fun(Results) ->
							 lists:foldl(Fun, Acc0, Results)
						 end}).

%%====================================================================
%% Callback functions
%%====================================================================

start_link(Ref, Fun) ->
    gen_statem:start_link({via, ?MODULE, Ref}, ?MODULE, #{key => Fun}, []).

init(Args) ->
    State = ready,
    Data = Args#{refs => [], results => []},
    {ok, State, Data}.

callback_mode() ->
    state_functions.

%%% state callback(s)

ready(cast, {execute, Parameters}, #{key := Fun} = Data) ->
    NewData = do_execute(Fun, Parameters, Data),
    {keep_state, NewData};
ready(info, {ok, Ref, Result}, Data) ->
    NewData = update_with_result(Ref, Result, Data),
    {keep_state, NewData};
ready({call, From}, {close, Fun}, #{refs := [_|_]} = Data) ->
    NewData = do_syncing(From, Fun, Data),
    {next_state, syncing, NewData};
ready({call, From}, {close, Fun}, #{refs := []} = Data) ->
    Reply = do_close(Data#{function => Fun}),
    {stop_and_reply, normal, [{reply, From, Reply}]}.

syncing(info, {ok, Ref, Result}, #{refs := [_,_|_]} = Data) ->
    NewData = update_with_result(Ref, Result, Data),
    {keep_state, NewData};
syncing(info, {ok, Ref, Result}, #{refs := [Ref|[]], from := From} = Data) ->
    NewData = update_with_result(Ref, Result, Data),
    Reply = do_close(NewData),
    gen_statem:reply(From, Reply),
    {stop_and_reply, normal, {reply, From, Reply}}.

%%====================================================================
%% Internal functions
%%====================================================================

do_execute(Fun, Parameters, #{refs := Refs} = Data) ->
    Ref = parallel_queue:execute(Fun, Parameters),
    Data#{refs := [Ref|Refs]}.

do_syncing(From, Fun, Data) ->
    Data#{from => From, function => Fun}.

update_with_result(Ref, Result, #{refs := Refs, results := Results} = Data) ->
    Data#{refs := lists:delete(Ref, Refs), results := [Result|Results]};
update_with_result(Ref, Result, #{refs := Refs, function := Fun, result := Acc} = Data) ->
    Data#{refs := lists:delete(Ref, Refs), result := apply(Fun, [Result, Acc])}.

do_close(#{results := Results, function := Fun}) ->
    apply(Fun, [Results]).
