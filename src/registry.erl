%% -*- coding: utf-8 -*-

-module(registry).

-behaviour(gen_server).

-export([
	 start_link/1,
	 init/1,
	 handle_call/3,
	 handle_cast/2,
	 terminate/2
	]).

-export([
	 register_name/3,
	 unregister_name/2,
	 whereis_name/2,
	 send/3
	]).

-export([
	 child_spec/2
	]).

%%====================================================================
%% Callback functions
%%====================================================================

child_spec(Id, Module) ->
    #{
       id => Id,
       start => {?MODULE, start_link, [Module]},
       type => worker
     }.

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

init(_Args) ->
    State = maps:new(),
    {ok, State}.

register_name(Module, Name, Pid) ->
    gen_server:call(Module, {register_name, Name, Pid}).

unregister_name(Module, Name) ->
    gen_server:call(Module, {unregister_name, Name}).

whereis_name(Module, Name) ->
    gen_server:call(Module, {whereis_name, Name}).

send(Module, Name, Msg) ->
    gen_server:call(Module, {send, Name, Msg}).

handle_call({register_name, Name, Pid}, _From, State) ->
    {Reply, NewState} = do_register_name(Name, Pid, State),
    {reply, Reply, NewState};
handle_call({unregister_name, Name}, _From, State) ->
    NewState = do_unregister_name(Name, State),
    {reply, ok, NewState};
handle_call({whereis_name, Name}, _From, State) ->
    Reply = do_whereis_name(Name, State, undefined),
    {reply, Reply, State};
handle_call({send, Name, Msg}, _From, State) ->
    Reply = do_send(Name, Msg, State),
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

do_register_name(Name, Pid, Map) ->
    IsPresent = maps:is_key(Name, Map),
    if not IsPresent -> {yes, maps:put(Name, Pid, Map)};
       IsPresent -> {no, Map}
    end.

do_unregister_name(Name, Map) ->
    maps:without([Name], Map).

do_whereis_name(Name, Map, Default) ->
    maps:get(Name, Map, Default).

do_send(Name, Msg, Map) ->
    Pid = do_whereis_name(Name, Map, undefined),
    Pid ! Msg.
