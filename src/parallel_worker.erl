%% -*- coding: utf-8 -*-

-module(parallel_worker).

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
	 new/0,
	 execute/3,
	 stop/1
	]).

%%====================================================================
%% API functions
%%====================================================================

new() ->
    ChildSpec = #{
      id => erlang:monotonic_time(),
      start => {?MODULE, start_link, []},
      type => worker
     },
    parallel_sup:start_child(ChildSpec).

execute(Pid, Fun, Args) ->
    gen_server:cast(Pid, {execute, Fun, Args}).

stop(Pid) ->
    parallel_sup:terminate_child(Pid).

%%====================================================================
%% Callback functions
%%====================================================================

start_link() ->
    gen_server:start_link(?MODULE, [], []).

init(Args) ->
    State = Args,
    {ok, State}.

handle_cast({execute, Fun, Args}, State) ->
    execute_job(Fun, Args),
    {noreply, State}.

handle_call(Call, _From, State) ->
    {stop, unexpected_call, {unexpected_call, Call}, State}.

handle_info(Msg, State) ->
    io:format("Unexpected message: ~p~n", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

execute_job(Fun, Args) ->
    Result = apply(Fun, Args),
    parallel_queue:result(self(), Result).
