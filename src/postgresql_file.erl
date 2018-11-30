%% -*- coding: utf-8 -*-

-module(postgresql_file).

-behaviour(gen_server).

-export([
	 start_link/2,
	 init/1,
	 handle_call/3,
	 handle_cast/2,
	 terminate/2
	]).

-export([
	 child_spec/2,
	 new/1,
	 write/2,
	 close/1
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

child_spec(Ref, Filename) ->
    #{
       id => {?MODULE, Filename},
       start => {?MODULE, start_link, [Ref, Filename]},
       type => worker
     }.

new(Filename) ->
    Ref = make_ref(),
    ChildSpec = child_spec(Ref, Filename),
    postgresql_file_sup:start_child(ChildSpec),
    {ok, Ref}.

write(Ref, Rows) ->
    gen_server:cast({via, ?MODULE, Ref}, {write, Rows}).

close(Ref) ->
    profile_locations_sup:terminate_child({?MODULE, Ref}).

%%====================================================================
%% Callback functions
%%====================================================================

start_link(Ref, Filename) ->
    gen_server:start_link({via, ?MODULE, Ref}, ?MODULE, Filename, []).

init(Filename) ->
    {ok, Cwd} = file:get_cwd(),
    Path = lists:concat([Cwd,  "/", Filename]),
    {ok, IoDevice} = file:open(Path, [write, raw, binary, delayed_write]),
    file:write(IoDevice, <<"PGCOPY\n\377\r\n\0", 0:1/big-signed-unit:32, 0:1/big-signed-unit:32>>),
    {ok, IoDevice}.

handle_call(_Msg, _From, State) ->
    {reply, error, State}.

handle_cast({write, Rows}, IoDevice) ->
    write_rows(IoDevice, Rows),
    {noreply, IoDevice}.

terminate(_Reason, State) ->
    close_binary_file(State).

%%====================================================================
%% Internal functions
%%====================================================================

write_rows(IoDevice, Rows) ->
    Data = lists:foldl(fun append_row/2, <<>>, Rows),
    case file:write(IoDevice, Data) of
	{error, Reason} -> throw({error, Reason});
	ok -> ok
    end.

append_row(Fields, Acc0) ->
    Count = length(Fields),
    lists:foldl(
      fun({Value, Type}, Acc) ->
	      Binary = encode_field(Type, Value),
	      Length = size(Binary),
	      <<Acc/binary, Length:1/big-signed-unit:32, Binary/binary>>
      end, <<Acc0/binary, Count:1/big-signed-unit:16>>, Fields).

encode_field(int8, Value) ->
   <<Value:1/big-signed-unit:64>>;
encode_field(int4, Value) ->
    <<Value:1/big-signed-unit:32>>;
encode_field(int2, Value) ->
    <<Value:1/big-signed-unit:16>>;
encode_field(text, Value) when is_binary(Value) ->
    Value;
encode_field(timestamptz, Value) ->
    epgsql_idatetime:encode(timestamptz, Value).

close_binary_file(IoDevice) ->
    file:write(IoDevice, <<-1:1/big-signed-unit:16>>),
    file:close(IoDevice).
