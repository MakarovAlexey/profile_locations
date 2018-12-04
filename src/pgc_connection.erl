%% -*- coding: utf-8 -*-

-module(pgc_connection).

-behaviour(gen_server).

-export([
	 start_link/2,
	 init/1,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 terminate/2
	]).

-export([
	 registry_spec/0,
	 new/1,
	 execute/3,
	 query/3,
	 close/1
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

start_link(Ref, Ip) ->
    {ok, Args} = application:get_env(?MODULE),
    gen_server:start_link({via, ?MODULE, Ref}, ?MODULE, Args#{ref => Ref, ip => Ip}, []).

init(#{
	username := Username,
	password := Password,
	database := Database,
	ip := Ip
      } = Args) ->
    case epgsql:connect(Ip, Username, Password, #{database => Database}) of
	{ok, Connection} ->
	    erlang:link(Connection),
	    State = Args#{
		      connection => Connection,
		      statements => dict:new()
		     },
	    {ok, State};
	{error, Error} ->
	    throw({error, Ip, Error})
    end.

handle_cast({execute, Query, TypedParameters}, State) ->
    NewState = execute_query(Query, TypedParameters, State),
    {noreply, NewState}.

handle_call({query, Query, TypedParameters}, _From, #{connection := Connection} = State) ->
    Reply = epgsql:equery(Connection, Query, TypedParameters),
    {reply, Reply, State}.

handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, #{connection := Connection}) ->
    epgsql:close(Connection).

new(Ip) ->
    Ref = make_ref(),
    ChildSpec = #{
      id => {Ip, Ref},
      start => {?MODULE, start_link, [Ref, Ip]},
      type => worker
     },
    pgc_connection_sup:start_child(ChildSpec),
    {ok, Ref}.

execute(Ref, Query, TypedParameters) ->
    gen_server:cast({via, ?MODULE, Ref}, {execute, Query, TypedParameters}).

query(Ref, Query, TypedParameters) ->
    gen_server:call({via, ?MODULE, Ref}, {query, Query, TypedParameters}).

close(Ref) ->
    pgc_connection_sup:terminate_child(Ref).

%%====================================================================
%% Internal functions
%%====================================================================

execute_query(QueryString, TypedParameters, #{
			     connection := Connection, ip := Ip, ref := Ref
			    } = State) ->
    {Types, Parameters} = lists:unzip(TypedParameters),
    with_statement(fun(StatementName) ->
			   {ok, _, Result} = epgsql:prepared_query(
					       Connection, StatementName, Parameters),
			   pgc_server:result(Ip, Ref, Result)
		   end, QueryString, Types, State).

with_statement(Fun, QueryString, Types, #{
				   statements := Statements,
				   connection := Connection
				  } = State) ->
    case dict:find(QueryString, Statements) of
	{ok, Name} ->
	    apply(Fun, [Name]),
	    State;
	error ->
	    Name = lists:concat([pid_to_list(self()), "_", dict:size(Statements)]),
	    {ok, _} = epgsql:parse(Connection, Name, QueryString, Types),
	    apply(Fun, [Name]),
	    State#{statements => dict:store(QueryString, Name, Statements)}
    end.
