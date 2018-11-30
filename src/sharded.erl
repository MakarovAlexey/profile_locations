%% -*- coding: utf-8 -*-

-module(sharded).

-behaviour(gen_server).

-export([
	 insert_new/3,
	 close/1,
	 count/1,
	 fold/4,
	 foreach/3,
	 lookup/3,
	 new/2,
	 list/2
	]).

-export([
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 init/1,
	 start_link/3,
	 terminate/2
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

new(Type, Dir) ->
    Ref = make_ref(),
    {ok, Cwd} = file:get_cwd(),
    Path = Cwd ++ "/database/" ++ Dir ++ "/",
    ChildSpec = #{
      id => {?MODULE, Ref},
      start => {?MODULE, start_link, [Ref, Type, Path]}
     },
    {ok, _} = profile_locations_sup:start_child(ChildSpec),
    Ref.

insert_new(ShardNumber, Rows, Ref) ->
    gen_server:cast({via, ?MODULE, Ref}, {insert_new, ShardNumber, Rows}).

fold(Fun, Acc, ShardNumber, Ref) ->
    gen_server:call({via, ?MODULE, Ref}, {foldl, Fun, Acc, ShardNumber}, infinity).

foreach(Fun, ShardNumber, Ref) ->
    gen_server:call({via, ?MODULE, Ref}, {foreach, Fun, ShardNumber}, infinity).

lookup(ShardNumber, Key, Ref) ->
    gen_server:call({via, ?MODULE, Ref}, {lookup, ShardNumber, Key}, infinity).

count(Ref) ->
    gen_server:call({via, ?MODULE, Ref}, {count}, infinity).

list(ShardNumber, Ref) ->
    gen_server:call({via, ?MODULE, Ref}, {list, ShardNumber}, infinity).

close(Ref) ->
    profile_locations_sup:terminate_child({?MODULE, Ref}).

%%====================================================================
%% Callback functions
%%====================================================================

start_link(Ref, Type, Path) ->
    gen_server:start_link({via, ?MODULE, Ref}, ?MODULE, #{type => Type, path => Path}, []).

init(#{path := Path} = Args) ->
    ok = filelib:ensure_dir(Path),
    Data = Args#{tables => dict:new()},
    {ok, Data}.

handle_cast({insert_new, ShardNumber, Rows}, State) ->
    {noreply, do_insert_new(ShardNumber, Rows, State)}.

handle_call({foldl, Fun, Acc, ShardNumber}, _From, State) ->
    Reply = do_foldl(Fun, Acc, ShardNumber, State),
    {reply, Reply, State};
handle_call({foreach, Fun, ShardNumber}, _From, State) ->
    Reply = do_foreach(ShardNumber, Fun, State),
    {reply, Reply, State};
handle_call({lookup, ShardNumber, Key}, _From, State) ->
    Reply = do_lookup(ShardNumber, Key, State),
    {reply, Reply, State};
handle_call({count}, _From, State) ->
    Reply = do_count(State),
    {reply, Reply, State};
handle_call({list, ShardNumber}, _From, State) ->
    Reply = do_list(ShardNumber, State),
    {reply, Reply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #{tables := Tables}) ->
    dict:fold(fun(_, Table, ok) ->
		      dets:close(Table)
	      end, ok,  Tables).

%%====================================================================
%% Internal functions
%%====================================================================

do_insert_new(ShardNumber, Rows, Data) ->
    with_table(fun(Table) ->
		       case dets:insert_new(Table, Rows) of
			   Value when is_boolean(Value) ->
			       Value;
			   {error, Reason} ->
			       lager:log(error, "console", "~p", [Reason]),
			       throw({error, Reason})
		       end
	       end, ShardNumber, Data).

with_table(Fun, ShardNumber, #{tables := Tables, type := Type, path := Path} = Data) ->
    case dict:find(ShardNumber, Tables) of
	{ok, Table} ->
	    apply(Fun, [Table]),
	    Data;
	error ->
	    Table = lists:concat(["table_", ShardNumber]),
	    Filepath = Path ++ Table,
	    case dets:open_file(Filepath, [{type, Type}, {file, Filepath}]) of
		{ok, Ref} ->
		    apply(Fun, [Ref]),
		    Data#{tables := dict:store(ShardNumber, Ref, Tables)};
		{error, Reason} ->
		    lager:log(error, "console", "error open DETS file: ~p, path: ~p, table name: ~p ", [Reason, Filepath, Table]),
		    throw({error, Reason})
	    end
    end.

do_foldl(Fun, Acc, ShardNumber, Data) ->
    with_existent_table(fun(Table) ->
				dets:foldl(Fun, Acc, Table)
			end, Acc, ShardNumber, Data).

with_existent_table(Fun, Default, ShardNumber, #{tables := Tables}) ->
    case dict:find(ShardNumber, Tables) of
	{ok, Table} -> apply(Fun, [Table]);
	error -> Default
    end.

do_foreach(Fun, ShardNumber, Data) ->
    with_existent_table(fun(Table) ->
				dets:traverse(Table, Fun)
			end, ok, ShardNumber, Data).

do_lookup(ShardNumber, Key, Data) ->
    with_existent_table(fun(Table) ->
				dets:lookup(Table, Key)
			end, [], ShardNumber, Data).

do_count(#{tables := Tables}) ->
    dict:fold(fun(_, Table, Acc) ->
		      Acc + dets:info(Table, no_objects)
	      end, 0, Tables).

do_list(ShardNumber, Data) ->
    with_existent_table(fun(Table) ->
				dets:foldl(fun(Row, List) ->
						   [Row|List]
					   end, [], Table)
			end, [], ShardNumber, Data).
