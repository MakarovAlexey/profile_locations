%% -*- coding: utf-8 -*-

-module(profile_id).

-behaviour(gen_server).

-export([
	 start_link/1,
	 init/1,
	 handle_call/3,
	 handle_cast/2,
	 terminate/2
	]).

-export([
	 child_spec/1,
	 new/0,
	 write_event_comment/3,
	 write_event_mention/3,
	 write_event_share/3,
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

child_spec(Ref) ->
    #{
       id => {?MODULE, Ref},
       start => {?MODULE, start_link, [Ref]},
       type => worker
     }.

new() ->
    Ref = make_ref(),
    ChildSpec = child_spec(Ref),
    profile_locations_sup:start_child(ChildSpec),
    {ok, Ref}.

write_event_comment(Ref, ShardNumber, Rows) ->
    gen_server:call({via, ?MODULE, Ref}, {write_event_comment, ShardNumber, Rows}, infinity).

write_event_mention(Ref, ShardNumber, Rows) ->
    gen_server:call({via, ?MODULE, Ref}, {write_event_mention, ShardNumber, Rows}, infinity).

write_event_share(Ref, ShardNumber, Rows) ->
    gen_server:call({via, ?MODULE, Ref}, {write_event_share, ShardNumber, Rows}, infinity).

close(Ref) ->
    profile_locations_sup:terminate_child({?MODULE, Ref}).

%%====================================================================
%% Callback functions
%%====================================================================

start_link(Ref) ->
    {ok, Args} = application:get_env(?MODULE),
    gen_server:start_link({via, ?MODULE, Ref}, ?MODULE, Args, []).

init(#{
	profile_events := ProfileEvents,
	profile_event_comments := EventComments,
	profile_event_mentions := EventMentions,
	profile_event_shares := EventShares
      }) ->
    {ok, Connection} = epgsql:connect("localhost", "makarov", "zxcvb", #{database => "makarov"}),
    Query = prepare_query(Connection, "profile_ids",
			  "SELECT account_id, ensure_profile(account_id) FROM (SELECT UNNEST($1) AS account_id) AS account_ids",
			  [{array, text}]),
    State = #{
      file_profile_events => open_file(ProfileEvents),
      file_event_comments => open_file(EventComments),
      file_event_mentions => open_file(EventMentions),
      file_event_shares => open_file(EventShares),
      connection => Connection,
      query => Query
     },
    {ok, State}.

handle_call({write_event_comment, ShardNumber, Rows}, _From, #{
							file_event_comments := File
						       } = State) ->
    with_events_data(fun(EventsData) ->
			     write_data(EventsData, File)
		     end, ShardNumber, Rows, State),
    {reply, ok, State};
handle_call({write_event_mention, ShardNumber, Rows}, _From, #{
							file_event_mentions := File
						       } = State) ->
    with_events_data(fun(EventsData) ->
			     write_data(EventsData, File)
		     end, ShardNumber, Rows, State),
    {reply, ok, State};
handle_call({write_event_share, ShardNumber, Rows}, _From, #{
						      file_event_shares := File
						     } = State) ->
    with_events_data(fun(EventsData) ->
			     write_data(EventsData, File)
		     end, ShardNumber, Rows, State),
    {reply, ok, State}.

handle_cast(Msg, State) ->
    lager:log(error, "console",
	      "Process ~p (~p) received unhadled cast:~n~p~n",
	      [self(), ?MODULE, Msg]),
    {noreply, State}.

terminate(_Reason, #{
	    file_profile_events := ProfileEvents,
	    file_event_comments := EventComments,
	    file_event_mentions := EventMentions,
	    file_event_shares := EventShares,
	    connection := Connection
	   }) ->
    postgresql_file:close(ProfileEvents),
    postgresql_file:close(EventComments),
    postgresql_file:close(EventMentions),
    postgresql_file:close(EventShares),
    epgsql:close(Connection).

%%====================================================================
%% Internal functions
%%====================================================================

prepare_query(Connection, Name, Query, Parameters) ->
    case epgsql:parse(Connection, Name, Query, Parameters) of
	{ok, _Statement} -> Name;
	Error -> throw(Error)
    end.

open_file(Filename) ->
    {ok, File} = postgresql_file:new(Filename),
    File.

with_events_data(Fun, ShardNumber, Rows, #{
			file_profile_events := ProfileEventsFile,
			connection := Connection,
			query := Query
		       }) ->
    AccountIds = account_ids(Rows),
    {ok, _, Result} = epgsql:prepared_query(Connection, Query, [AccountIds]),
    Map = maps:from_list(Result),
    ProfileEventsData = profile_events_data(ShardNumber, Rows, Map),
    write_data(ProfileEventsData, ProfileEventsFile),
    EventsData = events_data(ShardNumber, Rows, Map),
    apply(Fun, [EventsData]).

account_ids(Rows) ->
    AccountIds = lists:foldl(fun({_, AccountId, _, Dst}, Acc) ->
				     [AccountId, Dst|Acc]
			     end, [], Rows),
    lists:usort(AccountIds).

profile_events_data(ShardNumber, Rows, ProfileIds) ->
    lists:map(fun({Id, AccountId, PTime, _}) ->
		      ProfileId = maps:get(AccountId, ProfileIds),
		      [{ShardNumber, int2}, {Id, int8}, {ProfileId, int4}, {PTime, timestamptz}]
	      end, Rows).

events_data(ShardNumber, Rows, ProfileIds) ->
    lists:map(fun({Id, _, _, Dst}) ->
		      DstId = maps:get(Dst, ProfileIds),
		      [{ShardNumber, int2}, {Id, int8}, {DstId, int4}]
	      end, Rows).

write_data(Data, File) ->
    postgresql_file:write(File, Data).
