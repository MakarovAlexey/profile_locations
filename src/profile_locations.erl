%% -*- coding: utf-8 -*-

-module(profile_locations).

-behaviour(application).

-export([start/2, stop/1]).
-export([locate_users/2, count_shards_by_servers/0, load_local_db_shards/1, load_to_binary/1]).

%%====================================================================
%% API functions
%%====================================================================

start(_Type, _Args) ->
    profile_locations_sup:start_link().

stop(_State) ->
    ok.

count_shards_by_servers() ->
    ShardNumbers = lists:seq(0, 4095),
    ShardsByServers =
	lists:foldl(fun(ShardNumber, Dict) ->
			    Hostname = lists:concat(["rshard-", ShardNumber, ".pgc.kribrum"]),
			    Ip = case inet:getaddr(Hostname, inet) of
				     {error, nxdomain} ->
					 throw({nxdomain, Hostname});
				     {ok, Addr} ->
					 Addr
				 end,
			    dict:update_counter(Ip, 1, Dict)
		    end, dict:new(), ShardNumbers),
    dict:to_list(ShardsByServers).

locate_users(Platform, Filename) ->
    Path = init_result("_result"),
    QueryString = load_query_string("statistics.sql"),
    {ok, Query} = pgc_query:new(QueryString),
    {ok, Parallel} = parallel:new(fun do_shard/4),
    Parameters = [{text, <<"%@", Platform/binary>>}],
    shards:foreach(fun(ShardNumber) ->
			   Fun = fun(Rows) ->
					 parallel:execute(Parallel, [ShardNumber, Rows, Platform, Path])
				 end,
			   pgc_query:execute(Query, ShardNumber, Parameters, Fun)
		   end),
    pgc_query:close(Query),
    Data = parallel:close(Parallel, fun merge_results/2, #{
							profiles_with_locations => 0,
							successful_detections => 0,
							results => sets:new()
						       }),
    Thresold = count_thresold(Data),
    lager:log(info, "console", "Порог определения местоположения: ~p~n", [Thresold]),
    {ok, IoDevice} = file:open(Filename, [write, delayed_write]),
    Head = [<<"ExtId"/utf8>>, <<"Местоположение"/utf8>>, <<"Комментарий"/utf8>>, <<"Частота"/utf8>>],
    io:fwrite(IoDevice, "\"~s\",\"~s\",\"~s\",\"~s\"~n", Head),
    lager:log(info, "console", "Запись результата...~n", []),
    write_results(IoDevice, Thresold, Data),
    file:close(IoDevice),
    lager:log(info, "console", "Завершено.~n", []).

load_local_db_shards(Platform) ->
    CommentQueryString = load_query_string("comments.sql"),
    load_local_db(
      CommentQueryString, Platform,
      "INSERT INTO profiles_events_comments (id, dst) VALUES ($1, $2)"),
    MentionQueryString = load_query_string("mentions.sql"),
    load_local_db(
      MentionQueryString, Platform,
      "INSERT INTO profiles_events_mentions (id, dst) VALUES ($1, $2)"),
    ShareQueryString = load_query_string("shares.sql"),
    load_local_db(
      ShareQueryString, Platform,
      "INSERT INTO profiles_events_shares (id, dst) VALUES ($1, $2)").

load_to_binary(Platform) ->
    Parameters = [{text, <<"%@", Platform/binary>>}],
    {ok, Profiles} = profile_id:new(),
    do_load_binary("comments.sql", Parameters, fun profile_id:write_event_comment/3, Profiles),
    %do_load_binary("mentions.sql", Parameters, fun profile_id:write_event_mention/3, Profiles),
    %do_load_binary("shares.sql", Parameters, fun profile_id:write_event_share/3, Profiles),
    profile_id:close(Profiles).

load_profiles() ->
    {ok, Connection} = epgsql:connect("localhost", "makarov", "zxcvb", #{database => "makarov"}),
    Statement = epgsql:parse(Connection, "profiles", "SELECT account_id, id FROM profiles", []),
    Loader = authors_loader:new(),
    with_result(fun(Rows) ->
			authors_loader:write_authors(Loader, Rows)
		end, Connection, Statement, "profiles", 50000),
    authors_loader:close(Loader).

%%====================================================================
%% Internal functions
%%====================================================================

with_result(Fun, Connection, Statement, PortalName, MaxRows) ->
    ok = epgsql:bind(Connection, Statement, PortalName, []),
    execute_statement(Fun, Connection, Statement, PortalName, MaxRows).

execute_statement(Fun, Connection, Statement, PortalName, MaxRows) ->
    {Result, Rows} = epgsql:execute(Connection, Statement, PortalName, MaxRows),
    apply(Fun, Rows),
    case Result of
	partial ->
	    execute_statement(Fun, Connection, Statement, PortalName, MaxRows);
	ok ->
	    ok
    end.

%%% loaders

do_load_binary(QueryFilename, Parameters, WriterFun, Profiles) ->
    Loader = init_loader(QueryFilename, Parameters, WriterFun, Profiles),
    shards:foreach(fun(ShardNumber) ->
			   execute_loader(Loader, ShardNumber)
		   end),
    close_loader(Loader).

init_loader(QueryFile, Parameters, WriterFun, Profiles) ->
    QueryString = load_query_string(QueryFile),
    {ok, Query} = pgc_query:new(QueryString),
    Loader = fun(ShardNumber) ->
		     Fun = fun(Rows) ->
				   apply(WriterFun, [Profiles, ShardNumber, Rows])
			   end,
		     pgc_query:execute(Query, ShardNumber, Parameters, Fun)
	     end,
    #{
       function => Loader,
       query => Query
     }.

execute_loader(#{function := Fun}, ShardNumber) ->
    apply(Fun, [ShardNumber]).

close_loader(#{query := Query}) ->
    pgc_query:close(Query, fun skip_result/1).

%%%

skip_result(_) ->
    skipped.

load_local_db(QueryString, Platform, EventQuery) ->
    {ok, Connection} = epgsql:connect("localhost", "makarov", "zxcvb", #{database => "makarov"}),
    Statements = #{
      lock_profiles =>
	  prepare_query(name, Connection, "lock_profiles", "SELECT pg_advisory_xact_lock(16386)", []),
      insert_profiles =>
	  prepare_query(name, Connection, "insert_profiles",
			load_query_string("insert_profiles.sql"),
			[{array, text}]),
      profiles =>
	  prepare_query(name, Connection, "profiles",
			"SELECT account_id, id FROM profiles WHERE account_id IN (SELECT unnest($1))",
			[{array, text}]),
      exclude_existed =>
	  prepare_query(name, Connection, "excluding_existed_events",
			load_query_string("excluding_existed_events.sql"),
			[{array, int8}]),
      insert_event =>
	  prepare_query(statement, Connection, "insert_event",
			"INSERT INTO profiles_events (id, profile_id, ptime) VALUES ($1, $2, $3)",
			[int8, int4, timestamptz]),
      insert_comment =>
	  prepare_query(statement, Connection, "insert_concretee_event", EventQuery, [int8, int4])
     },
    Parameters = [{text, <<"%@", Platform/binary>>}],
    chunks(
      fun(ShardNumbers) ->
	      {ok, Query} = pgc_query:new(QueryString),
	      lists:foreach(
		fun(ShardNumber) ->
			Fun = fun(Rows) ->
				      AccountIds = list_account_ids(Rows),
				      lager:log(info, "console",
						"Начата загрузка с шарда shard_~p (аккаунтов: ~p, комментариев: ~p)~n",
						[ShardNumber, length(AccountIds), length(Rows)]),
				      case epgsql:with_transaction(
					     Connection, fun(_) ->
								 try
								     lock_profiles(Connection, Statements),
								     insert_profiles(Connection, AccountIds, Statements)
								 catch
								     Error ->
									 lager:log(info, "console", "~p~n", [Error]),
									 throw(Error)
								 end
							 end) of
					  {rollback, Rollback1} ->
					      lager:log(error, "console", "Transaction rolled back:~n~p~n", [Rollback1]);
					  _ ->
					      ok
				      end,
				      case epgsql:with_transaction(
					     Connection, fun(_) ->
								 try
								     Profiles = list_profiles(Connection, AccountIds, Statements),
								     NewEvents = exclude_existed_events(Connection, Rows, Statements),
								     Events = insert_events(NewEvents, Profiles, Statements),
								     Comments = insert_comments(NewEvents, Profiles, Statements),
								     execute_batch(Connection, Events ++ Comments)
								     %%lager:log(info, "console", "new events: ~p~n", [NewEvents])
								 catch
								     Error ->
									 lager:log(info, "console", "~p~n", [Error]),
									 throw(Error)
								 end
							 end) of
					  {rollback, Rollback2} ->
					      lager:log(error, "console", "Transaction rolled back:~n~p~n", [Rollback2]);
					  _ ->
					      ok
				      end
				      %%lager:log(info, "console", "Загрузка с шарда shard_~p закончена~n", [ShardNumber])
			      end,
			pgc_query:execute(Query, ShardNumber, Parameters, Fun)
		end, ShardNumbers),
	      pgc_query:close(Query, fun skip_result/1)
      end, shards:list()),
    epgsql:close(Connection),
    lager:log(info, "console", "Загрузка завершена~n", []).

prepare_query(Type, Connection, Name, Query, Parameters) ->
    case epgsql:parse(Connection, Name, Query, Parameters) of
	{ok, Statement} ->
	    case Type of
		name -> Name;
		statement -> Statement
	    end;
	Error ->
	    throw({parse_error, Name, Error})
    end.

execute_statement(Connection, Statement, Parameters) ->
    case epgsql:prepared_query(Connection, Statement, Parameters) of
	{ok, _, Result} ->
	    Result;
	{ok, Result} ->
	    Result;
	Error ->
	    throw({execution_error, Error})
    end.

list_account_ids(Rows) ->
    Ids = lists:foldl(fun({_, AccountId, _, Dst}, Acc) ->
			      [AccountId, Dst|Acc]
		      end, [], Rows),
    lists:usort(Ids).

lock_profiles(Connection, #{lock_profiles := Statement}) ->
    execute_statement(Connection, Statement, []).

insert_profiles(Connection, AccountIds, #{insert_profiles := Statement}) ->
    execute_statement(Connection, Statement, [AccountIds]).

list_profiles(Connection, AccountIds, #{profiles := Statement}) ->
    Rows = execute_statement(Connection, Statement, [AccountIds]),
    dict:from_list(Rows).

exclude_existed_events(Connection, Rows, #{exclude_existed := Statement}) ->
    Ids = lists:map(fun({Id, _, _, _}) ->
			    Id
		    end, Rows),
    Result = execute_statement(Connection, Statement, [Ids]),
    case Result of
	[_|_] ->
	    Dict = dict:from_list([{Id, Row} || {Id, _, _, _} = Row <- Rows]),
	    Set = sets:subtract(
		    sets:from_list(Rows),
		    sets:from_list([dict:fetch(Id, Dict) || {Id} <- Result])),
	    sets:to_list(Set);
	[] ->
	    Rows
    end.

insert_events(Rows, Profiles, #{insert_event := Statement}) ->
    lists:map(fun({Id, AccountId, Ptime, _}) ->
		      ProfileId = dict:fetch(AccountId, Profiles),
		      {Statement, [Id, ProfileId, Ptime]}
	      end, Rows).

insert_comments(Rows, Profiles, #{insert_comment := Statement}) ->
    lists:map(fun({Id, _, _, Dst}) ->
		      DstId = dict:fetch(Dst, Profiles),
		      {Statement, [Id, DstId]}
	      end, Rows).

execute_batch(Connection, [_|_] = Batch) ->
    epgsql:execute_batch(Connection, Batch);
execute_batch(_Connection, []) ->
    empty.

%% load_mentions(Platform, Data) ->
%%     Parameters = [Platform],
%%     Fun = fun({Id, AccountId, PublicationTime, Dst}) ->
%% 		  [
%% 		   make_insert_profile_query(AccountId, Data),
%% 		   make_insert_profile_query(Dst, Data),
%% 		   make_insert_profile_event_query(Id, AccountId, PublicationTime, Data),
%% 		   make_insert_profile_event_query(profile_event_mention, Id, Dst, Data)
%% 		  ]
%% 	  end,
%%     load_command(Fun, "mentions.sql", Parameters, Data).

%% load_shares(Platform, Data) ->
%%     Parameters = [Platform],
%%     Fun = fun({Id, AccountId, PublicationTime, Dst}) ->
%% 		  [
%% 		   make_insert_profile_query(AccountId, Data),
%% 		   make_insert_profile_query(Dst, Data),
%% 		   make_insert_profile_event_query(Id, AccountId, PublicationTime, Data),
%% 		   make_insert_profile_event_type_query(profile_event_share, Id, Dst, Data)
%% 		  ]
%% 	  end,
%%     load_command(Fun, "shares.sql", Parameters, Data).

%% load_command(Statement, Filename, Parameters, #{connection := Connection}) ->
%%     QueryString = load_query_string(Filename),
%%     {ok, Query} = pgc_query:new(QueryString),
%%     Fun = fun(Rows) ->
%% 		  Transaction = fun() -> epgsql:prepared_query(Connection, Statement, [{Rows, {array, true}}]) end,
%% 		  epgsql:with_transaction(Connection, Transaction)
%% 	  end,
%%     shards:foreach(fun(ShardNumber) ->
%% 			   pgc_query:execute(Query, ShardNumber, Parameters, Fun)
%% 		   end).

%%% locate_users

init_result(Name) ->
    {ok, Cwd} = file:get_cwd(),
    Path = Cwd ++ "/" ++ Name ++ "/",
    ok = filelib:ensure_dir(Path),
    {ok, Files} = file:list_dir(Path),
    lists:foreach(fun(File) ->
			  file:delete(Path ++ File)
		  end, Files),
    Path.

save_rows(ShardNumber, Result, Path) ->
    Table = lists:concat(["shard_", ShardNumber]),
    Filepath = Path ++ Table,
    case dets:open_file(Filepath, [{type, duplicate_bag}, {file, Filepath}]) of
	{ok, Ref} ->
	    dets:insert_new(Ref, Result),
	    dets:close(Filepath);
	{error, Reason} ->
	    lager:log(
	      error, "console",
	      "error open DETS file: ~p, path: ~p, table name: ~p ",
	      [Reason, Filepath, Table]),
	    throw({error, Reason})
    end,
    Filepath.

load_query_string(Filename) ->
    PrivDir = priv_pathname(),
    Pathname = filename:join([PrivDir, Filename]),
    {ok, Statement} = file:read_file(Pathname),
    binary:bin_to_list(Statement).

priv_pathname() ->
    {ok, Application} = application:get_application(?MODULE),
    case code:priv_dir(Application) of
	{error, bad_name} -> "priv";
	PrivDir -> PrivDir
    end.

do_shard(ShardNumber, Rows, Platform, ResultDir) ->
    Data = load_data(Rows, Platform),
    Profiles = proplists:get_keys(Rows),
    ProfilesWithLocations = list_with_locations(Profiles, Data),
    SuccessfulCount = count_successful(ProfilesWithLocations, Data),
    ProfilesWithoutLocations = list_without_locations(Profiles, Data),
    Results = lists:foldl(fun(ExtId, Acc) ->
				  [locate_user(ExtId, Data)|Acc]
			  end, [], ProfilesWithoutLocations),
    lager:log(info, "console", "Расчет для шарда shard_~p закончен.~n", [ShardNumber]),
    #{
       profiles_with_locations => length(ProfilesWithLocations),
       successful_detections => SuccessfulCount,
       results => save_rows(ShardNumber, Results, ResultDir)
     }.

list_with_locations(Profiles, Data) ->
    lists:foldl(fun(ExtId, Acc) ->
			case profile_location(ExtId, Data, unspecified) of
			    {ok, Location, _} -> [{ExtId, Location}|Acc];
			    unspecified -> Acc
			end
		end, [], Profiles).

list_without_locations(Profiles, #{locations := Locations}) ->
    dict:fold(fun(ExtId, _, Acc) ->
		      lists:delete(ExtId, Acc)
	      end, Profiles, Locations).

merge_results(Result, Acc) ->
    Acc#{
      profiles_with_locations := append_profiles_with_locations(Result, Acc),
      successful_detections := append_successful_detections(Result, Acc),
      results := append_results(Result, Acc)
     }.

append_results(#{results := New}, #{results := Set}) ->
    sets:add_element(New, Set).

append_profiles_with_locations(#{profiles_with_locations := New}, #{profiles_with_locations := Old}) ->
    New + Old.

append_successful_detections(#{successful_detections := New}, #{successful_detections := Old}) ->
    New + Old.

count_thresold(#{profiles_with_locations := Count, successful_detections := Successful}) ->
    lager:log(info, "console", "Расчет порога для определения местоположения: ~p/~p~n", [Successful, Count]),
    Successful / Count.

write_results(IoDevice, Thresold, #{results := Set}) ->
    Results = sets:to_list(Set),
    lists:foreach(
      fun(Filepath) ->
	      case dets:open_file(Filepath, [{access, read}, {type, duplicate_bag}, {file, Filepath}]) of
		  {ok, Ref} ->
		      dets:traverse(
			Ref, fun(Result) ->
				     apply(fun write_result/3, [Result, Thresold, IoDevice]),
				     continue
			     end),
		      dets:close(Filepath);
		  {error, Reason} ->
		      lager:log(
			error, "console", "error read DETS file: ~p, path: ~p ",
			[Reason, Filepath]),
		      throw({error, Reason})
	      end
      end, Results).

write_result({profile, ExtId, Location}, _, IoDevice) ->
    write_line(IoDevice, ExtId, Location, <<"указано в профиле пользователя"/utf8>>, 1.0);
write_result({relation_partner, ExtId, Location}, _, IoDevice) ->
    write_line(IoDevice, ExtId, Location, <<"указано в профиле супруга"/utf8>>, 1.0);
write_result({detected, ExtId, [], _}, _, IoDevice) ->
    write_line(IoDevice, ExtId, <<""/utf8>>, <<"у друзей нет ни одного указанного местоположения"/utf8>>, 0.0);
write_result({detected, ExtId, [_|_] = Locations, Frequency}, Thresold, IoDevice) ->
    if Frequency >= Thresold ->
	    lists:foreach(fun(Location) ->
				  write_line(IoDevice, ExtId, Location, <<"одно из наиболее частых местоположений"/utf8>>, Frequency)
			  end, Locations);
       Frequency < Thresold ->
	    ok
    end.

load_data(Rows, Platform) ->
    {Keys, Values} = lists:unzip(Rows),
    Profiles = lists:usort(Keys ++ Values),
    Data = get_profiles(Profiles, Platform, #{
				    relation_partners => dict:new(),
				    locations => dict:new(),
				    friends => load_friends(Rows)
				   }),
    load_relation_partners(Data, Platform).

friends(ExtId, #{friends := Friends}) ->
    dict:fetch(ExtId, Friends).

write_line(IoDevice, ExtId, Location, Description, Frequency) ->
    io:fwrite(IoDevice, "\"~s\",\"~s\",\"~s\",~e~n", [ExtId, Location, Description, Frequency]).

count_successful(ProfilesWithLocations, Data) ->
    lists:foldl(fun({ExtId, Location}, Count) ->
			{_, MostFrequentLocations} = most_frequent_locations(ExtId, Data),
			case lists:member(Location, MostFrequentLocations) of
			    false -> Count;
			    true -> Count + 1
			end
		end, 0, ProfilesWithLocations).

locate_user(ExtId, Data) ->
    case profile_location(ExtId, Data, unspecified) of
	{ok, Location, profile} ->
	    {profile, ExtId, Location};
	{ok, Location, relation_partner} ->
	    {relation_partner, ExtId, Location};
	unspecified ->
	    {Frequency, MostFrequentLocations} =
		most_frequent_locations(ExtId, Data),
	    {detected, ExtId, MostFrequentLocations, Frequency}
    end.

most_frequent_locations(ExtId, Data) ->
    Dict = locations_by_occurrence(ExtId, Data),
    Size = dict:size(Dict),
    if Size > 0 ->
	    Tree = dict:fold(fun gb_trees:enter/3, gb_trees:empty(), Dict),
	    gb_trees:largest(Tree);
       Size == 0 ->
	    {0.0, []}
    end.

locations_by_occurrence(ExtId, Data) ->
    Friends = friends(ExtId, Data),
    Dict = lists:foldl(fun(FriendId, Acc) ->
			       case profile_location(FriendId, Data, unspecified) of
				   {ok, FriendLocation, _} ->
				       dict:update_counter(FriendLocation, 1, Acc);
				   unspecified ->
				       Acc
			       end
		       end, dict:new(), Friends),
    FriendsWithLocations = dict:fold(fun(_, Count, Total) ->
					     Count + Total
				     end, 0, Dict),
    dict:fold(fun(Location, Count, Acc) ->
		      dict:append(Count / FriendsWithLocations,  Location, Acc)
	      end, dict:new(), Dict).

profile_location(ExtId, Data, Default) ->
    case location(ExtId, Data, unspecified) of
	{ok, Location} ->
	    {ok, Location, profile};
	unspecified ->
	    case relation_partner(ExtId, Data, unspecified) of
		{ok, RelationPartner} ->
		    case location(RelationPartner, Data, unspecified) of
			{ok, Location} ->
			    {ok, Location, relation_partner};
			unspecified ->
			    Default
		    end;
		unspecified ->
		    Default
	    end
    end.

location(ExtId, #{locations := Locations}, Default) ->
    case dict:find(ExtId, Locations) of
	{ok, Location} -> {ok, Location};
	error -> Default
    end.

relation_partner(ExtId, #{relation_partners := RelationPartners}, Default) ->
    case dict:find(ExtId, RelationPartners) of
	{ok, RelationPartner} -> {ok, RelationPartner};
	error -> Default
    end.

load_friends(Rows) ->
    lists:foldl(fun({ExtId, FriendId}, Acc) ->
			dict:append(ExtId, FriendId, Acc)
		end, dict:new(), Rows).
load_relation_partners(Data, Platform) ->
    ExtIds = list_relation_partners(Data),
    get_profiles(ExtIds, Platform, Data).

get_profiles(ExtIds, Platform, Data) ->
    {ok, NgQueue} = ng_query:new(select_author),
    Profiles = lists:foldl(fun(ExtId, Acc) ->
				   case cache:get(cache, ExtId) of
				       undefined ->
					   ng_query:execute(
					     NgQueue, [drop_platform(ExtId, Platform), Platform], fun load_profiles/1),
					   Acc;
				       Profile ->
					   [Profile|Acc]
				   end
			   end, [], ExtIds),
    %%lager:log(info, "console", "Из кэша: ~p, всего: ~p~n", [length(Profiles), length(ExtIds)]),
    lists:foldl(fun(Profile, #{relation_partners := RelationPartners, locations := Locations} = Acc) ->
			Acc#{
			  relation_partners := load_relation_partner_dict(Profile, RelationPartners),
			  locations := load_locations(Profile, Locations)
			 }
		end, Data, Profiles ++ ng_query:close(NgQueue, fun lists:append/1)).

drop_platform(AuthorId, Platform) ->
    case binary:match(AuthorId, <<"@", Platform/binary>>) of
	{Pos, _} ->
	    binary:part(AuthorId, 0, Pos);
	nomatch ->
	    throw({wrong_platform, AuthorId, Platform})
    end.

load_profiles([_|_] = Rows) ->
    lists:foldl(fun load_profile/2, [], Rows);
load_profiles([]) ->
    [].

load_profile([ExtId, Platform, Location, RelationPartnerId], Acc) ->
    AuthorId = <<ExtId/binary, <<"@">>/binary, Platform/binary>>,
    Profile = #{author_id => AuthorId},
    Loaded = maps:merge(
	       load_relation_partner(RelationPartnerId, Platform, Profile),
	       load_location(Location, Profile)
	      ),
    cache:put(cache, AuthorId, Loaded),
    [Loaded|Acc].

load_relation_partner(<<JsonString/binary>>, Platform, Profile) when size(JsonString) > 0 ->
    #{<<"id">> := Id} = jiffy:decode(JsonString, [return_maps]),
    ExtId = extid_to_binary(Id),
    RelationPartnerId = <<ExtId/binary, <<"@">>/binary, Platform/binary>>,
    Profile#{relation_partner => RelationPartnerId};
load_relation_partner(null, _Platform, Profile) ->
    Profile;
load_relation_partner(<<>>, _Platform, Profile) ->
    Profile.

load_location(<<Location/binary>>, Profile) when size(Location) > 0 ->
    Profile#{location => Location};
load_location(null, Profile) ->
    Profile;
load_location(<<>>, Profile) ->
    Profile.

extid_to_binary(ExtId) when is_binary(ExtId) ->
    ExtId;
extid_to_binary(ExtId) when is_integer(ExtId) ->
    erlang:integer_to_binary(ExtId).

load_relation_partner_dict(#{author_id := AuthorId} = Profile, RelationPartners) ->
    case maps:find(relation_partner, Profile) of
	{ok, RelationPartner} -> 
	    dict:store(AuthorId, RelationPartner, RelationPartners);
	error ->
	    RelationPartners
    end.

load_locations(#{author_id := AuthorId} = Profile, Locations) ->
    case maps:find(location, Profile) of
	{ok, Location} -> 
	    dict:store(AuthorId, Location, Locations);
	error ->
	    Locations
    end.

list_relation_partners(#{relation_partners := RelationPartners}) ->
    RelationPartnerIds = dict:fold(fun(_, RelationPartnerId, Acc) ->
					   [RelationPartnerId|Acc]
				   end, [], RelationPartners),
    lists:usort(RelationPartnerIds).

chunks(Fun, [_|_] = List) ->
    Length = length(List),
    ChunkLength = round(math:sqrt(Length) / 2),
    Chunks = chunks(List, ChunkLength, []),
    lists:foreach(Fun, Chunks);
chunks(_, []) ->
    ok.

chunks([_|_] = List, ChunkLength, Chunks) when length(List) >= ChunkLength ->
    {Chunk, Rest} = lists:split(ChunkLength, List),
    chunks(Rest, ChunkLength, [Chunk|Chunks]);
chunks([_|_] = List, ChunkLength, Chunks) when length(List) < ChunkLength ->
    [List|Chunks];
chunks([], _, Chunks) ->
    Chunks.
