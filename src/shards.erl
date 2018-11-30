-module(shards).

-define(MAX_SHARD_NUMBER, 4095).

-define(SHARD_COUNT, 4096).

-export([foreach/1, list/0]).

%%====================================================================
%% API functions
%%====================================================================

foreach(Fun) ->
    ShardNumbers = list(),
    lists:foreach(Fun, ShardNumbers).

list() ->
    Numbers = lists:seq(0, ?MAX_SHARD_NUMBER),
    %Count = length(Numbers),
    %[round(Number * ?SHARD_COUNT / Count) || Number <- Numbers].
    Numbers.

%%====================================================================
%% Internal functions
%%====================================================================

shard_number(ExtId) ->
    AccountHash = account_hash(ExtId),
    binary:decode_unsigned(AccountHash, little) rem 4096.

account_hash(ExtId) ->
    siphash:hash24(<<0:128>>, ExtId).
