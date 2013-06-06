%%% @author Eric Moritz <eric@themoritzfamily.com>
%%% @copyright (C) 2013, Eric Moritz
%%% @doc
%%%
%%% @end
%%% Created :  5 Jun 2013 by Eric Moritz <eric@themoritzfamily.com>

-module(rafterl_log).

-export([new/1, last_index/1, append_entry/3, entry/2]).

-define(INDEX_KEY, <<"last_index">>).

-type log() :: eleveldb:db_ref().
-type write_result() :: ok | {error, any()}.
-type get_result() :: {ok, any()} | not_found | {error, any()}.
-type index_result() :: {ok, integer()} | {error, any()}.

-spec new(Filename :: binary()) -> {ok, log()} | {error, any()}.
new(Filename) ->
    eleveldb:open(
      Filename,
      [{create_if_missing, true}]
     ).

-spec last_index(log()) -> index_result().
last_index(Log) ->
    get_term(Log, ?INDEX_KEY).

-spec append_entry(log(), Term :: integer(), Entry :: any()) -> ok | {error, any()}.
append_entry(Log, Term, Entry) ->
    NextIndex = next_index(Log),
    StoreEntry = store_entry(Log, NextIndex, Term, Entry),
    update_index(Log, NextIndex, StoreEntry).
    
-spec entry(log(), integer()) -> {ok, {integer(), any()}} | not_found | {error, any()}.
entry(Log, Index) ->
    get_term(Log, Index).

%%===================================================================
%% Internal functions
%%===================================================================
-spec next_index(log()) -> index_result().
next_index(Log) ->
    case last_index(Log) of
	{ok, LastIndex} ->
	    {ok, LastIndex + 1};
	E={error, _} ->
	    E
    end.

-spec update_index(log(), index_result(), write_result()) -> write_result().
update_index(Log, {ok, Index}, {ok, _}) ->
    write_term(Log, ?INDEX_KEY, Index);
update_index(_, E={error, _}, _) ->
    % index_result() error case
    E;
update_index(_, _, E={error, _}) ->
    % store_entry() error case
    E.


-spec store_entry(log(), index_result(), 
		  Term :: integer(), Entry :: any()) -> write_result().
store_entry(Log, {ok, Index}, Term, Entry) ->
    write_term(Log, Index, {Term, Entry});
store_entry(_, E={error, _}, _, _) ->
    E.

-spec get_term(log(), binary()) -> get_result().
get_term(Log, Key) ->
    case eleveldb:get(Log, Key) of
	{ok, BERT} ->
	    {ok, binary_to_term(BERT)};
	not_found ->
	    not_found;
	E={error, _} ->
	    E
    end.

-spec write_term(log(), binary(), any()) -> write_result().
write_term(Log, Key, Term) ->
    eleveldb:write(Log, Key, term_to_binary(Term)).
