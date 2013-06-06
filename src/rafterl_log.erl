%%% -*- erlang -*-
%%% @author Eric Moritz <eric@themoritzfamily.com>
%%% @copyright (C) 2013, Eric Moritz
%%% @doc
%%%
%%% @end
%%% Created :  5 Jun 2013 by Eric Moritz <eric@themoritzfamily.com>

-module(rafterl_log).

-export([new/1, 
         % log ops
         last_index/1, append_entry/3, entry/2, 

         % metadata
         voted_for/1, voted_for/2,
         term/1, term/2
        ]).

-define(METADATA_KEY, <<"metadata">>).
-define(INDEX_KEY, last_index).

-type log() :: eleveldb:db_ref().
-type write_result() :: ok | {error, any()}.
-type get_result() :: {ok, any()} | not_found | {error, any()}.
-type index_get() :: {ok, integer()} | {error, any()}.
-type metadata_get() :: {ok, dict()} | {error, any()}.

-spec new(Filename :: binary()) -> {ok, log()} | {error, any()}.
new(Filename) ->
    eleveldb:open(
      Filename,
      [{create_if_missing, true}]
     ).

-spec last_index(log()) -> index_get().
last_index(Log) ->
    metadata(Log, ?INDEX_KEY).

-spec append_entry(log(), Term :: integer(), Entry :: any()) -> ok | {error, any()}.
append_entry(Log, Term, Entry) ->
    NextIndex = next_index(Log),
    StoreEntry = store_entry(Log, NextIndex, Term, Entry),
    update_index(Log, NextIndex, StoreEntry).
    
-spec entry(log(), integer()) -> {ok, {integer(), any()}} | not_found | {error, any()}.
entry(Log, Index) ->
    get_term(Log, Index).

-spec voted_for(log()) -> get_result().
voted_for(Log) ->
    metadata(Log, voted_for).

-spec voted_for(log(), CandidateId :: any()) -> write_result().
voted_for(Log, CandidateId) ->
    metadata(Log, voted_for, CandidateId).

-spec term(log()) -> get_result().
term(Log) ->
    metadata(Log, term).

-spec term(log(), any()) -> write_result().
term(Log, Term) ->
    metadata(Log, term, Term).

%%===================================================================
%% Internal functions
%%===================================================================
-spec metadata(log(), Key :: any()) -> get_result().
metadata(Log, Key) ->
    MD = get_metadata(Log),
    get_metadata_value(MD, Key).

-spec get_metadata(log()) -> metadata_get().
get_metadata(Log) ->
    % convert the not_found get_result() into a new {ok, dict()}
    % so that we'll never have a not_found for get_metadata
    case get_term(Log, ?METADATA_KEY) of
        not_found ->
            {ok, dict:new()};
        R ->
            R
    end.

-spec write_metadata(log(), dict()) -> write_result().
write_metadata(Log, MD) ->
    write_term(Log, ?METADATA_KEY, MD).

-spec metadata(log(), Key :: any(), Value :: any()) -> write_result().
metadata(Log, Key, Value) ->
    case get_metadata(Log) of
        {ok, MD} ->
            MD2 = dict:store(MD, Key, Value),
            write_metadata(Log, MD2);
        E={error, _} ->
            E
    end.

            
-spec get_metadata_value(metadata_get(), Key :: any()) -> get_result().
get_metadata_value(E={error,_}, _) ->
    E;
get_metadata_value({ok, MD}, Key) ->
    case dict:find(Key, MD) of
        error ->
            not_found;
        {ok, V} ->
            {ok, V}
    end.
                                

-spec next_index(log()) -> index_get().
next_index(Log) ->
    case last_index(Log) of
	{ok, LastIndex} ->
	    {ok, LastIndex + 1};
	E={error, _} ->
	    E
    end.

-spec update_index(log(), index_get(), write_result()) -> write_result().
update_index(Log, {ok, Index}, {ok, _}) ->
    metadata(Log, ?INDEX_KEY, Index);
update_index(_, E={error, _}, _) ->
    % index_get() error case
    E;
update_index(_, _, E={error, _}) ->
    % store_entry() error case
    E.


-spec store_entry(log(), index_get(), 
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
