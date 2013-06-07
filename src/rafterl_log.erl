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
         voted_for/1, voted_for/2, last_term/1
        ]).

-define(METADATA_KEY, <<"metadata">>).
-define(INDEX_KEY, last_index).
-define(TERM_KEY, term).
-define(VOTED_FOR_KEY, voted_for).

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
    get_metadata(Log, ?INDEX_KEY, 0).

-spec append_entry(log(), Term :: integer(), Entry :: any()) -> ok | {error, any()}.
append_entry(Log, Term, Entry) ->
    NextIndex = next_index(Log),
    case store_entry(Log, NextIndex, Term, Entry) of
	E={error, _} ->
	    E;
	ok ->
	    update_metadata(Log, 
			    [
			     {?INDEX_KEY, NextIndex},
			     {?TERM_KEY, Term}
			    ])
    end.

    
-spec entry(log(), integer()) -> {ok, {integer(), any()}} | not_found | {error, any()}.
entry(Log, Index) ->
    get_term(Log, Index).

-spec voted_for(log()) -> get_result().
voted_for(Log) ->
    get_metadata(Log, voted_for).

-spec voted_for(log(), CandidateId :: any()) -> write_result().
voted_for(Log, CandidateId) ->
    update_metadata(Log, [{?VOTED_FOR_KEY, CandidateId}]).

-spec last_term(log()) -> get_result().
last_term(Log) ->
    get_metadata(Log, term, 0).


%%===================================================================
%% Internal functions
%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Get the metadata value for a key or not_found
%% @end
%%--------------------------------------------------------------------
-spec get_metadata(log(), Key :: any()) -> get_result().
get_metadata(Log, Key) ->
    MD = get_metadata_dict(Log),
    get_metadata_value(MD, Key).

%%--------------------------------------------------------------------
%% @doc
%% Get the metadata value for a key or return the default value
%% @end
%%--------------------------------------------------------------------
-spec get_metadata(log(), Key :: any(), Default :: any()) -> {ok, any()} | {error, any()}.
get_metadata(Log, Key, Default) ->
    MD = get_metadata_dict(Log),
    case get_metadata_value(MD, Key) of
	not_found ->
	    {ok, Default};
	{ok, Value} ->
	    {ok, Value};
	E={error, _} ->
	    E
    end.


%%--------------------------------------------------------------------
%% @doc
%% Get the metadata dict from leveldb
%% @end
%%--------------------------------------------------------------------
-spec get_metadata_dict(log()) -> metadata_get().
get_metadata_dict(Log) ->
    % convert the not_found get_result() into a new {ok, dict()}
    % so that we'll never have a not_found for get_metadata
    case get_term(Log, ?METADATA_KEY) of
        not_found ->
            {ok, dict:new()};
        R ->
            R
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns the value from the metadata dict
%% @end
%%--------------------------------------------------------------------
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

%%--------------------------------------------------------------------
%% @doc
%% Write a batch of key/value pairs to the metadata
%% @end
%%--------------------------------------------------------------------
-spec update_metadata(log(), [proplists:property()]) -> write_result().
update_metadata(Log, KVPairs) ->
    case get_metadata_dict(Log) of
        {ok, MD} ->
	    MD2 = lists:foldl(
		    fun({K,V}, Acc) -> dict:store(K, V, Acc) end,
		    MD,
		    KVPairs
		   ),
            write_metadata_term(Log, MD2);
        E={error, _} ->
            E
    end.

%%--------------------------------------------------------------------
%% @doc
%% Writes the metadata term to leveldb
%% @end
%%--------------------------------------------------------------------
-spec write_metadata_term(log(), dict()) -> write_result().
write_metadata_term(Log, MD) ->
    write_term(Log, ?METADATA_KEY, MD).


-spec next_index(log()) -> index_get().
next_index(Log) ->
    case last_index(Log) of
	E={error, _} ->
	    E;
	{ok, Index} ->
	    {ok, Index + 1}
    end.

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
