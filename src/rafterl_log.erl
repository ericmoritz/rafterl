%%% @author Eric Moritz <eric@eric-acer>
%%% @copyright (C) 2013, Eric Moritz
%%% @doc
%%% A really stupid, niave implementation of a event log
%%% @end
%%% Created : 24 May 2013 by Eric Moritz <eric@eric-acer>

-module(rafterl_log).


-export([
	 new/0,
	 last_index/1,
	 append/2,
	 update/3,
	 get/2
]).

-type entry() :: any().

-record(rafterl_log, {
	  index=0,
	  items=dict:new()
}).

new() ->
    #rafterl_log{}.

-spec last_index(#rafterl_log{}) -> integer().
last_index(#rafterl_log{index=N}) -> N.

-spec append(Log :: #rafterl_log{}, Entry :: entry()) -> {ok, #rafterl_log{}}.
append(Log=#rafterl_log{index=N, items=Items}, Entry) ->
    Items2 = dict:store(
	       N+1,
	       Entry,
	       Items
	      ),
    {ok, Log#rafterl_log{index=N+1, items=Items2}}.
	
      
-spec update(Log :: #rafterl_log{}, Index :: integer(), Entry :: entry()) -> {ok, #rafterl_log{}} | {error, out_of_bounds}.
update(#rafterl_log{index=N}, Index, _) when Index > N ->
    {error, out_of_bounds};
update(Log=#rafterl_log{items=Items}, Index, Entry) ->
    Items2 = dict:store(
	       Index,
	       Entry,
	       Items
	      ),
    {ok, Log#rafterl_log{items=Items2}}.
		    


-spec get(Log :: #rafterl_log{}, Index :: integer()) -> {ok, entry()} | {error, notfound}.
get(#rafterl_log{items=Items}, Index) ->
    case dict:find(Index, Items) of
	error ->
	    {error, notfound};
	Entry ->
	    {ok, Entry}
    end.
		 
