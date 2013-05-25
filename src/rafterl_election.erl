%%% @author Eric Moritz <eric@eric-acer>
%%% @copyright (C) 2013, Eric Moritz
%%% @doc
%%%
%%% @end
%%% Created : 24 May 2013 by Eric Moritz <eric@eric-acer>

-module(rafterl_election).


-export([
	 new/1,
	 vote/2,
	 won/1
]).


-record(rafterl_election, {majority, counted, votes}).

new(Voters) ->
    #rafterl_election{
     majority=Voters div 2,
     counted=0,
     votes=0
    }.

vote(true, Election=#rafterl_election{counted=C, votes=V}) ->
    Election#rafterl_election{
      counted=C+1,
      votes=V+1
     };
vote(false, Election=#rafterl_election{counted=C}) ->
    Election#rafterl_election{
      counted=C+1
    }.

won(#rafterl_election{votes=V, majority=M}) ->
    V > M.
