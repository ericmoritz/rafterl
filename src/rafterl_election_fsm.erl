%%% -*- erlang -*-
%%%-------------------------------------------------------------------
%%% @author Eric Moritz <eric@themoritzfamily.com>
%%% @copyright (C) 2013, Eric Moritz
%%% @doc
%%%
%%% @end
%%% Created :  6 Jun 2013 by Eric Moritz <eric@themoritzfamily.com>
%%%-------------------------------------------------------------------
-module(rafterl_election_fsm).

-behaviour(gen_fsm).

%% API
-export([start_link/5, vote/4, cancel/1]).

%% gen_fsm callbacks
-export([init/1, collecting_votes/2, collecting_votes/3, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).

-record(state, {
          nodes,
          election_term,
          candidate_id,
          last_log_index,
          last_log_term,
          majority,
          votes=sets:new()
         }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Nodes, ElectionTerm, CandidateId, LastLogIndex, LastLogTerm) ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, [], [Nodes, ElectionTerm, CandidateId, LastLogIndex, LastLogTerm]).

vote(ElectionPid, VoterId, Term, VoteGrated) ->
    gen_fsm:send_event(
      ElectionPid,
      {vote, VoterId, Term, VoteGrated}
     ).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init([Nodes, ElectionTerm, CandidateId, LastLogIndex, LastLogTerm]) ->
    Majority = majority(Nodes),
    {ok, 
     collecting_votes,
     #state{
        nodes=Nodes,
        election_term=ElectionTerm,
        candidate_id=CandidateId,
        last_log_index=LastLogIndex,
        last_log_term=LastLogTerm,
        majority=Majority
       }
    }.

%%--------------------------------------------------------------------
%% @doc
%% Cancels an election
%% @end
%%--------------------------------------------------------------------
cancel(ElectionPid) ->
    gen_fsm:send_sync_event(ElectionPid, cancel).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same
%% name as the current state name StateName is called to handle
%% the event. It is also called if a timeout occurs.
%%
%% @spec state_name(Event, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
collecting_votes({vote, _VoterID, Term, _VoteGranted}, State=#state{election_term=ElectionTerm}) when Term > ElectionTerm ->
    % Voter has a higher term, stepping down
    {stop, step_down, State};
collecting_votes({vote, VoterID, _Term, true}, State=#state{majority=Majority, votes=Votes}) ->
    % record vote
    Votes2 = sets:add_element(VoterID, Votes),
    VoteCount = sets:size(Votes2),
    case VoteCount >= Majority of
        true ->
            {stop, won, State};
        false ->
            State2 = State#state{votes=Votes2},
            {next_state, collecting_votes, State2}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_event/[2,3], the instance of this function with
%% the same name as the current state name StateName is called to
%% handle the event.
%%
%% @spec state_name(Event, From, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
collecting_votes(cancel, _From, State) ->
    {stop, canceled, stopped, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
majority(Nodes) ->
    size(Nodes) div 2 + 1.