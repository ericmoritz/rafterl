%%%-------------------------------------------------------------------
%%% @author Eric Moritz <eric@eric-acer>
%%% @copyright (C) 2013, Eric Moritz
%%% @doc
%%%
%%% @end
%%% Created :  5 Jun 2013 by Eric Moritz <eric@eric-acer>
%%%-------------------------------------------------------------------
-module(rafterl_node_fsm).

-behaviour(gen_fsm).

%% API
-export([start_link/0, set_cluster/2]).

%% gen_fsm callbacks
-export([init/1, handle_event/3,
	 handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).

-record(state, {
	  log,
	  nodes,
	  timeout_ref,
	  election_pid
	 }).

-record(vote_request, 
	{election_pid, term, candidate_pid, last_log_index, last_log_term}
       ).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_fsm:start_link(?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc
%% Updates the cluster configuration for this node
%% @end
%%--------------------------------------------------------------------
-spec set_cluster(NodePid :: pid(), [pid()]) -> ok.
set_cluster(NodePid, ClusterPids) ->
    gen_fsm:sync_send_all_state_event(
      NodePid,
      {set_cluster, ClusterPids}
     ).

%%--------------------------------------------------------------------
%% @doc
%% Requests a vote from a node
%% @end
%%--------------------------------------------------------------------
-spec request_vote(NodePid :: pid(), ElectionPid :: pid(),
		   Term :: integer(),
		   CandidatePid :: pid(), 
		   LastLogIndex :: integer(), LastLogTerm :: any())
		  -> ok.
request_vote(NodePid, ElectionPid, Term, CandidatePid, LastLogIndex, LastLogTerm) -> 
    gen_fsm:send_all_state_event(
      NodePid,
      #vote_request{
	election_pid=ElectionPid,
	term=Term, 
	candidate_pid=CandidatePid, 
	last_log_index=LastLogIndex, 
	last_log_term=LastLogTerm
       }
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
init([]) ->
    {ok, state_name, #state{}}.

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
state_name(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, state_name, State}.

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
handle_event(Vote=#vote_request{}, StateName, State=#state{log=Log}) ->
    % extract the needed info from the log
    {ok, CurrentTerm} = rafterl_log:current_term(Log),
    {ok, VotedFor} = rafterl_log:voted_for(Log),
    {ok, LastIndex} = rafterl_log:last_index(Log),
    {ok, LastTerm} = rafterl_log:last_term(Log),
    
    UpdateTerm = Vote#vote_request.term > CurrentTerm,
    StepDown = StateName =/= follower andalso UpdateTerm,
    GrantVote = grant_vote(Vote, CurrentTerm, VotedFor, LastIndex, LastTerm),

    % Update the term if we need to
    CurrentTerm2 = if UpdateTerm ->
			   rafterl_log:last_term(Log, Vote#vote_request.term),
			   Vote#vote_request.term;
		      true ->
			   CurrentTerm
		   end,
    {StateName2, State2} = if StepDown ->
				 {follower, step_down(State)};
			    true ->
				 {StateName, State}
			 end,
    % send the vote
    rafterl_log:vote(
      Vote#vote_request.election_pid,
      self(),
      CurrentTerm2,
      GrantVote
     ),
    {next_state, StateName2, State2}.
       
    
    
    

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
handle_sync_event({set_cluster, ClusterPids}, _From, init, State) ->
    Reply = ok,
    State2 = reset_timeout(State),
    {reply, Reply, init, State2#state{nodes=ClusterPids}};
handle_sync_event({set_custer, _}, _From, StateName, State) ->
    % TODO: Handle two phase reconfigure request
    {reply, {error, already_configured}, StateName, State}.


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
handle_info(election_timeout, _Any, State) ->
    State2 = broadcast_election(self(), State),
    {next_state, candidate, State2}.

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
-spec grant_vote(#vote_request{}, 
		 CurrentTerm :: integer(), 
		 VotedFor :: any(),
		 LastIndex :: integer(), 
		 LastEntry :: any()) 
		->
			{VoteTermStale :: boolean(),
			 UpdateCurrentTerm :: boolean(),
			 GrantVote :: boolean()}.

grant_vote(Vote, CurrentTerm, VotedFor, LastIndex, LastEntry) ->
    Vote#vote_request.term >= CurrentTerm andalso
	can_vote_for(VotedFor, Vote#vote_request.candidate_pid) andalso
	candidate_log_is_as_complete(Vote, LastIndex, LastEntry).

can_vote_for(undefined, CandidatePid) ->
    true;
can_vote_for(CandidatePid, CandidatePid) ->
    true;
can_vote_for(_, _) ->
    false.

candidate_log_is_as_complete(Vote, LastIndex, LastEntry) ->
    % TODO
    false.
	

%%--------------------------------------------------------------------
%% @doc
%% Resets the timeout (duh)
%% @end
%%--------------------------------------------------------------------
-spec reset_timeout(#state{}) -> #state{}.
reset_timeout(State=#state{timeout_ref=undefined}) ->
    % pick a random timeout between 150 and 300ms (§5.2)
    Timeout = 149 + random:uniform(150),
    % Scheduled a the election timeout
    Ref = erlang:send_after(
	    Timeout,
	    self(),
	    election_timeout
	   ),
    % update the ref in the state
    State#state{timeout_ref=Ref};
reset_timeout(State=#state{timeout_ref=Ref}) ->
    % cancel the existing timer
    _ = erlang:cancel_timer(Ref),

    % continue the reset
    reset_timeout(
      State#state{timeout_ref=undefined}
     ).

%%--------------------------------------------------------------------
%% @doc
%% Step Down from being a leader or a candidate
%% @end
%%--------------------------------------------------------------------
step_down(State) ->
    reset_timeout(
      cancel_election(
	State
       )
     ).

%%--------------------------------------------------------------------
%% @doc
%% Cancel any existing elections
%% @end
%%--------------------------------------------------------------------
cancel_election(State=#state{election_pid=undefined}) ->
    State;
cancel_election(State=#state{election_pid=Pid}) ->
    rafterl_node_fsm:cancel(Pid),
    State#state{election_pid=undefined}.


broadcast_election(CandidatePid, State) ->
    VoterCount = length(State#state.nodes),
    Term = rafterl_log:term(State#state.log),
    LastLogIndex = rafterl_log:last_index(State#state.log),

    {ok, Pid} = rafterl_election_fsm:start_link(
		  VoterCount,
		  Term
		 ),
    _ = [request_vote(NodePid, Pid, Term, self(), LastLogIndex, Term) ||
	    NodePid <- State#state.nodes],
    
    State#state{election_pid=Pid}.
