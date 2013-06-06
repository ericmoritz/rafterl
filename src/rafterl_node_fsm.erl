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
-export([start_link/0, set_cluster/2, 
	 request_vote/5, vote/4, append_entries/7, append_entries_reply/4]).

%% gen_fsm callbacks
-export([init/1, candidate/2, handle_event/3,
	 handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).

-record(state, {
	  current_term=0,
	  voted_for=undefined,
	  log,
	  nodes,
	  timeout_ref,
	  current_election
	 }).

-record(election, {
	  majority,
	  votes_for=ordsets:new()
	 }).

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
-spec request_vote(NodePid :: pid(), Term :: any(), 
		   CandidatePid :: pid(), 
		   LastLogIndex :: integer(), LastLogTerm :: any())
		  -> ok.
request_vote(NodePid, Term, CandidatePid, LastLogIndex, LastLogTerm) -> 
    gen_fsm:send_event(
      NodePid,
      {request_vote, Term, CandidatePid, LastLogIndex, LastLogTerm}
     ).
      
%%--------------------------------------------------------------------
%% @doc
%% Vote for a candidate
%% @end
%%--------------------------------------------------------------------
-spec vote(CandidatePid :: pid(), NodePid :: pid(), 
	   Term :: any(), VoteGranted :: boolean()) ->
		  ok.
vote(CandidatePid, NodePid, Term, VoteGranted) ->
    gen_fsm:send_event(
      CandidatePid,
      {vote, NodePid, Term, VoteGranted}
     ).

%%--------------------------------------------------------------------
%% @doc
%% Append a log entry, called by the leader
%% @end
%%--------------------------------------------------------------------
-spec append_entries(
	NodePid :: pid(), 
	Term :: any(), 
	LeaderPid :: pid(),
	PrevLogIndex :: integer(),
	PrevLogTerm :: integer(),
	Entries :: [any()],
	CommitIndex :: integer()) -> ok.
append_entries(NodePid, Term, LeaderPid, PrevLogIndex, PrevLogTerm, Entries, CommitIndex) ->
    gen_fsm:send_event(
      NodePid,
      {append_entry,
       {Term, LeaderPid, PrevLogIndex, PrevLogTerm, Entries, CommitIndex}
      }
     ).

%%--------------------------------------------------------------------
%% @doc
%% Reply to the append log entry request
%% @end
%%--------------------------------------------------------------------
append_entries_reply(LeaderPid, FollowerPid, Term, Success) ->
    gen_fsm:send_event(
      LeaderPid,
      {append_entry_reply,
       {FollowerPid, Term, Success}
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
candidate({vote, NodePid, Term, VoteGranted}, State=#state{current_term=CurrentTerm}) ->
    % record vote
    State1 = record_vote(NodePid, VoteGranted, State),
    % whats the election status?
    case election_status(Term, CurrentTerm, State1#state.current_election) of
	winner ->
	    % become the leader of these slobs
	    State2 = reset_timeout(State1),
	    {next_state, leader, State2};
	step_down ->
	    % crap, someone is already a leader, heed to their authority
	    State2 = step_down(State1),
	    {next_state, follower, State2};
	undetermined ->
	    % continue being candidate
	    {next_state, candidate, State1}
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
%% step down, resets the timeout and clear election
%% @end
%%--------------------------------------------------------------------
step_down(State) ->
    State1 = reset_timeout(State),
    State1#state{current_election=undefined}.

%%--------------------------------------------------------------------
%% @doc
%% Starts the election and broadcasts requestvote events to all
%% the nodes
%% @end
%%--------------------------------------------------------------------
-spec broadcast_election(pid(), #state{}) -> #state{}.
broadcast_election(CandidatePid, State=#state{log=Log, nodes=Nodes}) ->
    LastLogIndex = rafterl_log:last_index(Log),
    State1 = new_election(CandidatePid, State),
    State2 = reset_timeout(State1),
    CurrentTerm = State2#state.current_term,
    {LastLogTerm, _} = rafterl_log:entry(Log, LastLogIndex),
    do_vote_broadcast(
      Nodes, 
      CurrentTerm, 
      CandidatePid,
      LastLogIndex, 
      LastLogTerm
     ),
    State2.

%%--------------------------------------------------------------------
%% @doc
%% Creates a new election state
%% @end
%%--------------------------------------------------------------------
new_election(CandidateId, State=#state{nodes=Nodes, current_term=PreviousTerm}) ->
    Majority = size(Nodes) div 2 + 1,
    State#state{
      current_term=PreviousTerm+1,
      voted_for=CandidateId,
      current_election=#election{
	majority=Majority
       }
     }.

%%--------------------------------------------------------------------
%% @doc
%% record the vote in the state
%% @end
%%--------------------------------------------------------------------
record_vote(NodePid, true, State=#state{current_election=Election}) ->
    VotesFor = Election#election.votes_for,
    VotesFor2 = ordsets:add_element(NodePid, VotesFor),
    Election2 = Election#election{votes_for=VotesFor2},
    State#state{current_election=Election2};
record_vote(_, false, State) ->
    % ignore non-granted votes
    State.

	

%%--------------------------------------------------------------------
%% @doc
%% What is the status of the election?
%% @end
%%--------------------------------------------------------------------
election_status(CurrentTerm, Term, _) when Term > CurrentTerm ->
    step_down;
election_status(_, _, #election{majority=Majority, votes_for=Votes}) ->
    VoteForCount = ordsets:size(Votes),
    case VoteForCount >= Majority of
	true ->
	    winner;
	false ->
	    undetermined
    end.
%%--------------------------------------------------------------------
%% @doc
%% Actually do the request of the votes
%% @end
%%--------------------------------------------------------------------
do_vote_broadcast(Nodes, CurrentTerm, CandidatePid, LastLogIndex, LastLogTerm) ->
    % TODO: Create a FSM for each pending vote that repeatably sends
    % request vote messages 
    % 
    % 5.2 "If the candidate receives no response for an RPC, it
    % reissues the RPC repeatedly until a response arrives or the
    % election concludes
    _ = [request_vote(
       NodePid,
       CurrentTerm,
       CandidatePid,
       LastLogIndex,
       LastLogTerm
      ) || NodePid <- Nodes],
    ok.
