%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 10. Mär 2024 21:17
%%%-------------------------------------------------------------------
-module(hasen_pool_sup).
-author("heyoka").

-behaviour(supervisor).

%% API
-export([start_link/1, start_child/3]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%% @doc Starts the supervisor
-spec(start_link(term()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Name) ->
   supervisor:start_link({local, Name}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @private
%% @doc Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
-spec(init(Args :: term()) ->
   {ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(),
      MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
      [ChildSpec :: supervisor:child_spec()]}}
   | ignore | {error, Reason :: term()}).
init([]) ->
   MaxRestarts = 1000,
   MaxSecondsBetweenRestarts = 3600,
   SupFlags = #{strategy => one_for_one,
      intensity => MaxRestarts,
      period => MaxSecondsBetweenRestarts},
   {ok, {SupFlags, []}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
start_child(Key, AmqpOpts, MaxChannels) ->
   ChildId0 = integer_to_list(new_child_index(Key)),
   ChildId = "hasen_worker_" ++ Key ++ "_" ++ ChildId0,
   lager:info("start child with id ~p",[ChildId]),

   Child =
      #{id => ChildId,
      start => {rabbit_connection, start_link, [Key, self(), AmqpOpts, MaxChannels]},
      restart => permanent,
      shutdown => 3000,
      type => worker},

   supervisor:start_child(get_sup(Key), Child).

new_child_index(Key) ->
   ets:update_counter(pool_index, Key, 1, {Key, 0}).

get_sup(Key) ->
   case ets:lookup(pool_sups, Key) of
      [] -> {error, sup_not_found};
      [{Key, SupervisorPid}] -> SupervisorPid
   end.