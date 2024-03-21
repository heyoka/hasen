%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2024
%%% @doc
%%% manages a pool of rabbit_connection processes, especially starts, grows and shrinks the pool via
%%% the hasen_pool_sup supervisor
%%% @end
%%%-------------------------------------------------------------------
-module(hasen_handler).

-behaviour(gen_server).

-include("hasen.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-export([start_link/3, start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(INITIAL_SIZE, 3).

-record(state, {
   initial_size = ?INITIAL_SIZE,
   %% amqp connection parameters
   opts :: #amqp_params_network{},
   %% pool-key
   pool_key :: term(),
   %% max number of channels per connection
   max_channels :: pos_integer(),
   %% rabbit_connection processes, that have free channel capacity
   table_available :: atom(),
   %% local map of connection processes, currently available
   pool = #{},
   %% grow multiplier
   grow = 2 :: pos_integer(),
   %% shrink multiplier
   shrink = 4 :: pos_integer(),
   %% conn processes with full capacity (no channels)
   empty_conns = [] :: list(pid()),
   %% stop candidate
   stop_candidate = undefined :: pid()
}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

-spec start_link(#amqp_params_network{}, pos_integer(), pos_integer()) -> {ok, pid()} | {error, term()}.
start_link(AMQPOpts = #amqp_params_network{}, InitialPoolSize, MaxChannels) ->
   gen_server:start_link(?MODULE, [AMQPOpts, InitialPoolSize, MaxChannels], []).

-spec start_link(Opts :: map()) -> {ok, pid()} | {error, term()}.
start_link(Opts = #{}) ->
   gen_server:start_link(?MODULE, [Opts], []).

init([#{
   amqp_opts := AMQPOpts, pool_key := PoolKey,
   max_channels := MaxChannels, initial_size := InitialSize,
   grow := Grow, shrink := Shrink}]) ->

   lager:notice("[~p] starting ...", [?MODULE]),
   TableAvailable = hasen_ets:table_name(?ETS_POOL_AVAILABLE, PoolKey, true),

   State = #state{
      opts = AMQPOpts,
      pool_key = PoolKey,
      max_channels = MaxChannels,
      initial_size = InitialSize,
      table_available = TableAvailable,
      grow = Grow,
      shrink = Shrink
   },

   %% init (with) ets table for this pool key
   Pool =
      case hasen_ets:exists(TableAvailable) of
         true -> maps:from_list(ets:tab2list(TableAvailable));
         false ->
            %% first start
            ok = hasen_ets:new_table(TableAvailable, ordered_set),
            add_initial(State),
            #{}
      end,

   {ok, State#state{pool = Pool}}.


handle_call(_Request, _From, State = #state{}) ->
   {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
   {noreply, State}.



handle_info(stop_conn, State = #state{stop_candidate = undefined}) ->
   {noreply, State};
handle_info(stop_conn, State = #state{stop_candidate = Conn, empty_conns = Empty}) ->
   NewState =
   case catch rabbit_connection:prepare_stop(Conn) of
      true ->
         rabbit_connection:stop(Conn),
         NewEmpty = lists:delete(Conn, Empty),
         State#state{empty_conns = NewEmpty};
      false ->
         lager:warning("cannot stop connection process ~p", [Conn]),
         %% no not this time
         State;
      Other ->
         lager:warning("ERROR when calling rabbit_connection: ~p",[Other]),
         NewEmpty = lists:delete(Conn, Empty),
         State#state{empty_conns = NewEmpty}
   end,
   {noreply, NewState#state{stop_candidate = undefined}};

handle_info({available, {Worker, Capacity}}, State = #state{pool = Pool, max_channels = Max, empty_conns = Empty0}) ->
%%   lager:info("got AVAILABLE from worker ~p",[{Worker, Capacity}]),
   NewPool = Pool#{Worker => Capacity},
   Empty = lists:delete(Worker, Empty0),
   NewEmptyConns =
   case Capacity of
      Max -> lists:append(Empty, [Worker]);
      _Other -> Empty
   end,
   NewState = check_capacity_more(State#state{pool = NewPool, empty_conns = NewEmptyConns}),
   {noreply, NewState};

handle_info({unavailable, Worker}, State = #state{pool = Pool, empty_conns = Empty}) ->
   lager:info("got UNAVAILABLE from worker ~p",[Worker]),
   NewPool = maps:without([Worker], Pool),
   NewEmpty = lists:delete(Worker, Empty),
   NewState = check_capacity_less(State#state{pool = NewPool, empty_conns = NewEmpty}),
   {noreply, NewState};

handle_info(_Info, State = #state{}) ->
   {noreply, State}.

terminate(_Reason, #state{}) ->
   lager:warning("[~p] terminates with reson: ~p",[?MODULE, _Reason]).

code_change(_OldVsn, State = #state{}, _Extra) ->
   {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

add_worker(S=#state{opts = Opts, max_channels = MaxChannels, pool_key = Key}) ->
   lager:notice("add worker with max channels ~p",[MaxChannels]),
   hasen_pool_sup:start_child(Key, Opts, MaxChannels),
   S.

add_workers(0, State) ->
   State;
add_workers(Num, State) ->
   S = add_worker(State),
   add_workers(Num-1, S).

add_initial(State = #state{initial_size = Initial}) ->
   add_workers(Initial, State).

check_capacity_less(State = #state{max_channels = MaxChannels, grow = Grow}) ->
   Total = total_capacity(State),
   case Total < MaxChannels * Grow of
      true ->
         add_worker(State);
      false -> ok
   end,
   State.

check_capacity_more(State = #state{stop_candidate = C}) when is_pid(C) ->
   State;
check_capacity_more(State = #state{empty_conns = []}) ->
   State;
check_capacity_more(State = #state{max_channels = MaxChannels, shrink = Shrink, empty_conns = [Conn | _]=EC}) ->
   Total = total_capacity(State),
   lager:notice("total capacity: ~p empty: ~p",[Total, EC]),
   case Total > MaxChannels * Shrink of
      true ->
         erlang:send_after(0, self(), stop_conn),
         lager:warning("attempt to stop worker ~p , because capacity is ~p",[Conn, Total]),
         State#state{stop_candidate = Conn};
      false ->
         State
   end.

-spec total_capacity(State :: #state{}) -> non_neg_integer().
total_capacity(#state{pool = Connections}) ->
   maps:fold(
      fun(_Pid, Capacity, Total) ->
         Capacity + Total
      end,
      0,
      Connections
   ).
