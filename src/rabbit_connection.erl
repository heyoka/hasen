%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2024
%%% @doc
%%% 'hasen' pool worker / rabbitmq connection and channel handling
%%% @end
%%%-------------------------------------------------------------------
-module(rabbit_connection).

-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("hasen.hrl").

-export([
   get_connection/1, checkout_channel/1,
   checkin_channel/2, create_channel/1, get_monitors/1,
   get_channels/1, prepare_stop/1, stop/1, start_link/4]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
   code_change/3]).

-define(MAX_CHANNELS, 15).

-record(state, {
   connection = nil,
   config,
   channels = [] :: list(), %% list(pid())
   monitors = #{} :: map(), %% #{channel-pid => monitor-ref}
   reconnector, %%:: #backoff{}
   parent :: pid(),
   max_channels :: pos_integer(),
   available = false :: boolean(),

   table_available :: atom()
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Key, Parent, Config, MaxChannels) ->
   gen_server:start_link(?MODULE, [ Key, Config, MaxChannels, Parent], []).

stop(Server) ->
   Server ! stop.

-spec get_monitors(pid()) -> {ok, map()}.
get_monitors(Server) ->
   gen_server:call(Server, get_monitors).

-spec get_channels(pid()) -> list().
get_channels(Server) ->
   gen_server:call(Server, get_channels).

-spec get_connection(pid()) -> {ok, pid()} | {error, Reason::term()}.
get_connection(Server) ->
   gen_server:call(Server, get_connection).

-spec checkout_channel(pid()) -> {ok, pid()} | {error, Reason::term()}.
checkout_channel(Server) ->
   gen_server:call(Server, checkout_channel).

-spec checkin_channel(pid(), pid()) -> no_return().
checkin_channel(Server, ChannelPid) ->
   gen_server:cast(Server, {checkin_channel, ChannelPid}).

-spec create_channel(pid()) -> {ok, pid()} | {error, Reason::term()}.
create_channel(Server) ->
   gen_server:call(Server, create_channel).

-spec prepare_stop(pid()) -> true | false | {error, Reason::term()}.
prepare_stop(Server) ->
   gen_server:call(Server, prepare_stop).

%%%===================================================================
%%% gen_server implementation
%%%===================================================================

init([PoolKey, AmqpParams, MaxChannels, CallerPid]) ->
%%   lager:warning("~p starting ~p~n" ,[?MODULE, [AmqpParams, MaxChannels, CallerPid]]),
   process_flag(trap_exit, true),
   Reconnector = backoff:new({100, 4200}),
   erlang:send_after(0, self() , connect),
   {ok, #state{
      reconnector = Reconnector,
      config = AmqpParams,
      parent = CallerPid,
      max_channels = MaxChannels,
      available = false,
      table_available = hasen_ets:table_name(?ETS_POOL_AVAILABLE, PoolKey)
   }}.

handle_call(get_monitors, _From, State = #state{monitors = Monitors}) ->
   {reply, {ok, Monitors}, State};
handle_call(get_channels, _From, State = #state{channels = Channels}) ->
   {reply, {ok, Channels}, State};
handle_call(get_connection, _From, State = #state{connection = nil}) ->
   {reply, {error, disconnected}, State};
handle_call(get_connection, _From, State = #state{connection = Conn}) ->
   {reply, {ok, Conn}, State};
handle_call(checkout_channel, _From, State = #state{connection = nil}) ->
   {reply, {error, disconnected}, State};
handle_call(checkout_channel, _From, State = #state{available = false}) ->
   {reply, {error, out_of_channels}, State};
handle_call(checkout_channel, {FromPid, _From},
    State = #state{connection = Conn, channels = Channels, monitors = Monitors}) ->
   case start_channel(Conn) of
      {ok, C} = Res ->
         link(C),
         MonitorRef = monitor(process, FromPid),
         NewMonitors = Monitors#{C => MonitorRef},
         NewState = check_capacity(State#state{channels = [C | Channels], monitors = NewMonitors}),
         {reply, Res, NewState};
      {error, Reason} = ERes ->
         lager:error("Error starting channel: ~p", [Reason]),
         {reply, ERes, State}
   end;

handle_call(create_channel, _From, State = #state{connection = Conn}) ->
   {reply, start_channel(Conn), State};
handle_call(prepare_stop, _From, State = #state{channels = [], parent = Caller}) ->
   %% disconnect, then give ok to stop
   NewState = disconnect(State),
   Caller ! {unavailable, self()},
   {reply, true, NewState};
handle_call(prepare_stop, _From, State = #state{}) ->
   {reply, false, State};
handle_call(_Request, _From, State = #state{}) ->
   {reply, ok, State}.

handle_cast({checkin_channel, ChanPid}, State = #state{monitors = Monitors, channels = Channels}) ->
   io:format("checkin_channel ~p~n",[ChanPid]),
   NewChannels = remove_channel(Channels, ChanPid),
   NewMonitors = remove_monitor(Monitors, ChanPid),
   NewState = check_capacity(State#state{monitors = NewMonitors, channels = NewChannels}),
   {noreply, NewState};
handle_cast(_Request, State = #state{}) ->
   {noreply, State}.

handle_info(connect, State) ->
   io:format("[~p] connect to rmq~n",[?MODULE]),
   NewState = start_connection(State),
   {noreply, NewState};

%% amqp connection died
handle_info({'EXIT', Conn, Reason}, State=#state{connection = Conn, reconnector = Recon, parent = Caller} ) ->
   lager:notice("RMQ Connection died with Reason: ~p",[Reason]),
   {ok, Reconnector} = backoff:execute(Recon, connect),
   Caller ! {unavailable, self()},
   {noreply, State#state{
      reconnector = Reconnector,
      connection = nil,
      available = false
   }};
%% channel died and connection already dead
handle_info({'EXIT', ChanPid, Reason}, State=#state{connection = nil} ) ->
   lager:notice("MQ channel DIED: ~p", [Reason]),
   NewState = channel_died(ChanPid, State),
   {noreply, NewState};
%% channel died, connection still alive
handle_info({'EXIT', ChanPid, Reason}, State=#state{} ) ->
   lager:notice("MQ channel DIED: ~p", [Reason]),
   NewState = channel_died(ChanPid, State),
   {noreply, NewState};
%% client holding a channel is DOWN
handle_info({'DOWN', Ref, process, _ClientPid, Info}, State=#state{channels = Channels, monitors = Monitors} ) ->
   lager:notice("Client is down: ~p", [Info]),
   case find_monitor(Monitors, Ref) of
      error ->
         {noreply, State};
      {ok, ChannelPid} ->
         NewChannels = remove_channel(Channels, ChannelPid),
         io:format("remove monitor ~p ~p ~n",[Monitors, ChannelPid]),
         NewMonitors = remove_monitor(Monitors, ChannelPid),
         NewState = check_capacity(State#state{monitors = NewMonitors, channels = NewChannels}),
         {noreply, NewState}
   end;

handle_info(stop, State = #state{}) ->
   {stop, normal, State};

handle_info(_Info, State = #state{}) ->
   {noreply, State}.

terminate(_Reason, State = #state{}) ->
   disconnect(State).

code_change(_OldVsn, State = #state{}, _Extra) ->
   {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
start_connection(State = #state{config = Config, reconnector = Recon}) ->
   case amqp_connection:start(Config) of
      {ok, Conn} ->
         link(Conn),
         check_capacity(State#state{connection = Conn, reconnector = backoff:reset(Recon)});
      {error, Error} ->
         io:format("Error starting amqp connection: ~p :: ~p",[Config, Error]),
         lager:warning("Error starting amqp connection: ~p :: ~p",[Config, Error]),
         {ok, Reconnector} = backoff:execute(Recon, connect),
         State#state{connection = nil, reconnector = Reconnector, available = false}
   end.

disconnect(State = #state{connection = nil}) ->
   State;
disconnect(State = #state{connection = Conn}) ->
   amqp_connection:close(Conn),
   State#state{connection = nil, available = false}.

start_channel(nil) ->
   {error, disconnected};
start_channel(Connection) ->
   case is_process_alive(Connection) of
      true ->
         amqp_connection:register_blocked_handler(Connection, self()),
         %% using a random channel number here, because we saw issues with using the same number
         %% after a crash where it seems like the server did not release the number yet and reports a
         %% CHANNEL_ERROR - second 'channel.open' seen (this is the case, when a channel-number is used twice)
%%       ChannelNumber = rand:uniform(?MAX_CHANNEL_NUMBER),
         amqp_connection:open_channel(Connection);
      false ->
         {error, closing}
   end.

channel_died(ChanPid, State = #state{channels = Channels, monitors = Monitors}) ->
   NewChannels = remove_channel(Channels, ChanPid),
   NewMonitors = remove_monitor(Monitors, ChanPid),
   check_capacity(State#state{monitors = NewMonitors, channels = NewChannels}).


remove_channel(Channels, Pid) ->
   lists:delete(Pid, Channels).

remove_monitor(Monitors, Pid) when is_map_key(Pid, Monitors) ->
   MonitorRef = maps:get(Pid, Monitors),
   true = erlang:demonitor(MonitorRef),
   maps:without([Pid], Monitors);
remove_monitor(Monitors, _Pid) ->
   Monitors.

-spec find_monitor(map(), reference()) -> {ok, term()} | error.
find_monitor(Monitors, MonitorRef) ->
   Flipped = maps:fold(fun(K, V, NewMap) -> NewMap#{V => K}  end, #{}, Monitors),
   maps:find(MonitorRef, Flipped).

%% we cannot provide channels
check_capacity(State = #state{channels = Chans, max_channels = Max, parent = Caller}) when length(Chans) >= Max ->
   Caller ! {unavailable, self()},
   ets:delete(State#state.table_available, self()),
   State#state{available = false};
%% we have free capacity (again)
check_capacity(State = #state{parent = Caller, max_channels = Max, channels = Chans}) ->
   Capacity = Max - length(Chans),
   Msg = {self(), Capacity},
   Caller ! {available, Msg},
   ets:insert(State#state.table_available, Msg),
   State#state{available = true}.

