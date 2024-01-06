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

-export([start_link/1, get_connection/1, checkout_channel/1, checkin_channel/2, create_channel/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
   code_change/3]).

-define(MAX_CHANNELS, 15).

-record(state, {
   connection = nil,
   config,
   channels = [],
   monitors = #{},
   reconnector
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Config) ->
   gen_server:start_link(?MODULE, [Config], []).

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

%%%===================================================================
%%% gen_server implementation
%%%===================================================================

init([Config]) ->
   io:format("~p starting~n" ,[?MODULE]),
   process_flag(trap_exit, true),
   Reconnector = backoff:new({100, 4200}),
   erlang:send_after(0, self() , connect),
   AmqpParams = amqp_options:parse(Config),
   io:format("params ~p~n",[AmqpParams]),
   {ok, #state{
      reconnector = Reconnector,
      config = AmqpParams
   }}.

handle_call(get_connection, _From, State = #state{connection = nil}) ->
   {reply, {error, disconnected}, State};
handle_call(get_connection, _From, State = #state{connection = Conn}) ->
   {reply, {ok, Conn}, State};
handle_call(checkout_channel, _From, State = #state{connection = nil}) ->
   {reply, {error, disconnected}, State};
handle_call(checkout_channel, _From, State = #state{channels = Channels}) when length(Channels) >= ?MAX_CHANNELS ->
   {reply, {error, out_of_channels}, State};
handle_call(checkout_channel, {FromPid, _From},
    State = #state{connection = Conn, channels = Channels, monitors = Monitors}) ->
   case start_channel(Conn) of
      {ok, C} = Res ->
         link(C),
         MonitorRef = monitor(process, FromPid),
         NewMonitors = Monitors#{FromPid => MonitorRef},
         {reply, Res, State#state{channels = [C | Channels], monitors = NewMonitors}};
      {error, Reason} = ERes ->
         lager:error("Error starting channel: ~p", [Reason]),
         {reply, ERes, State}
   end;

handle_call(create_channel, _From, State = #state{connection = Conn}) ->
   {reply, start_channel(Conn), State};
handle_call(_Request, _From, State = #state{}) ->
   {reply, ok, State}.

handle_cast({checkin_channel, ChanPid}, State = #state{monitors = Monitors, channels = Channels}) ->
   NewChannels = remove_channel(Channels, ChanPid),
   NewMonitors = remove_monitor(Monitors, ChanPid),
   {noreply, State#state{monitors = NewMonitors, channels = NewChannels}};
handle_cast(_Request, State = #state{}) ->
   {noreply, State}.

handle_info(connect, State) ->
   io:format("[~p] connect to rmq",[?MODULE]),
   NewState = start_connection(State),
   {noreply, NewState};

%% amqp connection died
handle_info({'EXIT', Conn, Reason}, State=#state{connection = Conn, reconnector = Recon} ) ->
   lager:notice("RMQ Connection died with Reason: ~p",[Reason]),
   {ok, Reconnector} = backoff:execute(Recon, connect),
   {noreply, State#state{
      reconnector = Reconnector,
      connection = nil
   }};
%% channel died and connection already dead
handle_info({'EXIT', ChanPid, Reason}, State=#state{connection = nil, channels = Channels, monitors = Monitors} ) ->
   lager:notice("MQ channel DIED: ~p", [Reason]),
   NewChannels = remove_channel(Channels, ChanPid),
   NewMonitors = remove_monitor(Monitors, ChanPid),
   {noreply, State#state{
      monitors = NewMonitors,
      channels = NewChannels
   }};
%% channel died, connection still alive
handle_info({'EXIT', ChanPid, Reason}, State=#state{channels = Channels, monitors = Monitors} ) ->
   lager:notice("MQ channel DIED: ~p", [Reason]),
   NewChannels = remove_channel(Channels, ChanPid),
   NewMonitors = remove_monitor(Monitors, ChanPid),
   {noreply, State#state{
      monitors = NewMonitors,
      channels = NewChannels
   }};
handle_info(_Info, State = #state{}) ->
   {noreply, State}.

terminate(_Reason, _State = #state{connection = Conn}) ->
   amqp_connection:close(Conn).

code_change(_OldVsn, State = #state{}, _Extra) ->
   {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
start_connection(State = #state{config = Config, reconnector = Recon}) ->
   case amqp_connection:start(Config) of
      {ok, Conn} ->
         link(Conn),
         State#state{connection = Conn, reconnector = backoff:reset(Recon)};
      {error, Error} ->
         io:format("Error starting amqp connection: ~p :: ~p",[Config, Error]),
         lager:warning("Error starting amqp connection: ~p :: ~p",[Config, Error]),
         {ok, Reconnector} = backoff:execute(Recon, connect),
         State#state{connection = nil, reconnector = Reconnector}
   end.

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

remove_channel(Channels, Pid) ->
   lists:delete(Pid, Channels).

remove_monitor(Monitors, Pid) when is_map_key(Pid, Monitors) ->
   MonitorRef = maps:get(Pid, Monitors),
   true = erlang:demonitor(MonitorRef),
   maps:without([Pid], Monitors);
remove_monitor(Monitors, _Pid) ->
   Monitors.