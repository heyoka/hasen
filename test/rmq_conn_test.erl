%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 05. Jän 2024 19:54
%%%-------------------------------------------------------------------
-module(rmq_conn_test).
-author("heyoka").

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include("hasen.hrl").

-define(AMQP_PARAMS, #{host => "localhost", port => 5672, user => <<"guest">>, pass => <<"guest">>}).
-define(POOL_KEY, "my_hasen_pool").

amqp_params() ->
   amqp_options:parse(?AMQP_PARAMS).

setup_ets() ->
   PoolKey = ?POOL_KEY ,
   TableAvailable = hasen_ets:table_name(?ETS_POOL_AVAILABLE, PoolKey, true),
   hasen_ets:create_table(TableAvailable, ordered_set).



reconnect_test() ->
   setup_ets(),
   {ok, ConnWorker} = rabbit_connection:start_link(?POOL_KEY, self(), amqp_params(), 5),
   timer:sleep(200),
   {ok, Conn} = rabbit_connection:get_connection(ConnWorker),
   catch exit(Conn, stop),
   timer:sleep(500),
   ?assertMatch({ok, _}, rabbit_connection:get_connection(ConnWorker)),
   rabbit_connection:stop(ConnWorker).

basic_channel_test() ->
   {ok, ConnWorker} = rabbit_connection:start_link(?POOL_KEY, self(), amqp_params(), 3),
   timer:sleep(200),
   {ok, ChannelPid} = rabbit_connection:checkout_channel(ConnWorker),
%%   ?assertMatch({ok, _}, rabbit_connection:checkout_channel(ConnWorker)),
   {ok, Monitors} = rabbit_connection:get_monitors(ConnWorker),
   ?assertEqual(1, maps:size(Monitors)),
   {ok, Channels} = rabbit_connection:get_channels(ConnWorker),
   ?assertEqual(1, length(Channels)),
   rabbit_connection:checkin_channel(ConnWorker, ChannelPid),
   {ok, Monitors1} = rabbit_connection:get_monitors(ConnWorker),
   ?assertEqual(0, maps:size(Monitors1)),
   {ok, Channels1} = rabbit_connection:get_channels(ConnWorker),
   ?assertEqual(0, length(Channels1)),
   rabbit_connection:stop(ConnWorker).

client_down_test() ->
   {ok, ConnWorker} = rabbit_connection:start_link(?POOL_KEY, self(), amqp_params(), 5),
   timer:sleep(200),
   Parent = self(),
   _Client = spawn(
      fun() ->
         {ok, _ChannelPid} = rabbit_connection:checkout_channel(ConnWorker),
         Parent ! done
      end),
   receive
      done ->
         {ok, Monitors1} = rabbit_connection:get_monitors(ConnWorker),
         ?assertEqual(0, maps:size(Monitors1)),
         {ok, Channels1} = rabbit_connection:get_channels(ConnWorker),
         ?assertEqual(0, length(Channels1)),
         rabbit_connection:stop(ConnWorker)
   after 5000 ->
      exit(timeout)
   end
   .



