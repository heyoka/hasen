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

%%setup() ->
%%   ?debugMsg("setup"),
%%   Config = #{host => "localhost", port => 5672},
%%   Config.

reconnect_test() ->
   Config = #{host => "localhost", port => 5672, user => <<"guest">>, pass => <<"guest">>},
   {ok, ConnWorker} = rabbit_connection:start_link(Config),
   timer:sleep(200),
   {ok, Conn} = rabbit_connection:get_connection(ConnWorker),
   exit(Conn, stop),
   timer:sleep(500),
   ?assertMatch({ok, _}, rabbit_connection:get_connection(ConnWorker)).

get_channel_test() ->
   Config = #{host => "localhost", port => 5672, user => <<"guest">>, pass => <<"guest">>},
   {ok, ConnWorker} = rabbit_connection:start_link(Config),
   timer:sleep(200),
   ?assertMatch({ok, _}, rabbit_connection:checkout_channel(ConnWorker)).



