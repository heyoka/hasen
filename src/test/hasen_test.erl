%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2024
%%% @doc
%%%
%%% @end
%%% Created : 12. Mär 2024 17:31
%%%-------------------------------------------------------------------
-module(hasen_test).
-author("heyoka").

%% API
-export([fast_aquire_test/0]).

-define(CONN_PARAMS, #{host => "localhost", port => 5672, user => <<"guest">>, pass => <<"guest">>}).
-define(POOL_KEY, "hasen_test").

fast_aquire_test() ->
   rand:seed(default),
   NumClients = 10000,
   hasen:ensure_pool(
      #{pool_key => ?POOL_KEY,
         amqp_opts => ?CONN_PARAMS,
         initial_size => 3,
         max_channels => 15}),

   lists:foreach(
      fun(_) -> hasen_test_client:start_link(?POOL_KEY), timer:sleep(50) end,
      lists:seq(1, NumClients)
   ).

