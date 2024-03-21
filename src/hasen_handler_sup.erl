%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2024
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(hasen_handler_sup).

-behaviour(supervisor).

-export([start_link/0, init/1, start_handler/1]).

start_link() ->
   supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
   {ok, {#{strategy => one_for_one,
      intensity => 5,
      period => 30},
      []}
   }.

start_handler(Opts = #{pool_key := Key}) ->
   HandlerSpec =
      #{id => Key,
         start => {hasen_handler, start_link, [Opts]},
         restart => permanent,
         shutdown => 3000,
         type => worker
      },

   supervisor:start_child(?MODULE, HandlerSpec).


