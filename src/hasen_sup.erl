%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(hasen_sup).

-behaviour(supervisor).

-export([start_link/0, init/1, start_pool_sup/1]).

start_link() ->
   supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
   Ets = #{id => hasen_ets,
      start => {hasen_ets, start_link, []},
      restart => permanent,
      shutdown => 3000,
      type => worker},

   HandlerSup =
      #{id => hasen_handler_sup,
      start => {hasen_handler_sup, start_link, []},
      restart => permanent,
      shutdown => infinity,
      type => supervisor,
      modules => [hasen_handler_sup]
   },

   {ok, {#{strategy => one_for_one,
      intensity => 5,
      period => 30},
      [Ets, HandlerSup]}
   }.

start_pool_sup(Name) ->
   lager:notice("Start hasen_pool_sup with name ~p",["hasen_pool_sup_" ++ Name]),
   Id = erlang:list_to_atom("hasen_pool_sup_" ++ Name),
   SupervisorSpec =
   #{id => Id,
      start => {hasen_pool_sup, start_link, [Id]},
      restart => permanent,
      shutdown => infinity,
      type => supervisor
      },

   {ok, SupPid} = supervisor:start_child(?MODULE, SupervisorSpec),
   ets:insert(pool_sups, {Name, SupPid}),
   {ok, SupPid}.

