%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2024
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(hasen_test_client).

-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
   code_change/3]).

-record(state, {
   recon, %:: backoff(),
   pool_key :: string(),
   channel :: pid()
}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(PoolKey) ->
   gen_server:start_link(?MODULE, [PoolKey], []).

init([PoolKey]) ->
   Backoff0 = backoff:new({300, 10000, infinity}),
   {ok, Backoff} = backoff:execute(Backoff0, reconnect),
   {ok, #state{recon = Backoff, pool_key = PoolKey}}.

handle_call(_Request, _From, State = #state{}) ->
   {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
   {noreply, State}.

handle_info(reconnect, State = #state{recon = Backoff}) ->
   NewBackoff = backoff:reset(Backoff),
   NewState = connect(State#state{recon = NewBackoff}),
   {noreply, NewState};
handle_info(die, State = #state{}) ->
%%   lager:notice("going to die ...."),
%%   X = 1, X = 2,
   {noreply, State};
handle_info(_Info, State = #state{}) ->
   {noreply, State}.

terminate(_Reason, _State = #state{}) ->
   ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
   {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
connect(State = #state{pool_key = Key}) ->
   case hasen:get_channel(Key) of
      {ok, Channel} when is_pid(Channel) ->
%%         lager:notice("[~p<~p>] got channel with pid ~p",[?MODULE, self(), Channel]),
         case die_or_not() of
            true -> erlang:send_after(rand:uniform(50000)+2000, self(), die);
            false -> ok
         end,
         State#state{channel = Channel};
      {error, Reason} ->
         lager:warning("Failed to get amqp channel, reason: ~p",[Reason]),
         {ok, NewBackoff} = backoff:execute(State#state.recon, reconnect),
         State#state{channel = undefined, recon = NewBackoff}
   end.

die_or_not() ->
   rand:uniform() > 0.85.
