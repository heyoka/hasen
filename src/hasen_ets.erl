%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(hasen_ets).

-behaviour(gen_server).

-include("hasen.hrl").

-export([start_link/0, exists/1, table_name/2, table_name/3, new_table/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
   code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================
table_name(Type, PoolKey) when is_list(Type) andalso is_list(PoolKey) ->
   erlang:list_to_existing_atom(Type ++ "_" ++ PoolKey).
table_name(Type, PoolKey, true) when is_list(Type) andalso is_list(PoolKey) ->
   erlang:list_to_atom(Type ++ "_" ++ PoolKey).

exists(TableName) ->
   ets:whereis(TableName) /= undefined.

new_table(TableName, Type) when is_atom(TableName) andalso is_atom(Type) ->
   gen_server:call(?MODULE, {create_table, TableName, Type}).

start_link() ->
   gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
   create_table(pool_sups, set),
   create_table(pool_index, set),
   {ok, #state{}}.

handle_call({create_table, TableName, Type}, _From, State = #state{}) ->
   create_table(TableName, Type),
   {reply, ok, State};
handle_call(_Request, _From, State = #state{}) ->
   {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
   {noreply, State}.

handle_info(_Info, State = #state{}) ->
   {noreply, State}.

terminate(_Reason, _State = #state{}) ->
   ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
   {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
create_table(NameAtom, Type) ->
   ets:new(NameAtom,
      [Type, public, named_table, {read_concurrency,true}, {write_concurrency,true},{heir, self(), NameAtom} ]).