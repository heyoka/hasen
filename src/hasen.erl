-module(hasen).

%% @doc
%% config: provide a map or a proplist with the following entries:
%%
%% #{
%%    pool_key => binary/string/atom,
%%
%%    amqp_opts => map/proplist/#amqp_params_network{},
%%
%%    initial_size => pos_integer() (default 2) (initial number of amqp connections),
%%
%%    max_channels => pos_integer() (default 10) (maximum number of channels per amqp connection),
%%
%%    grow => pos_integer() (default 2) (multiples of the max_channels that must be available at all times),
%%                               (when number of channels available < (max_channels x this number), then
%%                               the hasen_handler will start an additional connection process)
%%
%%    shrink => pos_integer() (default 4) (> grow) (when number of channels available (multiple of max_channels) >
%%                                  (max_channels x this number), then the hasen_handler will try to stop
%%                                  a connection (that has no channels yet))
%%
%%    }
%%

-include_lib("amqp_client/include/amqp_client.hrl").
-include("hasen.hrl").

-export([get_channel/1, ensure_pool/1]).

-define(POOL_OPTS, #{
   pool_key => undefined,
   amqp_opts => undefined,
   initial_size => 2,
   max_channels => 10,
   grow => 2,
   shrink => 4}).

ensure_pool(PoolOpts) ->
   PoolOptions = #{pool_key := PoolKey} = eval_opts(PoolOpts),
   hasen_sup:start_pool_sup(PoolKey),
   hasen_handler_sup:start_handler(PoolOptions).


default_opts(#{} = Opts) ->
   maps:merge(?POOL_OPTS, Opts).

eval_opts(Opts) when is_list(Opts) ->
   OptMap = eval_opts(Opts, #{}),
   default_opts(OptMap);
eval_opts(#{} = Opts) ->
   eval_opts(maps:to_list(Opts)).

eval_opts([{pool_key, PoolKey}|R], ResMap) when is_binary(PoolKey) ->
   eval_opts(R, ResMap#{pool_key => binary_to_list(PoolKey)});
eval_opts([{pool_key, PoolKey}|R], ResMap) when is_atom(PoolKey) ->
   eval_opts(R, ResMap#{pool_key => atom_to_list(PoolKey)});
eval_opts([{pool_key, PoolKey}|R], ResMap) when is_list(PoolKey) ->
   eval_opts(R, ResMap#{pool_key => PoolKey});
eval_opts([{amqp_opts, AmqpOpts}|R], ResMap)
      when is_list(AmqpOpts) orelse is_map(AmqpOpts) orelse is_record(AmqpOpts, amqp_params_network) ->
   eval_opts(R, ResMap#{amqp_opts => amqp_options:parse(AmqpOpts)});
eval_opts([{initial_size, Initial}|R], ResMap) when is_integer(Initial) andalso Initial > 0 ->
   eval_opts(R, ResMap#{initial_size => Initial});
eval_opts([{max_channels, Max}|R], ResMap) when is_integer(Max) andalso Max > 0 ->
   eval_opts(R, ResMap#{max_channels => Max});
eval_opts([{grow, Grow}|R], ResMap) when is_integer(Grow) and Grow > 0 ->
   eval_opts(R, ResMap#{grow => Grow});
eval_opts([{shrink, Shrink}|R], ResMap) when is_integer(Shrink) and Shrink > 0 ->
   eval_opts(R, ResMap#{shrink => Shrink});
eval_opts([Option|_R], _ResMap) ->
   erlang:error(lists:flatten(io_lib:format("unrecognized option or wrong format '~p'", [Option])));
%%   eval_opts(R, ResMap);
eval_opts([], ResMap) ->
   ResMap.


-spec get_channel(PoolKey :: string()) -> {ok, pid()} | {error, Reason :: term()}.
get_channel(PoolKey) ->
   case get_connection(PoolKey) of
      {ok, Conn} ->
         rabbit_connection:checkout_channel(Conn);
      {error, Reason} ->
         {error, Reason}
   end.

%%%===================================================================
%%% sort of round robin selection of connection processes
%%% table is of type ord_set
%%%===================================================================

get_connection(PoolKey) ->
   Table = hasen_ets:table_name(?ETS_POOL_AVAILABLE, PoolKey),
   case ets:next(Table, get_index(Table)) of
      '$end_of_table' ->
         case ets:next(Table, -1) of
            '$end_of_table' -> {error, no_connection_in_pool};
            Next ->
               update_index(Table, Next),
               {ok, Next}
         end;
      NextConnPid ->
         update_index(Table, NextConnPid),
         {ok, NextConnPid}
   end.

update_index(Table, Current) ->
   ets:insert(pool_index, {Table, Current}).


get_index(Key) ->
   case ets:lookup(pool_index, Key) of
      [] -> -1;
      [{Key, LastConnPid}] -> LastConnPid
   end.
