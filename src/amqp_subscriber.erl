%%%-------------------------------------------------------------------
%%% @author Jack Tang <jack@taodinet.com>
%%% @copyright (C) 2013, Jack Tang
%%% @doc
%%%
%%% @end
%%% Created : 25 Oct 2013 by Jack Tang <jack@taodinet.com>
%%%-------------------------------------------------------------------
-module(amqp_subscriber).

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, 
         handle_call/3, 
         handle_cast/2, 
         handle_info/2,
         terminate/2, 
         code_change/3]).

-define(SERVER, ?MODULE). 
-include_lib("amqp_client/include/amqp_client.hrl").


-record(state, { channel,
                 queue,
                 consumer_tag}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(RoutingKey) when is_binary(RoutingKey) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [RoutingKey], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([RoutingKey]) ->
    HandlersConf    = application:get_env(lager, handlers, []),
    Params          = proplists:get_value(lager_amqp_backend, HandlersConf, []),
    
    Exchange        = proplists:get_value(exchange, Params, <<"lager_amqp_backend">>),
    AmqpParams = #amqp_params_network {
      username       = proplists:get_value(amqp_user, Params, <<"guest">>),
      password       = proplists:get_value(amqp_pass, Params, <<"guest">>),
      virtual_host   = proplists:get_value(amqp_vhost, Params, <<"/">>),
      host           = proplists:get_value(amqp_host, Params, "localhost"),
      port           = proplists:get_value(amqp_port, Params, 5672)
     },

    {ok, Channel} = amqp_client:amqp_channel(AmqpParams),
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, #'exchange.declare'{ exchange = Exchange, 
                                                                               type = <<"topic">> }),

    %% Declare a queue
    #'queue.declare_ok'{queue = Q} = amqp_channel:call(Channel, #'queue.declare'{}),
    Binding = #'queue.bind'{queue       = Q,
                            exchange    = Exchange,
                            routing_key = RoutingKey},
     #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),

    % Subscribe the channel and consume the message
    Sub = #'basic.consume'{queue = Q},
    Consumer = self(),
    #'basic.consume_ok'{consumer_tag = CTag} = amqp_channel:subscribe(Channel, Sub, Consumer),
    
    {ok, #state{channel      = Channel,
                queue        = Q,
                consumer_tag = CTag}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(close, #state{}=State) ->
    #state{ consumer_tag = CTag,
            channel      = Channel } = State,
    
    Cancel = #'basic.cancel'{consumer_tag = CTag},
    #'basic.cancel_ok'{} = amqp_channel:call(Channel, Cancel),
    amqp_channel:close(Channel),
    {noreply, State};

handle_cast(subscribe, #state{}=State) ->
    #state{ channel = Channel,
            queue   = Q     } = State,
    
    Sub = #'basic.consume'{queue = Q},
    Consumer = self(),
    #'basic.consume_ok'{consumer_tag = CTag} = amqp_channel:subscribe(Channel, Sub, Consumer),
    {noreply,#state{consumer_tag=CTag}};

handle_cast(_Msg, State) ->
    io:format("Can't handle msg: ~p", [_Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};
handle_info(#'basic.cancel_ok'{}, State) ->
    {noreply, State};

handle_info({#'basic.deliver'{delivery_tag = DTag}, Content}, 
    #state{channel = Channel} = State) ->
    Message = binary_to_term(Content),
    io:format("> ~s~n", [Message]),
    
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = DTag}),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
