%%%-------------------------------------------------------------------
%%% @author Jack Tang <jack@taodinet.com>
%%% @copyright (C) 2014, Jack Tang
%%% @doc
%%%
%%% @end
%%% Created :  5 May 2014 by Jack Tang <jack@taodinet.com>
%%%-------------------------------------------------------------------
-module(lager_amqp_backend).

-behaviour(gen_event).

%% API
-export([test/0]).
-export([start_link/0, add_handler/0]).

%% gen_event callbacks
-export([init/1,
         handle_event/2,
         handle_call/2, 
         handle_info/2,
         terminate/2,
         code_change/3]).


-export([config_to_id/1]).

-define(SERVER, ?MODULE). 
-include_lib("amqp_client/include/amqp_client.hrl").


-record(state, { name,
                 level,
                 exchange,
                 params,
                 routing_key }).


-define(DEFAULT_TRUNCATION, 4096).

%%--------------------------------------------------------------------
%% @doc
%% Creates an event manager
%%
%% @spec start_link() -> {ok, Pid} | {error, Error}
%% @end
%%--------------------------------------------------------------------
test() ->
  application:load(lager),
  %%application:set_env(lager, handlers, [{lager_console_backend, debug}, {lager_amqp_backend, []}]),
  %%application:set_env(lager, error_logger_redirect, false),
  application:start(lager),
  lager:log(info, self(), erlang:list_to_binary("Test INFO message")),
  lager:log(debug, self(), erlang:list_to_binary("Test DEBUG message")),
  lager:log(error, self(), erlang:list_to_binary("Test ERROR message")).



%%%===================================================================
%%% gen_event callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates an event manager
%%
%% @spec start_link() -> {ok, Pid} | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_event:start_link({local, ?SERVER}).



%%--------------------------------------------------------------------
%% @doc
%% Adds an event handler
%%
%% @spec add_handler() -> ok | {'EXIT', Reason} | term()
%% @end
%%--------------------------------------------------------------------
add_handler() ->
    gen_event:add_handler(?SERVER, ?MODULE, []).

%%%===================================================================
%%% gen_event callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a new event handler is added to an event manager,
%% this function is called to initialize the event handler.
%%
%% @spec init(Args) -> {ok, State}
%% @end
%%--------------------------------------------------------------------
init({RoutingKey, Level, Host}) when is_binary(RoutingKey), is_atom(Level) ->
    init([{routing_key, RoutingKey}, {level, Level}, {amqp_host, Host}]);


init(Params) when is_list(Params) ->  
    Name        = proplists:get_value(name, Params, ?MODULE),  
    Level       = lager_util:level_to_num(proplists:get_value(level, Params, debug)),
    Exchange    = proplists:get_value(exchange, Params, list_to_binary(atom_to_list(?MODULE))),
    RoutingKey  = proplists:get_value(routing_key, Params, undefined),

    AmqpParams = #amqp_params_network {
      username       = proplists:get_value(amqp_user, Params, <<"guest">>),
      password       = proplists:get_value(amqp_pass, Params, <<"guest">>),
      virtual_host   = proplists:get_value(amqp_vhost, Params, <<"/">>),
      host           = proplists:get_value(amqp_host, Params, "127.0.0.1"),
      port           = proplists:get_value(amqp_port, Params, 5672)
     },
  
    {ok, Channel} = amqp_utils:amqp_channel(AmqpParams),
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, #'exchange.declare'{ exchange = Exchange, 
                                                                               type = <<"topic">> }),
  
    {ok, #state{ name        = Name,
                 routing_key = RoutingKey,
                 level       = Level, 
                 exchange    = Exchange,
                 params      = AmqpParams
               }}.



%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever an event manager receives an event sent using
%% gen_event:notify/2 or gen_event:sync_notify/2, this function is
%% called for each installed event handler to handle the event.
%%
%% @spec handle_event(Event, State) ->
%%                          {ok, State} |
%%                          {swap_handler, Args1, State1, Mod2, Args2} |
%%                          remove_handler
%% @end
%%--------------------------------------------------------------------
handle_event({log,  Message}, #state{routing_key = RoutingKey, level = L } = State) ->

    case lager_util:is_loggable(Message, L, {lager_amqp_backend, RoutingKey}) of
        true ->
            Meta = lager_msg:metadata(Message),
            try

                Payload = proplists:get_value('amqp.payload', Meta),
                Metadata2 = [ {<<"amqp.payload">>, Payload }| [ { tcl_tools:binarize([K]), tcl_tools:binarize([V])}  ||{K,V} <- Meta,  K /= 'amqp.payload' ]],
            ContentType = proplists:get_value('amqp.content_type', Meta),

            {ok, log(Metadata2,
                     ContentType,
                     State,
                     lager_msg:datetime(Message),
                     lager_msg:severity_as_int(Message),
                     Message)}
            catch
                _Error ->
                    erlang:error(badarg, [Meta])
            end;

        false ->
            {ok, State}
    end;

handle_event(_Event, State) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever an event manager receives a request sent using
%% gen_event:call/3,4, this function is called for the specified
%% event handler to handle the request.
%%
%% @spec handle_call(Request, State) ->
%%                   {ok, Reply, State} |
%%                   {swap_handler, Reply, Args1, State1, Mod2, Args2} |
%%                   {remove_handler, Reply}
%% @end
%%--------------------------------------------------------------------
handle_call({set_loglevel, Level}, #state{ name = _Name } = State) ->
    {ok, ok, State#state{ level=lager_util:level_to_num(Level) }};
    
handle_call(get_loglevel, #state{ level = Level } = State) ->
    {ok, Level, State};
    
handle_call(_Request, State) ->
    Reply = ok,
    {ok, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called for each installed event handler when
%% an event manager receives any other message than an event or a
%% synchronous request (or a system message).
%%
%% @spec handle_info(Info, State) ->
%%                         {ok, State} |
%%                         {swap_handler, Args1, State1, Mod2, Args2} |
%%                         remove_handler
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever an event handler is deleted from an event manager, this
%% function is called. It should be the opposite of Module:init/1 and
%% do any necessary cleaning up.
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
config_to_id(Config) ->
    case proplists:get_value(routing_key, Config) of
        undefined ->
            erlang:error(no_file);
        RoutingKey ->
            {?MODULE, RoutingKey}
    end.


log(Metadata, <<"application/json">> = ContentType, #state{params = AmqpParams } = State, {Date, Time}, Level, _Message) ->
    %Payload = proplists:get_value(<<"payload">>, Metadata),
    case amqp_utils:amqp_channel(AmqpParams) of
        {ok, Channel} ->
            Node = atom_to_list(node()),
            Level1 = atom_to_list(lager_util:num_to_level(Level)),
            Payload2 = encode_json_event(<<"application/json">>, Node, undefined, undefined, Level1, Date, Time, _Message, Metadata),
            send(State, Node, Level,
                 %term_to_binary([Date, Time, Node, Level1, Message]),
                 Payload2,
                 Channel,
                 <<"application/json">>, Metadata);
        _ ->
            State
    end;

%treat everything else as text
log(Metadata,  _ContentType, #state{params = AmqpParams } = State, {Date, Time}, Level, _Message) ->

    case amqp_utils:amqp_channel(AmqpParams) of
        {ok, Channel} ->
            Node = atom_to_list(node()),
            Level1 = atom_to_list(lager_util:num_to_level(Level)),
            Payload = encode_json_event(undefined, Node, undefined, undefined, Level1, Date, Time, _Message, Metadata),
            send(State, Node, Level,
                Payload,
                Channel, <<"text/plain">>, Metadata);
        _ ->
            State
    end;

log(_Metadata, _ContentType, State, _, _, _) ->
    State.



send(#state{ name        = Name,
             exchange    = Exchange,
             routing_key = RK } = State, Node, Level, MsgBin, Channel, ContentType, Metadata) ->
    RoutingKey = case RK of
                     undefined -> routing_key(Node, Name, Level);
                     _ -> RK
                 end,
    %Metadata2 = jiffy:encode({Metadata}),
    Props =  #'P_basic'{content_type =ContentType},
    Publish = #'basic.publish'{ exchange = Exchange, routing_key = RoutingKey},
    Msg = #amqp_msg{ payload = MsgBin, props = Props },
    amqp_channel:cast(Channel, Publish, Msg),
    State.

routing_key(Node, Name, Level) ->
    RkPrefix = atom_to_list(lager_util:num_to_level(Level)),
    RoutingKey =  case Name of
                      []   ->  string:join([Node, RkPrefix], ".");
                      Name ->  string:join([Node, Name, RkPrefix], ".")
                  end,
    list_to_binary(RoutingKey).


encode_json_event(<<"application/json">>, Node, Node_Role, Node_Version, Severity, Date, Time, Message, Metadata) ->
    Payload0  = proplists:get_value(<<"amqp.payload">>, Metadata),
    try
        DateTime = io_lib:format("~sT~s", [Date,Time]),

         Encoded = proplists:get_value(<<"encoded">>, Metadata),

         Metadata1 = proplists:delete(<<"amqp.payload">>, Metadata),


         Payload =
             case tcl_tools:binarize([Encoded]) of
                 <<"false">>  ->
                     case catch shared_json:to_json(binary_to_term(Payload0)) of
                         Payload1 when is_binary(Payload1) -> {<<"json_data">>, p_decode(Payload1) };
                         _ ->
                             % TODO  log these as text
                             PayloadBin =  {<<"json_data2">>, [tcl_tools:binarize(lager_default_formatter:format(Message,[]))]}
                     end;

                 _ ->  {<<"json_data">>, p_decode(Payload0)}
             end,
        
        JSON =
        jiffy:encode( {[
            {<<"type">>, <<"erlang-json">>}
        ] ++ [Payload] ++ Metadata1
        }),


        JSON

    catch
        _Error ->
            _Stacktrace = erlang:get_stacktrace(),
            %io:format("stacktrace:~p~n",[_Stacktrace]),
            erlang:error(badarg, [Payload0])
    end;


encode_json_event(_, Node, Node_Role, Node_Version, Severity, Date, Time, Message, Metadata) ->
   try
    DateTime = io_lib:format("~sT~s", [Date,Time]),

    FormattedMsg = tcl_tools:binarize(lager_default_formatter:format(Message,[message],[])),

    jiffy:encode({[
        {<<"lager_level">>, tcl_tools:binarize([Severity])},
        {<<"lager_role">>, tcl_tools:binarize([Node_Role])},
        {<<"lager_role_version">>, tcl_tools:binarize([Node_Version])},
        {<<"lager_node">>,tcl_tools:binarize([Node])},
        {message, FormattedMsg},
        {<<"lager_timestamp">>, tcl_tools:binarize([DateTime])}, %% use the logstash timestamp
        {<<"type">>, <<"erlang-logs">>}
    ] ++ Metadata
    })

   catch
       _Error ->
           %Stacktrace = erlang:get_stacktrace(),
           erlang:error(badarg, [Message])
   end.


p_decode(Payload) when is_binary(Payload) ->
    jiffy:decode(Payload);

p_decode(_) ->
   <<"">>.
