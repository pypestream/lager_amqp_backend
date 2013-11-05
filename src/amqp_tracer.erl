%%%-------------------------------------------------------------------
%%% @author Jack Tang <jack@taodinet.com>
%%% @copyright (C) 2013, Jack Tang
%%% @doc
%%%
%%% @end
%%% Created : 30 Oct 2013 by Jack Tang <jack@taodinet.com>
%%%-------------------------------------------------------------------
-module(amqp_tracer).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([distributed_trace/3,
         trace_amqp/2,
         trace_amqp/3,
         stop_trace/1,
         clear_all_traces/0]).

-define(SERVER, ?MODULE). 

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================
distributed_trace(RoutingKey, Filter, Level) ->
    lists:foreach(fun(Node) ->
                          gen_server:cast({?SERVER, Node}, {trace, RoutingKey, Filter, Level})
                  end, nodes()).

trace_amqp(RoutingKey, Filter) ->
    trace_amqp(RoutingKey, Filter, debug).

trace_amqp(RoutingKey, Filter, Level) ->
    Trace0 = { Filter, Level, {lager_amqp_backend, RoutingKey} },
%% get params from config
    LagerEnv = case application:get_all_env(lager) of
                   undefined -> [];
                   Env -> Env
               end,
    HandlerConf = config_val(handlers, LagerEnv, []),
    Params = config_val(lager_amqp_backend, HandlerConf, []),

    Name      = config_val(name, Params, <<"lager_amqp_backend">>),
    Exchange = config_val(exchange, Params, <<"lager_amqp_backend">>),
    UserName       = config_val(amqp_user, Params, <<"guest">>),
    PassWord       = config_val(amqp_pass, Params, <<"guest">>),
    Vhost   = config_val(amqp_vhost, Params, <<"/">>),
    Host           = config_val(amqp_host, Params, "localhost"),
    Port           = config_val(amqp_port, Params, 5672),
%%----------------------------------------------------------------------
    case lager_util:validate_trace(Trace0) of
        {ok, Trace} ->
            Handlers = gen_event:which_handlers(lager_event),
            %% check if this file backend is already installed
            Res = case lists:member({lager_amqp_backend, RoutingKey}, Handlers) of
                false ->
                    %% install the handler ,https://github.com/basho/lager/issues/65
                    supervisor:start_child(lager_handler_watcher_sup,
                        [lager_event, {lager_amqp_backend, RoutingKey}, {Name, none, Exchange,    
                       UserName, PassWord, Vhost, Host, RoutingKey, Port}]);
                _ ->
                    {ok, exists}
            end,
            case Res of
              {ok, _} ->
                add_trace_to_loglevel_config(Trace),
                {ok, Trace};
              {error, _} = E ->
                E
            end;
        Error ->
            Error
    
    end.

stop_trace({_Filter, _Level, Target} = Trace) ->
    gen_server:cast(?SERVER, {stop_trace, Target, Trace}).

clear_all_traces() ->
    gen_server:cast(?SERVER, clear_all_traces).


%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initiates the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    {ok, #state{}}.

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
handle_cast({trace, RoutingKey, Filter, Level}, State) ->
    Trace0 = { Filter, Level, {lager_amqp_backend, RoutingKey} },
%% get params from config
    LagerEnv = case application:get_all_env(lager) of
                   undefined -> [];
                   Env -> Env
               end,
    HandlerConf = config_val(handlers, LagerEnv, []),
    Params = config_val(lager_amqp_backend, HandlerConf, []),

    Name      = config_val(name, Params, <<"lager_amqp_backend">>),
    Exchange = config_val(exchange, Params, <<"lager_amqp_backend">>),
    UserName       = config_val(amqp_user, Params, <<"guest">>),
    PassWord       = config_val(amqp_pass, Params, <<"guest">>),
    Vhost   = config_val(amqp_vhost, Params, <<"/">>),
    Host           = config_val(amqp_host, Params, "localhost"),
    Port           = config_val(amqp_port, Params, 5672),
%%----------------------------------------------------------------------
    case lager_util:validate_trace(Trace0) of
        {ok, Trace} ->
            Handlers = gen_event:which_handlers(lager_event),
            %% check if this file backend is already installed
            Res = case lists:member({lager_amqp_backend, RoutingKey}, Handlers) of
                false ->
                    %% install the handler ,https://github.com/basho/lager/issues/65
                    supervisor:start_child(lager_handler_watcher_sup,
                        [lager_event, {lager_amqp_backend, RoutingKey}, {Name, none, Exchange,    
                       UserName, PassWord, Vhost, Host, RoutingKey, Port}]);
                _ ->
                    {ok, exists}
            end,
            case Res of
              {ok, _} ->
                add_trace_to_loglevel_config(Trace),
                {ok, Trace};
              {error, _} = E ->
                E
            end;
        Error ->
            Error
    
    end,
    {noreply, State};

handle_cast({stop_trace, Target, Trace}, State) ->
    {Level, Traces} = lager_config:get(loglevel),
    NewTraces =  lists:delete(Trace, Traces),
    lager_util:trace_filter([ element(1, T) || T <- NewTraces ]),
    lager_config:set(loglevel, {Level, NewTraces}),
    case lager:get_loglevel(Target) of
        none ->
            %% check no other traces point here
            case lists:keyfind(Target, 3, NewTraces) of
                false ->
                    gen_event:delete_handler(lager_event, Target, []);
                _ ->
                    ok
            end;
        _ ->
            ok
    end,
    {noreply, State};

handle_cast({clear_all_traces}, State) ->
    {Level, _Traces} = lager_config:get(loglevel),
    lager_util:trace_filter(none),
    lager_config:set(loglevel, {Level, []}),
    lists:foreach(fun(Handler) ->
          case lager:get_loglevel(Handler) of
            none ->
              gen_event:delete_handler(lager_event, Handler, []);
            _ ->
              ok
          end
      end, gen_event:which_handlers(lager_event)),
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
add_trace_to_loglevel_config(Trace) ->
    {MinLevel, Traces} = lager_config:get(loglevel),
    case lists:member(Trace, Traces) of
        false ->
            NewTraces = [Trace|Traces],
            lager_util:trace_filter([ element(1, T) || T <- NewTraces]),
            lager_config:set(loglevel, {MinLevel, [Trace|Traces]});
        _ ->
            ok
    end.
config_val(C, Params, Default) ->
  case lists:keyfind(C, 1, Params) of
    {C, V} -> V;
    _ -> Default
  end.