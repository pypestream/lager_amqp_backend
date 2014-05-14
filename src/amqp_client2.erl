%%%-------------------------------------------------------------------
%%% @author Jack Tang <jack@taodinet.com>
%%% @copyright (C) 2014, Jack Tang
%%% @doc
%%%
%%% @end
%%% Created :  5 May 2014 by Jack Tang <jack@taodinet.com>
%%%-------------------------------------------------------------------
-module(amqp_client2).

%% API
-export([amqp_channel/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------
amqp_channel(AmqpParams) ->
    case maybe_new_pid({AmqpParams, connection},
                       fun() -> amqp_connection:start(AmqpParams) end) of
        {ok, Client} ->
            maybe_new_pid({AmqpParams, channel},
                          fun() -> amqp_connection:open_channel(Client) end);
        Error ->
            Error
    end.
 
%%%===================================================================
%%% Internal functions
%%%===================================================================
maybe_new_pid(Group, StartFun) ->
    case pg2:get_closest_pid(Group) of
        {error, {no_such_group, _}} ->
            pg2:create(Group),
            maybe_new_pid(Group, StartFun);
        {error, {no_process, _}} ->
            case StartFun() of
                {ok, Pid} ->
                    pg2:join(Group, Pid),
                    {ok, Pid};
                Error ->
                    Error
            end;
        Pid ->
            {ok, Pid}
    end.   
