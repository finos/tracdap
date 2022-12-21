/*
 * Copyright 2022 Accenture Global Solutions Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.gateway.routing;

import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.gateway.exec.Route;
import org.finos.tracdap.gateway.proxy.grpc.GrpcProtocol;
import org.finos.tracdap.gateway.proxy.http.Http1ProxyBuilder;
import org.finos.tracdap.gateway.proxy.grpc.GrpcProxyBuilder;
import org.finos.tracdap.gateway.proxy.rest.RestApiProxyBuilder;

import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.*;


public class Http1Router extends CoreRouter {

    // See also CoreRouter
    // Code for all the router classes could be simplified

    private static final int SOURCE_IS_HTTP_1 = 1;

    private static final List<RequestStatus> REQUEST_STATUS_FINISHED = List.of(
            RequestStatus.COMPLETED,
            RequestStatus.FAILED);

    private static final List<RequestStatus> REQUEST_STATUS_CAN_RECEIVE = List.of(
            RequestStatus.RECEIVING,
            RequestStatus.RECEIVING_BIDI);

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Map<Long, RequestState> requests;

    private long currentInboundRequest;
    private long currentOutboundRequest;

    public Http1Router(List<Route> routes, int connId) {

        super(routes, connId, "HTTP/1");

        this.requests = new HashMap<>();

        this.currentInboundRequest = -1;
        this.currentOutboundRequest = -1;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // MESSAGES AND EVENTS
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) {

        try {

            if (!(msg instanceof HttpObject))
                throw new EUnexpected();

            if (msg instanceof HttpRequest) {
                processNewRequest(ctx, (HttpRequest) msg);
            }
            else if (msg instanceof HttpContent) {
                processRequestContent(ctx, (HttpContent) msg);
            }
            else {
                throw new EUnexpected();
            }

            if (msg instanceof LastHttpContent) {
                processEndOfRequest(ctx, (LastHttpContent) msg);
            }
        }
        finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {

        try {

            // TODO: Handle this?
            if (!(msg instanceof HttpObject))
                throw new EUnexpected();

            ReferenceCountUtil.retain(msg);
            ctx.write(msg, promise);
        }
        finally {
            ReferenceCountUtil.release(msg);
            destroyAssociation(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable error) {

        log.error("conn = {}, Unhandled error in HTTP/1 routing handler", connId, error);

        // Only send an error response if there is an active request that has not been responded to yet
        // If there is no active request, or a response has already been sent,
        // then an error response would not be recognised by the client

        var currentRequest = requests.get(currentOutboundRequest);
        var responseNotSent = Set.of(RequestStatus.RECEIVING, RequestStatus.WAITING_FOR_RESPONSE);

        if (currentRequest != null && responseNotSent.contains(currentRequest.status)) {

            log.error("conn = {}, Sending 500 error", connId);

            var errorResponse = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);

            ctx.writeAndFlush(errorResponse);
        }

        // If an error reaches this handler, then the channel is in an inconsistent
        // Regardless of whether an error message could be sent or not, we're going to close the connection

        // todo: full clean up

        log.error("conn = {}, This client connection will now be closed", connId);

        ctx.close();
    }


    // -----------------------------------------------------------------------------------------------------------------
    // REQUEST PROCESSING
    // -----------------------------------------------------------------------------------------------------------------


    // TODO: Improve error handling

    private void processNewRequest(ChannelHandlerContext ctx, HttpRequest req) {

        // Set up a new request state and record it in the requests map

        var request = new RequestState();
        request.requestId = ++currentInboundRequest;
        request.status = RequestStatus.RECEIVING;
        requests.put(request.requestId, request);


        // Look up the route for this request
        // If there is no matching route then fail the request immediately with 404
        // This is a normal error, i.e. the client channel can remain open for more requests

        var uri = URI.create(req.uri());
        var route = lookupRoute(uri, req.method(), request.requestId);

        if (route == null) {

            var protocolVersion = req.protocolVersion();
            var response = new DefaultFullHttpResponse(protocolVersion, HttpResponseStatus.NOT_FOUND);
            response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 0);
            ctx.writeAndFlush(response);

            request.status = RequestStatus.FAILED;
            ++currentOutboundRequest;

            return;
        }

        request.routeIndex = route.getIndex();

        // Look up the proxy target for the selected route
        // If there is no state for the required target, create a new channel and target state record

        var target = getOrCreateTarget(ctx, route);

        // Retain, in case this is a FullHttpRequest including content
        ReferenceCountUtil.retain(req);

        if (target.channelActiveFuture.isSuccess())
            target.channel.write(req);
        else
            target.outboundQueue.add(req);
    }

    private void processRequestContent(ChannelHandlerContext ctx, HttpContent msg) {

        var request = requests.getOrDefault(currentInboundRequest, null);

        if (request == null)
            throw new EUnexpected();

        // Inbound content may be received for requests that have already finished
        // E.g. After a 404 error or an early response from a source server
        // In this case the content can be silently discarded without raising a (new) error

        if (REQUEST_STATUS_FINISHED.contains(request.status))
            return;

        if (!REQUEST_STATUS_CAN_RECEIVE.contains(request.status))
            throw new EUnexpected();

        var target = getTarget(request.routeIndex);

        if (target == null)
            throw new EUnexpected();

        msg.retain();

        if (target.channel.isActive())
            target.channel.write(msg);
        else
            target.outboundQueue.add(msg);
    }

    private void processEndOfRequest(ChannelHandlerContext ctx, LastHttpContent msg) {

        var request = requests.getOrDefault(currentInboundRequest, null);

        if (request == null)
            throw new EUnexpected();

        switch (request.status) {

            case RECEIVING:
                request.status = RequestStatus.WAITING_FOR_RESPONSE;
                break;

            case RECEIVING_BIDI:
                request.status = RequestStatus.RESPONDING;
                break;

            case COMPLETED:
            case FAILED:
                requests.remove(currentInboundRequest);
                return;

            default:
                throw new EUnexpected();
        }

        var target = getTarget(request.routeIndex);

        if (target == null)
            throw new EUnexpected();

        if (target.channelActiveFuture.isSuccess())
            target.channel.flush();
    }

    // -----------------------------------------------------------------------------------------------------------------
    // PROXY CHANNEL HANDLING
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    protected ChannelInitializer<Channel> initializeProxyRoute(
            ChannelHandlerContext ctx, CoreRouterLink link,
            Route routeConfig) {

        switch (routeConfig.getConfig().getRouteType()) {

            case HTTP:
                return new Http1ProxyBuilder(routeConfig.getConfig(), link, connId);

            case GRPC:
                return new GrpcProxyBuilder(routeConfig.getConfig(), link, connId, SOURCE_IS_HTTP_1, GrpcProtocol.GRPC_WEB);

            case REST:
                return new RestApiProxyBuilder(routeConfig, SOURCE_IS_HTTP_1, link, ctx.executor(), connId);

            default:
                throw new EUnexpected();
        }
    }

    @Override
    protected void reportProxyRouteError(ChannelHandlerContext ctx, Throwable error, boolean direction) {

        // An error may occur straight away when a client tries to send a request to a route,
        // or at the beginning of a new request on a keep-alive connection. In either case this
        // approach is fine. However, it may also be that an error occurs midway through serving
        // a request, e.g. if a connection drops midway through sending a large file. In this case
        // there is no way notify the client with an error response and the client connection needs
        // to be forcibly closed.

        // For pipelined requests, an error may occur in response to a pipelined request while a response
        // to a previous request is still being sent. In this case, the error response should be sent at
        // the correct point in the pipeline. In practice no major browsers use pipelining in HTTP/1.1
        // under their default settings.

        // In practice, handling all of these conditions correctly may be difficult to implement and to debug.
        // One simpler strategy would be to close the whole client connection by default when an error bubbles
        // up to this point. Then special cases can be added for very common, well-defined cases to provide a
        // better end-user experience.

        // TODO: More intelligent error handling, map different errors to the right HTTP response codes / messages

        var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.SERVICE_UNAVAILABLE);
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 0);
        ctx.writeAndFlush(response);
    }

    private static class RequestState {

        long requestId;
        int routeIndex;

        RequestStatus status;
    }

    private enum RequestStatus {
        RECEIVING,
        RECEIVING_BIDI,
        WAITING_FOR_RESPONSE,
        RESPONDING,
        COMPLETED,
        FAILED
    }
}