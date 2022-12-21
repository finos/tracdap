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

package org.finos.tracdap.gateway.proxy.http;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.*;
import io.netty.util.ReferenceCountUtil;
import org.finos.tracdap.common.exception.ENetworkHttp;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.exception.EUnexpected;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.net.SocketAddress;
import java.util.*;


public class Http2FlowControl extends Http2ChannelDuplexHandler {

    // See the HTTP/2 RFC for reference:
    // https://httpwg.org/specs/rfc7540.html

    // TODO: Handle RST_STREAM and GOAWAY inbound frames

    // Currently this handler only works on the proxy side, it will need updating to work on the client side
    // Stream create / destroy operations would need to be supported on both inbound and outbound frames

    // This implementation does not try to propagate flow control through the GW or on to the client connection
    // Instead writes are buffered in this class, reads are sent on immediately
    // True flow control could be achieved using custom events to start / stop the flow in both directions
    // One approach would be to use an on/off flag with some buffering capacity
    // Given the layers of translation, this may be simpler than requesting explicit numbers of bytes / messages

    // Default HTTP/2 settings values as per the protocol
    // If values are not specified in initial settings, these are the implicit defaults
    public static final int HTTP2_DEFAULT_INITIAL_WINDOW_SIZE = (1 << 16) - 1;
    public static final int HTTP2_DEFAULT_MAX_FRAME_SIZE = 1 << 14;

    // Alternate initial settings to use for data transfer endpoints
    public static final int TRAC_DATA_INITIAL_WINDOW_SIZE = (1 << 19) - 1;
    public static final int TRAC_DATA_MAX_FRAME_SIZE = (1 << 16);

    private static final boolean INBOUND_DIRECTION = true;
    private static final boolean OUTBOUND_DIRECTION = false;

    // Flags indicating whether ping / settings frames are auto-acked in the HTTP/2 codec
    private static final boolean AUTO_ACK_PING = true;
    private static final boolean AUTO_ACK_SETTINGS = true;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final int connId;
    private final SocketAddress target;

    private final Http2Settings inboundSettings;  // We do not try to modify settings after the connection starts
    private Http2Settings outboundSettings;
    private boolean outboundHandshake;
    private boolean inboundHandshake;

    private final Map<Http2FrameStream, StreamState> streams;


    public Http2FlowControl(int connId, SocketAddress target, Http2Settings inboundSettings) {

        this.connId = connId;
        this.target = target;

        this.inboundSettings = fillDefaultSettings(inboundSettings);
        this.outboundSettings = null;  // waiting for handshake

        this.streams = new HashMap<>();
    }

    @Override
    public void channelActive(@Nonnull ChannelHandlerContext ctx) {

        if (log.isDebugEnabled())
            log.debug("conn = {}, target = {}, channel active", connId, target);

        // Do not fire the channel active event until the handshake is complete
        // If the channel somehow re-activates after the handshake, check the outbound queues for messages

        if (inboundHandshake && outboundHandshake) {
            ctx.fireChannelActive();
            processAllQueues(ctx);
        }
    }

    @Override
    public void channelInactive(@Nonnull ChannelHandlerContext ctx) {

        if (log.isDebugEnabled())
            log.debug("conn = {}, target = {}, channel inactive", connId, target);

        ctx.fireChannelInactive();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

        try {

            if (!(msg instanceof Http2Frame)) {
                log.error("conn = {}, target = {}, unexpected outbound message of type [{}]", connId, target, msg.getClass().getName());
                throw new EUnexpected();
            }

            var frame = (Http2Frame) msg;

            if (log.isTraceEnabled())
                logFrameTrace(frame, OUTBOUND_DIRECTION);

            if (frame.name().equals("HEADERS")) {

                var headersFrame = (Http2HeadersFrame) frame;
                processOutboundHeaders(ctx, headersFrame, promise);
            }
            else if (frame.name().equals("DATA")) {

                var dataFrame = (Http2DataFrame) frame;
                processOutboundData(ctx, dataFrame, promise);
            }

            // Client code should not be sending other HTTP frame types down the pipe
            // Since this is controlled inside the gateway, it can be treated as an internal error

            else {

                log.error("conn = {}, target = {}, unexpected outbound HTTP/2 frame [{}]", connId, target, frame.name());
                throw new EUnexpected();
            }
        }
        finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void flush(ChannelHandlerContext ctx) {

        if (log.isTraceEnabled()) {
            log.trace("conn = {}, target = {}, outbound flush", connId, target);
        }

        ctx.flush();
    }

    @Override
    public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) throws Exception {

        try {

            if (!(msg instanceof Http2Frame)) {
                log.error("conn = {}, target = {}, unexpected outbound message of type [{}]", connId, target, msg.getClass().getName());
                throw new EUnexpected();
            }

            var frame = (Http2Frame) msg;

            if (log.isTraceEnabled())
                logFrameTrace(frame, INBOUND_DIRECTION);

            if (frame.name().equals("DATA")) {

                var dataFrame = (Http2DataFrame) msg;
                processInboundData(ctx, dataFrame);
            }

            else if (frame.name().equals("HEADERS")) {

                var headersFrame = (Http2HeadersFrame) frame;
                processInboundHeaders(ctx, headersFrame);
            }

            else if (frame.name().equals("SETTINGS")) {

                var settingFrame = (Http2SettingsFrame) frame;
                processSettings(ctx, settingFrame);
            }

            else if (frame.name().equals("SETTINGS(ACK)")) {

                processSettingsAck(ctx);
            }

            else if (frame.name().equals("WINDOW_UPDATE")) {

                var windowFrame = (Http2WindowUpdateFrame) frame;
                processWindowUpdate(ctx, windowFrame);
            }

            else if (frame.name().equals("PING")) {

                var pingFrame = (Http2PingFrame) frame;
                processPing(ctx, pingFrame);
            }

            else {

                log.warn("conn = {}, target = {}, unhandled HTTP/2 frame [{}]", connId, target, frame.name());
            }
        }
        finally  {
            ReferenceCountUtil.release(msg);
        }
    }


    // -----------------------------------------------------------------------------------------------------------------
    // OUTBOUND FRAMES
    // -----------------------------------------------------------------------------------------------------------------


    private void processOutboundHeaders(ChannelHandlerContext ctx, Http2HeadersFrame headersFrame, ChannelPromise promise) {

        var stream = headersFrame.stream();

        if (stream.id() < 0) {

            if (log.isDebugEnabled()) {

                // Use a promise listener, because stream ID is not set until the first frame is written
                promise.addListener(
                        x -> log.debug("conn = {}, target = {}, new outbound stream [{}]",
                        connId, target, stream.id()));
            }

            createStreamState(stream);
        }

        ctx.write(headersFrame, promise);

        if (headersFrame.isEndStream()) {

            if (log.isDebugEnabled()) {

                promise.addListener(
                        x -> log.debug("conn = {}, target = {}, EOS for outbound stream [{}]",
                        connId, target, stream.id()));
            }

            ctx.flush();
        }
    }

    private void processOutboundData(ChannelHandlerContext ctx, Http2DataFrame dataFrame, ChannelPromise promise) {

        var state = streams.get(dataFrame.stream());
        var frameSize = dataFrame.content().readableBytes();

        if (state == null) {
            // Treat as internal - state should have been created when the headers frame was sent
            var message = String.format("No HTTP/2 stream state available for stream [%d]", dataFrame.stream().id());
            log.error(message);
            throw new ETracInternal(message);
        }

        // Frame will be consumed, so retain it now
        dataFrame.retain();

        if (state.writeWindow >= frameSize && state.writeQueue.isEmpty())
            dispatchFrame(ctx, state, dataFrame, promise);
        else
            queueFrame(state, dataFrame, promise);
    }

    private void dispatchFrame(ChannelHandlerContext ctx, StreamState state, Http2DataFrame dataFrame, ChannelPromise promise) {

        var frameSize = dataFrame.content().readableBytes();

        if (log.isTraceEnabled()) {

            log.trace("conn = {}, target = {}, dispatch data frame, size = [{}], eos = [{}], window = [{}], queue = [{}]",
                    connId, target, frameSize, dataFrame.isEndStream(),
                    state.writeWindow, state.writeQueue.size());
        }

        ctx.write(dataFrame, promise);
        state.writeWindow -= frameSize;

        if (dataFrame.isEndStream()) {

            if (log.isDebugEnabled()) {
                log.debug("conn = {}, target = {}, EOS for outbound stream [{}]", connId, target, dataFrame.stream().id());
            }

            ctx.flush();
        }
    }

    private void queueFrame(StreamState state, Http2DataFrame dataFrame, ChannelPromise promise) {

        var frameSize = dataFrame.content().readableBytes();

        if (log.isTraceEnabled()) {

            log.trace("conn = {}, target = {}, queue data frame, size = [{}], eos = [{}], window = [{}], queue = [{}]",
                    connId, target, frameSize, dataFrame.isEndStream(),
                    state.writeWindow, state.writeQueue.size());
        }

        state.writeQueue.add(Map.entry(dataFrame, promise));
    }

    private void processQueue(ChannelHandlerContext ctx, int streamId, StreamState state) {

        int framesSent = 0;

        if (log.isTraceEnabled()) {
            log.trace("conn = {}, target = {}, processing write queue for stream [{}]", connId, target, streamId);
        }

        while (!state.writeQueue.isEmpty()) {

            var queueHead = state.writeQueue.peek();
            var dataFrame = (Http2DataFrame) queueHead.getKey();
            var frameSize = dataFrame.content().readableBytes();
            var promise = queueHead.getValue();

            // Stop processing if the write window fills up
            if (frameSize > state.writeWindow)
                break;

            state.writeQueue.remove();
            dispatchFrame(ctx, state, dataFrame, promise);

            framesSent += 1;
        }

        if (framesSent > 0) {

            if (log.isTraceEnabled()) {
                log.trace("conn = {}, target = {}, sent [{}] queued frames on stream [{}]", connId, target, framesSent, streamId);
            }

            ctx.flush();
        }
    }

    private void processAllQueues(ChannelHandlerContext ctx) {

        if (log.isTraceEnabled())
            log.trace("conn = {}, target = {}, process all outbound queues", connId, target);

        for (var stream : streams.entrySet()) {

            var streamId = stream.getKey().id();
            var state = stream.getValue();
            processQueue(ctx, streamId, state);
        }
    }


    // -----------------------------------------------------------------------------------------------------------------
    // INBOUND FRAMES
    // -----------------------------------------------------------------------------------------------------------------


    private void processInboundHeaders(ChannelHandlerContext ctx, Http2HeadersFrame headersFrame) {

        // Currently this handler is only used on the proxy side, so there are no new inbound streams
        // If it is applied on the client side, we can log new inbound streams here
        // We'd also need to create the stream state for new streams

        if (log.isDebugEnabled() && headersFrame.isEndStream()) {
            log.debug("conn = {}, target = {}, EOS for inbound stream [{}]", connId, target, headersFrame.stream().id());
        }

        ctx.fireChannelRead(headersFrame);

        // For the same reason, CLOSE events should always occur on inbound frames

        if (headersFrame.stream().state() == Http2Stream.State.CLOSED) {
            destroyStreamState(headersFrame.stream());
        }
    }

    private void processInboundData(ChannelHandlerContext ctx, Http2DataFrame dataFrame) {

        // HTTP/2 frame codec and decoder handle flow control, sending window_update when required
        // Client code only needs to notify the codec that data frames have been consumed
        // To avoid unexpected interplay with byte counting in the codec, it is simplest to notify for each frame

        // Note: Not flushing the flow frame, leaving that logic to the codec implementation

        var dataSize = dataFrame.content().readableBytes();
        var windowFrame = new DefaultHttp2WindowUpdateFrame(dataSize).stream(dataFrame.stream());
        ctx.write(windowFrame);

        // Frame is being sent up the pipe, do not release
        dataFrame.retain();
        ctx.fireChannelRead(dataFrame);

        if (log.isDebugEnabled() && dataFrame.isEndStream()) {
            log.debug("conn = {}, target = {}, EOS for inbound stream [{}]", connId, target, dataFrame.stream().id());
        }

        // Flow control currently applied on the proxy side only, not the client side
        // So CLOSE events should always occur on inbound frames

        if (dataFrame.stream().state() == Http2Stream.State.CLOSED) {
            destroyStreamState(dataFrame.stream());
        }
    }

    private void processPing(ChannelHandlerContext ctx, Http2PingFrame pingFrame) {

        // If this is a ping, reply with a pong
        // Not needed if auto ack ping is set in the HTTP/2 codec

        if (!AUTO_ACK_PING && !pingFrame.ack()) {
            var pongFrame = new DefaultHttp2PingFrame(pingFrame.content(), true);
            ctx.write(pongFrame);
            ctx.flush();
        }
    }

    private void processSettings(ChannelHandlerContext ctx, Http2SettingsFrame settingsFrame) {

        if (!outboundHandshake) {

            // Initial settings frame from the server
            // Store these settings, infer any missing values as the HTTP/2 defaults

            outboundSettings = fillDefaultSettings(settingsFrame.settings());
            outboundHandshake = true;

            if (log.isTraceEnabled()) {

                log.trace("conn = {}, target = {}, initial outbound settings, max frame = [{}], window = [{}]",
                        connId, target,
                        outboundSettings.maxFrameSize(),
                        outboundSettings.initialWindowSize());
            }

            if (inboundHandshake && ctx.channel().isActive()) {

                if (log.isDebugEnabled()) {
                    log.debug("conn = {}, target = {}, http/2 handshake complete", connId, target);
                }

                ctx.fireChannelActive();
                processAllQueues(ctx);
            }
        }

        else {

            // If there is a new window size setting, apply the adjustment to all active streams
            // The remote host may not send WINDOW_UPDATE until the new capacity is exhausted

            var priorWindowSize = outboundSettings.initialWindowSize();
            var newWindowSize = settingsFrame.settings().initialWindowSize();
            var windowAdjustment = newWindowSize - priorWindowSize;

            if (windowAdjustment != 0) {

                log.warn("conn = {}, target = {}, settings update, window size adjustment [{}]",
                        connId, target, windowAdjustment);

                for (var streamState : streams.values())
                    streamState.writeWindow += windowAdjustment;
            }

            // Store all updates to the outbound settings

            outboundSettings.putAll(settingsFrame.settings());

            // If the outbound windows are increased, try to send any buffered frames

            if (windowAdjustment > 0)
                processAllQueues(ctx);
        }

        // No need to send the ACK if autoAckSettingFrame(true) is set in the HTTP/2 codec

        if (!AUTO_ACK_SETTINGS) {
            ctx.write(Http2SettingsAckFrame.INSTANCE);
            ctx.flush();
        }
    }

    private void processSettingsAck(ChannelHandlerContext ctx) {

        if (!inboundHandshake) {

            // Ack frame for our initial settings

            inboundHandshake = true;

            if (log.isTraceEnabled()) {

                log.trace("conn = {}, target = {}, initial inbound settings, max frame = [{}], window = [{}]",
                        connId, target,
                        inboundSettings.maxFrameSize(),
                        inboundSettings.initialWindowSize());
            }

            if (outboundHandshake && ctx.channel().isActive()) {

                if (log.isDebugEnabled()) {
                    log.debug("conn = {}, target = {}, http/2 handshake complete", connId, target);
                }

                ctx.fireChannelActive();
                processAllQueues(ctx);
            }
        }
    }

    private void processWindowUpdate(ChannelHandlerContext ctx, Http2WindowUpdateFrame windowFrame) {

        if (windowFrame.windowSizeIncrement() <= 0) {

            log.error("conn = {}, target = {}, invalid HTTP/2 window update, increment = [{}]",
                    connId, target, windowFrame.windowSizeIncrement());

            ctx.close();
        }

        var stream = windowFrame.stream();

        // Updates to the connection-wide window should be managed internally by the frame codec
        // If these message appear on the channel, ignore them with a warning
        // (this would be a change in behavior, perhaps due to TRAC using some different settings somewhere)

        if (stream.id() == 0) {

            log.warn("conn = {}, target = {}, unexpected increment on connection write window, increment = [{}]",
                    connId, target, windowFrame.windowSizeIncrement());

            return;
        }

        var state = getStreamState(stream);

        // WINDOW_UPDATE on a closed stream is not an error, it can be safely ignored
        // https://httpwg.org/specs/rfc7540.html#WINDOW_UPDATE
        if (state == null)
            return;

        // A real window update on an active stream
        // Check the stream buffer queue and start sending messages if there are any pending

        state.writeWindow += windowFrame.windowSizeIncrement();

        if (log.isTraceEnabled()) {

            log.trace("conn = {}, target = {}, window update, stream = [{}], increment = [{}], new window = [{}]",
                    connId, target, windowFrame.stream().id(),
                    windowFrame.windowSizeIncrement(),
                    state.writeWindow);
        }

        processQueue(ctx, stream.id(), state);
    }


    // -----------------------------------------------------------------------------------------------------------------
    // HELPERS
    // -----------------------------------------------------------------------------------------------------------------


    private static class StreamState {

        int writeWindow;
        Queue<Map.Entry<Http2Frame, ChannelPromise>> writeQueue;
    }

    private void createStreamState(Http2FrameStream stream) {

        var state = new StreamState();
        state.writeWindow = outboundSettings.initialWindowSize();
        state.writeQueue = new ArrayDeque<>();

        streams.put(stream, state);
    }

    private StreamState getStreamState(Http2FrameStream stream) {

        return streams.get(stream);
    }

    private void destroyStreamState(Http2FrameStream stream) {

        // Try not to throw errors during clean-up

        var state = streams.remove(stream);

        // Stream might be abandoned before it was properly initialised
        if (state == null) {
            log.warn("conn = {}, target = {}, stream [{}] destroyed before it was initialized", connId, target, stream.id());
            return;
        }

        var queueMsg = state.writeQueue.poll();

        if (queueMsg != null) {

            log.warn("conn = {}, target = {}, stream [{}] destroyed before all data had been sent", connId, target, stream.id());

            do {
                var frame = queueMsg.getKey();
                var promise = queueMsg.getValue();

                var message = "Data was not fully sent";
                var error = new ENetworkHttp(HttpResponseStatus.BAD_GATEWAY.code(), message);

                ReferenceCountUtil.release(frame);
                promise.setFailure(error);

                queueMsg = state.writeQueue.poll();

            } while (queueMsg != null);
        }
    }

    private Http2Settings fillDefaultSettings(Http2Settings settings) {

        if (settings.initialWindowSize() == null) {
            settings.initialWindowSize(HTTP2_DEFAULT_INITIAL_WINDOW_SIZE);
        }

        if (settings.maxFrameSize() == null) {
            settings.maxFrameSize(HTTP2_DEFAULT_MAX_FRAME_SIZE);
        }

        return settings;
    }

    private void logFrameTrace(Http2Frame frame, boolean direction) {

        var logDirection = direction == INBOUND_DIRECTION ? "inbound" : "outbound";

        if (frame instanceof Http2DataFrame) {

            var dataFrame = (Http2DataFrame) frame;

            log.trace("conn = {}, target = {}, {} frame [{}], stream = {}, size = [{}], eos = [{}]",
                    connId, target, logDirection, dataFrame.name(),
                    dataFrame.stream().id(),
                    dataFrame.content().readableBytes(),
                    dataFrame.isEndStream());

        }
        else if (frame instanceof Http2HeadersFrame) {

            var headersFrame = (Http2HeadersFrame) frame;

            log.trace("conn = {}, target = {}, {} frame [{}], stream = [{}], eos = [{}]",
                    connId, target, logDirection, headersFrame.name(),
                    headersFrame.stream().id(),
                    headersFrame.isEndStream());
        }
        else if (frame instanceof Http2StreamFrame) {

            var streamFrame = (Http2StreamFrame) frame;

            log.trace("conn = {}, target = {}, {} frame [{}], stream = [{}",
                    connId, target, logDirection, streamFrame.name(),
                    streamFrame.stream().id());
        }
        else {

            log.trace("conn = {}, target = {}, {} frame [{}]",
                    connId, target, logDirection, frame.name());
        }
    }
}