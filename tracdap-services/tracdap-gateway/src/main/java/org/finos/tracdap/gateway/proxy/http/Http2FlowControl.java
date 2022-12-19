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
import io.netty.handler.codec.http2.*;
import io.netty.util.ReferenceCountUtil;
import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.exception.EUnexpected;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;

public class Http2FlowControl extends Http2ChannelDuplexHandler {

    public static final int DEFAULT_INITIAL_WINDOW_SIZE = (1 << 16) - 1;
    public static final int DEFAULT_MAX_FRAME_SIZE = 1 << 14;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final int connId;

    private final Http2FrameCodec codec;
    private final Map<Http2FrameStream, StreamState> streams;

    private boolean writable;
    private boolean outboundHandshake;
    private boolean inboundHandshake;

    private final Http2Settings inboundSettings;  // We do not try to modify settings after the connection starts
    private Http2Settings outboundSettings;

    private int totalSent = 0;

    private boolean ready;
//    private int connWriteWindow;
//    private int connReadWindow;

    public Http2FlowControl(int connId, Http2FrameCodec codec, Http2Settings inboundSettings) {

        this.connId = connId;

        this.codec = codec;
        this.streams = new HashMap<>();

        this.inboundSettings = inboundSettings;

        if (inboundSettings.initialWindowSize() == null)
            this.inboundSettings.initialWindowSize(DEFAULT_INITIAL_WINDOW_SIZE);

        if (inboundSettings.maxFrameSize() == null)
            this.inboundSettings.maxFrameSize(DEFAULT_MAX_FRAME_SIZE);

        this.writable = false;
        this.ready = false;
//        this.connWriteWindow = 0;
//        this.connReadWindow = inboundSettings.initialWindowSize();
    }

    @Override
    public void channelActive(@Nonnull ChannelHandlerContext ctx) {

        if (log.isDebugEnabled())
            log.debug("conn = {}, target = {}, channel active", connId, ctx.channel().remoteAddress());

        writable = true;

        // do not fire channelActive, wait for the handshake to complete
    }

    @Override
    public void channelInactive(@Nonnull ChannelHandlerContext ctx) {

        if (log.isDebugEnabled())
            log.debug("conn = {}, target = {}, channel inactive", connId, ctx.channel().remoteAddress());

        writable = false;

        ctx.fireChannelInactive();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

        try {

            if (!(msg instanceof Http2Frame))
                throw new EUnexpected();

            var frame = (Http2Frame) msg;

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

                log.error("conn = {}, unexpected outbound HTTP/2 frame [{}]", connId, frame.name());
                throw new EUnexpected();
            }
        }
        finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void flush(ChannelHandlerContext ctx) {

        log.warn("Real flush called");
        ctx.flush();
    }

    @Override
    public void channelRead(@Nonnull ChannelHandlerContext ctx, @Nonnull Object msg) throws Exception {

        try {

            if (!(msg instanceof Http2Frame))
                throw new EUnexpected();

            var frame = (Http2Frame) msg;

            if (frame.name().equals("DATA")) {

                var dataFrame = (Http2DataFrame) msg;
                processInboundData(ctx, dataFrame);

                // Frame is being sent up the pipe, do not release
                dataFrame.retain();

                ctx.fireChannelRead(dataFrame);
            }

            else if (frame.name().equals("HEADERS")) {

                var headersFrame = (Http2HeadersFrame) frame;
                processInboundHeaders(ctx, headersFrame);

                ctx.fireChannelRead(frame);
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

                log.warn("conn = {}, unhandled HTTP/2 frame [{}]", connId, frame.name());
            }
        }
        finally  {
            ReferenceCountUtil.release(msg);
        }
    }

    private void processOutboundHeaders(ChannelHandlerContext ctx, Http2HeadersFrame headersFrame, ChannelPromise promise) {

        var stream = headersFrame.stream();

        if (stream.id() < 0)
            createStream(stream);

        ctx.write(headersFrame, promise);

        if (headersFrame.isEndStream())
            ctx.flush();
    }

    private void processOutboundData(ChannelHandlerContext ctx, Http2DataFrame dataFrame, ChannelPromise promise) {

        var streamState = streams.get(dataFrame.stream());
        var frameSize = dataFrame.content().readableBytes();

        if (streamState == null) {
            var message = String.format("No HTTP/2 stream state available for stream [%d]", dataFrame.stream().id());
            log.error(message);
            throw new ETracInternal(message);
        }

        var sendDirect = ready && streamState.writeWindow >= frameSize && streamState.writeQueue.isEmpty();

        if (sendDirect) {

            log.info("Sending direct, available window = [{}], totalSent = [{}]", streamState.writeWindow, totalSent);

            sliceAndSend(ctx, dataFrame, promise);
        }
        else {

            log.info("Queuing data frame, ready = [{}], writeWindow = [{}], writeQueue = [{}], totalSent = [{}]",
                    ready, streamState.writeWindow, streamState.writeQueue.size(), totalSent);

            if (streamState.writeQueue.isEmpty())
                ctx.flush();

            streamState.writeQueue.add(Map.entry(dataFrame, promise));
        }

        if (dataFrame.isEndStream())
            ctx.flush();
    }

    private void processInboundHeaders(ChannelHandlerContext ctx, Http2HeadersFrame headersFrame) {

        if (headersFrame.isEndStream()) {

            log.info("Got inbound EOS");

            // todo
        }
    }

    private void processInboundData(ChannelHandlerContext ctx, Http2DataFrame dataFrame) {

        var state = streams.get(dataFrame.stream());

        var dataSize = dataFrame.content().readableBytes();
//        var windowFrame = new DefaultHttp2WindowUpdateFrame(dataSize).stream(dataFrame.stream());
//        ctx.write(windowFrame);
//        ctx.flush();


        state.readWindow -= dataSize;

        if (state.readWindow < inboundSettings.initialWindowSize() / 2.0 - 1) {

            var windowIncrement = inboundSettings.initialWindowSize() - state.readWindow;
            var windowFrame = new DefaultHttp2WindowUpdateFrame(windowIncrement).stream(dataFrame.stream());
            ctx.write(windowFrame);
            ctx.flush();

            state.readWindow += windowIncrement;
        }

        if (dataFrame.isEndStream()) {

            log.info("Got inbound EOS");

            // todo
        }
    }

    private void processPing(ChannelHandlerContext ctx, Http2PingFrame pingFrame) {

        // If this is a ping, reply with a pong

        if (!pingFrame.ack()) {
            var pongFrame = new DefaultHttp2PingFrame(pingFrame.content(), true);
            //ctx.write(pongFrame);
        }
    }

    private void processSettings(ChannelHandlerContext ctx, Http2SettingsFrame settingsFrame) {

        if (outboundSettings == null) {

            Http2Settings.defaultSettings();

            outboundSettings = settingsFrame.settings();
            outboundHandshake = true;

            if (outboundSettings.initialWindowSize() == null) {
                log.info("Using default initial window size");
                outboundSettings.initialWindowSize(DEFAULT_INITIAL_WINDOW_SIZE);
            }

            if (outboundSettings.maxFrameSize() == null) {
                log.info("Using default max frame size");
                outboundSettings.maxFrameSize(DEFAULT_MAX_FRAME_SIZE);
            }

//            connWriteWindow = outboundSettings.initialWindowSize();

            //ctx.write(Http2SettingsAckFrame.INSTANCE);

            if (writable && inboundHandshake && !ready) {
                ready = true;
                ctx.fireChannelActive();
                sendAllQueues(ctx);
            }
        }
        else {

            // If there is a new window size setting, apply the adjustment to all active streams
            // The remote host may not send WINDOW_UPDATE until the new capacity is exhausted

            if (settingsFrame.settings().initialWindowSize() != null) {

                var priorWindowSize = outboundSettings.initialWindowSize();
                var newWindowSize = settingsFrame.settings().initialWindowSize();
                var windowAdjustment = newWindowSize - priorWindowSize;

                log.warn("Applying window size adjustment of [{}]", windowAdjustment);

                for (var streamState : streams.values())
                    streamState.writeWindow += windowAdjustment;
            }

            outboundSettings.putAll(settingsFrame.settings());

//            try {
//                codec.encoder().remoteSettings(settingsFrame.settings());
//            }
//            catch (Http2Exception e) {
//                throw new EUnexpected(e);
//            }

            //ctx.write(Http2SettingsAckFrame.INSTANCE);

            if (settingsFrame.settings().initialWindowSize() != null)
                sendAllQueues(ctx);
        }
    }

    private void processSettingsAck(ChannelHandlerContext ctx) {

        if (!inboundHandshake) {

            inboundHandshake = true;

            if (writable && outboundHandshake && !ready) {
                ready = true;
                ctx.fireChannelActive();
                sendAllQueues(ctx);
            }
        }
    }

    private void processWindowUpdate(ChannelHandlerContext ctx, Http2WindowUpdateFrame windowFrame) {

        if (windowFrame.windowSizeIncrement() <= 0) {
            // todo: error, close connection
        }

        var stream = windowFrame.stream();

        // Updates to the connection-wide window
        if (stream.id() == 0) {

            log.warn("(UNEXPECTED) Increment global write window [{}]", windowFrame.windowSizeIncrement());

//            connWriteWindow += windowFrame.windowSizeIncrement();
            // sendAllQueues(ctx);
        }

        // Updates to individual stream windows
        else {

            var state = streams.get(stream);

            // WINDOW_UPDATE on a closed stream is not an error
            // https://httpwg.org/specs/rfc7540.html#WINDOW_UPDATE
            if (state == null)
                return;

            state.writeWindow += windowFrame.windowSizeIncrement();

            log.warn("Increment stream [{}] write window by [{}], available window = [{}]",
                    stream.id(), windowFrame.windowSizeIncrement(), state.writeWindow);

            sendQueue(ctx, stream);
        }
    }

    private void createStream(Http2FrameStream stream) {

        var state = new StreamState();
        state.readWindow = inboundSettings.initialWindowSize();
        state.writeWindow = outboundSettings.initialWindowSize();
        state.writeQueue = new ArrayDeque<>();

        streams.put(stream, state);
    }

    private void sendAllQueues(ChannelHandlerContext ctx) {

        log.info("Sending all queues");

        var streamIds = streams.keySet();

        for (var streamId : streamIds)
            sendQueue(ctx, streamId);
    }

    private void sendQueue(ChannelHandlerContext ctx, Http2FrameStream streamId) {

        log.info("Sending from queue for stream [{}]...", streamId);

        var streamState = streams.get(streamId);

        if (streamState == null)
            return;  // todo

        while (!streamState.writeQueue.isEmpty()) {

            var queueHead = streamState.writeQueue.peek();
            var dataFrame = (Http2DataFrame) queueHead.getKey();
            var frameSize = dataFrame.content().readableBytes();
            var promise = queueHead.getValue();

            if (ready && streamState.writeWindow >= frameSize) {

                log.info("Sending queued frame");

                streamState.writeQueue.remove();
                sliceAndSend(ctx, dataFrame, promise);
            }
            else {

                log.info("Cannot send more, ready = [{}], writeWindow = [{}], totalSent = [{}]", ready, streamState.writeWindow, totalSent);

                ctx.flush();

                return;
            }
        }

        log.info("All queued frames have been sent, writeWindow = [{}]", streamState.writeWindow);
    }

    private void sliceAndSend(ChannelHandlerContext ctx, Http2DataFrame dataFrame, ChannelPromise promise) {

        var frameSize = dataFrame.content().readableBytes();

        log.info("Sending slice size = [{}]", frameSize);
        ctx.write(dataFrame, promise);

        var stream = streams.get(dataFrame.stream());
        stream.writeWindow -= frameSize;
        totalSent += frameSize;
    }

    private static class StreamState {

        int readWindow;
        int writeWindow;

        Queue<Map.Entry<Object, ChannelPromise>> writeQueue;
    }
}