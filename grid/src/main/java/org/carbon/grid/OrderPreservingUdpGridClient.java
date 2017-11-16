/*
 * Copyright 2017 Marco Helmich
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.carbon.grid;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This client class is responsible for sending out UDP packets to one particular node in the cluster.
 * It does preserve ordering of messages as in: Messages will be received by the other party in the order they are being sent.
 * This class does so by implementing a simple stop-and-wait ARQ (see https://en.wikipedia.org/wiki/Sliding_window_protocol).
 *
 * ToDos:
 * - build more efficient message packing and sending
 * -- right now we send one message and wait for acknowledgement
 */
class OrderPreservingUdpGridClient implements Closeable {
    private final static Logger logger = LoggerFactory.getLogger(OrderPreservingUdpGridClient.class);

    private final NonBlockingHashMap<Integer, LatchAndMessage> messageIdToLatchAndMessage = new NonBlockingHashMap<>();
    private final AtomicInteger lastAckedMessage = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicBoolean msgInFlight = new AtomicBoolean(false);

    // as long as these two members are only called inside synchronized blocks
    // it's perfectly fine to use non-thread-safe data structures
    private final LinkedList<Integer> messageIdsToSend = new LinkedList<>();
    private int messageSequenceIdGenerator = Integer.MIN_VALUE;

    private final short theNodeITalkTo;
    private final ChannelFuture channelFuture;
    private final InetSocketAddress addr;

    OrderPreservingUdpGridClient(short theNodeITalkTo, InetSocketAddress addr, EventLoopGroup workerGroup, InternalCache internalCache) {
        this.theNodeITalkTo = theNodeITalkTo;
        Bootstrap b = createBootstrap(workerGroup, internalCache);
        this.channelFuture = b.bind(0);
        this.addr = addr;
    }

    // visible for testing
    protected Bootstrap createBootstrap(EventLoopGroup workerGroup, InternalCache internalCache) {
        return new Bootstrap().group(workerGroup)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_BROADCAST, true)
                .handler(new UdpGridClientHandler(internalCache, this::ackResponseCallback, this::resendCallBack));
    }

    /**
     * This is virtually non-blocking.
     * Technically it calls synchronized methods down below
     * but the code is designed to asynchronously send the message
     * or enqueue the message in a queue to be sent later.
     * Therefore this method returns "right away" and takes care of the
     * sending later.
     */
    CountDownLatchFuture send(Message msg) throws IOException {
        CountDownLatchFuture latch = new CountDownLatchFuture();
        queueUpMessage(msg, latch);
        processMsgQueue();
        return latch;
    }

    // visible for testing
    void ackResponseCallback(int messageId) {
        LatchAndMessage lAndM = messageIdToLatchAndMessage.remove(messageId);
        // this is a little loose code
        // if we ever move away from send-and-wait ARQ,
        // we will have to deal with multiple threads competing
        lastAckedMessage.set(messageId);

        if (lAndM == null) {
            logger.info("received message with id {} twice. Discarding...", messageId);
        } else {
            logger.info("releasing latch for message with id: {}", messageId);
            lAndM.latch.countDown();
        }

        msgInFlight.set(false);
        // piggy back another send if possible
        processMsgQueue();
    }

    private void resendCallBack(int messageIdFromWhereToResend) {
        logger.info("received resend message for id {}", messageIdFromWhereToResend);
    }

    /**
     * This methods triggers the sending process...
     * It's called in two places -- when a consumer calls send and asynchronously when the client handler
     * acknowledges an incoming message. At any point in time we may choose to not do anything as there
     * is a message in-flight already.
     */
    private void processMsgQueue() {
        Integer msgIdToSend = nextMessageToSend();
        // null means no message to send because a different message is still in-flight
        if (msgIdToSend != null) {
            LatchAndMessage lnm = messageIdToLatchAndMessage.get(msgIdToSend);
            // if we have a message id in the queue and no corresponding message in the map,
            // that's a big problem
            assert lnm != null;
            try {
                innerSend(lnm.msg);
                // only remove the message from the queue after sending it succeeded
                messageIdsToSend.poll();
            } catch (IOException xcp) {
                throw new RuntimeException(xcp);
            }
        }
    }

    /////////////////////////////////////////////////
    /////////////////////////////////////////////
    /////////////////////////////////
    // concurrency right now is dealt with by these
    // two synchronized methods
    // they make sure all our bookkeeping lines up
    // you might argue that this implementation of
    // concurrency control has optimization potential
    // and I can't disagree but I deem this fine for now

    private synchronized void queueUpMessage(Message msg, CountDownLatchFuture latch) {
        msg.messageId = generateNextMessageId();
        messageIdToLatchAndMessage.put(msg.messageId, new LatchAndMessage(latch, msg));
        messageIdsToSend.offer(msg.messageId);
    }

    // nullable
    private synchronized Integer nextMessageToSend() {
        if (msgInFlight.get()) {
            return null;
        } else {
            msgInFlight.set(true);
            return messageIdsToSend.peek();
        }
    }

    private int generateNextMessageId() {
        if (messageSequenceIdGenerator == Integer.MAX_VALUE) {
            messageSequenceIdGenerator = Integer.MIN_VALUE;
        } else {
            messageSequenceIdGenerator++;
        }

        return messageSequenceIdGenerator;
    }

    /**
     * This method takes care of the actual serialization and sending of messages.
     */
    protected ChannelFuture innerSend(Message msg) throws IOException {
        Channel ch = channelFuture.syncUninterruptibly().channel();
        ByteBuf bites = ch.alloc().buffer(msg.calcByteSize());
        try (MessageOutput out = new MessageOutput(bites)) {
            msg.write(out);
        }

        return ch.writeAndFlush(new DatagramPacket(bites, addr));
    }

    @Override
    public void close() throws IOException {
        channelFuture.channel().close();
    }

    @Override
    public String toString() {
        return "theNodeITalkTo: " + theNodeITalkTo + " addr: " + addr;
    }

    private static class LatchAndMessage {
        final CountDownLatchFuture latch;
        final Message msg;
        LatchAndMessage(CountDownLatchFuture latch, Message msg) {
            this.latch = latch;
            this.msg = msg;
        }

        @Override
        public String toString() {
            return "latch: " + latch + " msg: " + msg;
        }
    }
}
