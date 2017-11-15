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

    protected Bootstrap createBootstrap(EventLoopGroup workerGroup, InternalCache internalCache) {
        return new Bootstrap().group(workerGroup)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_BROADCAST, true)
                .handler(new GridClientHandler(internalCache, this::ackResponseCallback, this::resendCallBack));
    }

    CountDownLatchFuture send(Message msg) throws IOException {
        CountDownLatchFuture latch = new CountDownLatchFuture();
        queueUpMessage(msg, latch);
        processMsgQueue();
        return latch;
    }

    // visible for testing
    void ackResponseCallback(int messageId) {
        LatchAndMessage lAndM = messageIdToLatchAndMessage.remove(messageId);
        lastAckedMessage.set(messageId);

        if (lAndM == null) {
            logger.info("received message with id {} twice. Discarding...", messageId);
        } else {
            logger.info("counting down latch for message id: {}", messageId);
            lAndM.latch.countDown();
        }

        msgInFlight.set(false);
        // piggy back another send
        processMsgQueue();
    }

    private void resendCallBack(int messageIdFromWhereToResend) {
        logger.info("received resend message for id {}", messageIdFromWhereToResend);
    }

    private void processMsgQueue() {
        Integer msgIdToSend = nextMessageToSend();
        if (msgIdToSend != null) {
            LatchAndMessage lnm = messageIdToLatchAndMessage.get(msgIdToSend);
            if (lnm == null) throw new RuntimeException("Can't find msg id " + msgIdToSend + " in my records!");
            try {
                innerSend(lnm.msg);
            } catch (IOException xcp) {
                throw new RuntimeException(xcp);
            }
        }
    }

    private synchronized void queueUpMessage(Message msg, CountDownLatchFuture latch) {
        msg.messageId = generateNextMessageId();
        messageIdToLatchAndMessage.put(msg.messageId, new LatchAndMessage(latch, msg));
        messageIdsToSend.offer(msg.messageId);
    }

    private int generateNextMessageId() {
        if (messageSequenceIdGenerator == Integer.MAX_VALUE) {
            messageSequenceIdGenerator = Integer.MIN_VALUE;
        } else {
            messageSequenceIdGenerator++;
        }

        return messageSequenceIdGenerator;
    }

    private synchronized Integer nextMessageToSend() {
        if (msgInFlight.get()) {
            return null;
        } else {
            msgInFlight.set(true);
            return messageIdsToSend.poll();
        }
    }

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
    }
}
