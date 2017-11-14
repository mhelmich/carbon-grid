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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

class OrderPreservingUdpGridClient implements Closeable {
    private final static Logger logger = LoggerFactory.getLogger(OrderPreservingUdpGridClient.class);
    private final ChannelFuture channelFuture;
    private final InetSocketAddress addr;

    private final ConcurrentHashMap<Integer, LatchAndMessage> messageIdToLatchAndMessage = new ConcurrentHashMap<>(128, .75f, 64);
    private final LinkedBlockingQueue<Integer> messageIdsToSend = new LinkedBlockingQueue<>();
    private final AtomicInteger lastAckedMessage = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger messageIdGenerator = new AtomicInteger(Integer.MIN_VALUE);

    OrderPreservingUdpGridClient(InetSocketAddress addr, EventLoopGroup workerGroup, InternalCache internalCache) {
        Bootstrap b = new Bootstrap();
        b.group(workerGroup)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_BROADCAST, true)
                .handler(new GridClientHandler(internalCache, this::ackResponseCallback, this::resendCallBack));
        this.channelFuture = b.bind(0);
        this.addr = addr;
    }

    CountDownLatchFuture send(Message msg) throws IOException {
        msg.messageId = nextMessageId();
        CountDownLatchFuture latch = new CountDownLatchFuture();
        messageIdToLatchAndMessage.put(msg.messageId, new LatchAndMessage(latch, msg));
        messageIdsToSend.offer(msg.messageId);
        processMsgQueue();
        return latch;
    }

    private int nextMessageId() {
        if (messageIdGenerator.get() == Integer.MAX_VALUE) {
            messageIdGenerator.set(Integer.MIN_VALUE);
        }
        return messageIdGenerator.incrementAndGet();
    }

    private void ackResponseCallback(int messageId) {
        LatchAndMessage lAndM = messageIdToLatchAndMessage.remove(messageId);
        lastAckedMessage.set(messageId);

        if (lAndM == null) {
            logger.info("received message with id {} twice. Discarding...", messageId);
        } else {
            logger.info("counting down latch for message id: {}", messageId);
            lAndM.latch.countDown();
        }

        // piggy back another send
        processMsgQueue();
    }

    private void resendCallBack(int messageIdFromWhereToResend) {

    }

    private void processMsgQueue() {
        Integer msgIdToSend = messageIdsToSend.poll();
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

    private ChannelFuture innerSend(Message msg) throws IOException {
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

    private static class LatchAndMessage {
        final CountDownLatchFuture latch;
        final Message msg;
        LatchAndMessage(CountDownLatchFuture latch, Message msg) {
            this.latch = latch;
            this.msg = msg;
        }
    }
}
