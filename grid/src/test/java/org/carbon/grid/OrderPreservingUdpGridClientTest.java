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
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.util.internal.SocketUtils;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.LinkedList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OrderPreservingUdpGridClientTest {
    @Test
    public void testMessagesInQueue() throws IOException, NoSuchFieldException, IllegalAccessException {
        EventLoopGroup workerGroup = Mockito.mock(EventLoopGroup.class);
        InternalCache cache = Mockito.mock(InternalCache.class);
        InetSocketAddress addr = SocketUtils.socketAddress("localhost", 9876);
        try (
                OrderPreservingUdpGridClient client = new OrderPreservingUdpGridClient((short)7879, addr, workerGroup, cache) {
                    @Override
                    protected Bootstrap createBootstrap(EventLoopGroup workerGroup, InternalCache internalCache) {
                        return Mockito.mock(Bootstrap.class);
                    }

                    @Override
                    protected ChannelFuture innerSend(Message msg) throws IOException {
                        return null;
                    }

                    @Override
                    public void close() throws IOException {
                        // no op
                    }
                }
        ) {
            LinkedList<Integer> mq = getMessageQueue(client);
            Message m1 = new Message.GET(456, 1234567);
            Message m2 = new Message.GET(456, 2345678);
            Message m3 = new Message.GET(456, 3456789);
            assertEquals(0, mq.size());
            CountDownLatchFuture f1 = client.send(m1);
            assertEquals(0, mq.size());
            assertFalse(f1.isDone());
            CountDownLatchFuture f2 = client.send(m2);
            assertEquals(1, mq.size());
            assertFalse(f1.isDone());
            assertFalse(f2.isDone());
            CountDownLatchFuture f3 = client.send(m3);
            assertEquals(2, mq.size());
            assertFalse(f1.isDone());
            assertFalse(f2.isDone());
            assertFalse(f3.isDone());

            client.ackResponseCallback(m1.messageId);
            assertEquals(1, mq.size());
            assertTrue(f1.isDone());
            assertFalse(f2.isDone());
            assertFalse(f3.isDone());

            client.ackResponseCallback(m2.messageId);
            assertEquals(0, mq.size());
            assertTrue(f1.isDone());
            assertTrue(f2.isDone());
            assertFalse(f3.isDone());

            client.ackResponseCallback(m3.messageId);
            assertEquals(0, mq.size());
            assertTrue(f1.isDone());
            assertTrue(f2.isDone());
            assertTrue(f3.isDone());
        }
    }

    @Test(expected = RuntimeException.class)
    public void testSendFailed() throws IOException, NoSuchFieldException, IllegalAccessException {
        EventLoopGroup workerGroup = Mockito.mock(EventLoopGroup.class);
        InternalCache cache = Mockito.mock(InternalCache.class);
        InetSocketAddress addr = SocketUtils.socketAddress("localhost", 9876);
        try (
                OrderPreservingUdpGridClient client = new OrderPreservingUdpGridClient((short)7879, addr, workerGroup, cache) {
                    @Override
                    protected Bootstrap createBootstrap(EventLoopGroup workerGroup, InternalCache internalCache) {
                        return Mockito.mock(Bootstrap.class);
                    }

                    @Override
                    protected ChannelFuture innerSend(Message msg) throws IOException {
                        throw new IOException("BOOOM -- this has been planted for you");
                    }

                    @Override
                    public void close() throws IOException {
                        // no op
                    }
                }
        ) {
            LinkedList<Integer> mq = getMessageQueue(client);
            Message m1 = new Message.GET(456, 1234567);
            assertEquals(0, mq.size());
            client.send(m1);
        }
    }

    @SuppressWarnings("unchecked")
    private LinkedList<Integer> getMessageQueue(OrderPreservingUdpGridClient client) throws IllegalAccessException, NoSuchFieldException {
        Field field = OrderPreservingUdpGridClient.class.getDeclaredField("messageIdsToSend");
        field.setAccessible(true);
        Object value = field.get(client);
        return (LinkedList<Integer>) value;
    }
}
