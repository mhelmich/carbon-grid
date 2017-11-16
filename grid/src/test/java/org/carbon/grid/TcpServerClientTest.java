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

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.internal.SocketUtils;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;

public class TcpServerClientTest {
    @Test
    public void testBasic() throws IOException, ExecutionException, InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        int serverPort = new Random().nextInt(60000) + 2000;

        Message.Request get = new Message.GET((short)123, 123456789L);

        InternalCache cacheServer = new InternalCacheImpl(123, serverPort);
        InternalCache cacheClient = Mockito.mock(InternalCache.class);

        try {
            try (TcpGridServer server = new TcpGridServer(serverPort, bossGroup, workerGroup, cacheServer)) {
                try (TcpGridClient client = new TcpGridClient((short)123, SocketUtils.socketAddress("localhost", serverPort), workerGroup, cacheClient)) {
                    CountDownLatchFuture latch = client.send(get);
                    latch.get();
                    assertTrue(latch.isDone());
                }
            }
        } finally {
            try {
                bossGroup.shutdownGracefully();
            } finally {
                workerGroup.shutdownGracefully();
            }
        }
    }
}
