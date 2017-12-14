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

import com.google.common.net.HostAndPort;
import com.orbitz.consul.Consul;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.AfterClass;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.mockito.Mockito.when;

public class BaseTest {
    protected static final int TIMEOUT_SECS = 555;
    protected final Random random = new Random();
    private static final Set<ByteBuf> buffers = new HashSet<>();

    protected CarbonGrid.ServerConfig mockServerConfig(int port) {
        CarbonGrid.ServerConfig sc = Mockito.mock(CarbonGrid.ServerConfig.class);
        when(sc.port()).thenReturn(port);
        when(sc.timeout()).thenReturn(60);
        return sc;
    }

    protected CarbonGrid.ConsulConfig mockConsulConfig() {
        CarbonGrid.ConsulConfig sc = Mockito.mock(CarbonGrid.ConsulConfig.class);
        when(sc.host()).thenReturn("localhost");
        when(sc.port()).thenReturn(getConsulPort());
        when(sc.timeout()).thenReturn(60);
        return sc;
    }

    protected CarbonGrid.CacheConfig mockCacheConfig() {
        CarbonGrid.CacheConfig cc = Mockito.mock(CarbonGrid.CacheConfig.class);
        when(cc.maxAvailableMemory()).thenReturn(Long.MAX_VALUE);
        when(cc.maxCacheLineSize()).thenReturn(Integer.MAX_VALUE);
        return cc;
    }

    protected ByteBuf newRandomBuffer() {
        byte[] bites = new byte[1024];
        random.nextBytes(bites);
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(1024);
        buffer.writeBytes(bites);
        buffers.add(buffer);
        return buffer;
    }

    protected Consul createConsul() {
        return Consul.builder()
                .withHostAndPort(HostAndPort.fromParts("localhost", getConsulPort()))
                .build();
    }

    private int getConsulPort() {
        return 32769;
    }

    // not the nicest but effective
    @AfterClass
    public static void clearBuffers() {
        for (ByteBuf buf : buffers) {
            if (buf.refCnt() > 0) {
                buf.release();
            }
        }
        buffers.clear();
    }
}
