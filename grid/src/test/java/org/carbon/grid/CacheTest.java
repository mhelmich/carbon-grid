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

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertTrue;

public class CacheTest {
    @Test
    public void testPingPong() throws IOException, ExecutionException, InterruptedException {
        short node1 = 123;
        short node2 = 456;
        int port1 = 4444;
        int port2 = 5555;
        InternalCacheImpl internalCache1 = new InternalCacheImpl(node1, port1);
        InternalCacheImpl internalCache2 = new InternalCacheImpl(node2, port2);
        internalCache1.comms.addPeer(node2, "localhost", port2);
        internalCache1.comms.addPeer(node1, "localhost", port1);

        Message.GET get = new Message.GET(node2, 999L);
        Future<Void> f1 = internalCache1.comms.send(get);
        f1.get();
        assertTrue(f1.isDone());
    }

    @Test
    public void testGetPutCache() {
        ThreeCaches threeCaches = createCluster();
    }

    private ThreeCaches createCluster() {
        short node1 = 123;
        short node2 = 456;
        short node3 = 789;
        int port1 = 4444;
        int port2 = 5555;
        int port3 = 6666;
        InternalCacheImpl internalCache1 = new InternalCacheImpl(node1, port1);
        InternalCacheImpl internalCache2 = new InternalCacheImpl(node2, port2);
        InternalCacheImpl internalCache3 = new InternalCacheImpl(node3, port3);

        internalCache1.comms.addPeer(node2, "localhost", port2);
        internalCache1.comms.addPeer(node3, "localhost", port3);
        internalCache2.comms.addPeer(node1, "localhost", port1);
        internalCache2.comms.addPeer(node3, "localhost", port3);
        internalCache3.comms.addPeer(node1, "localhost", port1);
        internalCache3.comms.addPeer(node2, "localhost", port2);

        return new ThreeCaches(internalCache1, internalCache2, internalCache3);
    }

    private static class ThreeCaches {
        final InternalCacheImpl cache1;
        final InternalCacheImpl cache2;
        final InternalCacheImpl cache3;
        ThreeCaches(InternalCacheImpl cache1, InternalCacheImpl cache2, InternalCacheImpl cache3) {
            this.cache1 = cache1;
            this.cache2 = cache2;
            this.cache3 = cache3;
        }
    }
}
