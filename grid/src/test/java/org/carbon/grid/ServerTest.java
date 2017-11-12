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

public class ServerTest {
    @Test
    public void testBasic() throws IOException, ExecutionException, InterruptedException {
        short node1 = 123;
        short node2 = 456;
        int port1 = 4444;
        int port2 = 5555;
        Cache cache1 = new CacheImpl();
        Cache cache2 = new CacheImpl();
        GridCommunications comm1 = new GridCommunications(port1, cache1);
        GridCommunications comm2 = new GridCommunications(port2, cache2);
        comm1.addPeer(node2, "localhost", port1);
        comm2.addPeer(node1, "localhost", port2);

        Message.GET get = new Message.GET(node2, 999L);
        Future<Void> f1 = comm1.send(get);
        f1.get();
        assertTrue(f1.isDone());
    }
}
