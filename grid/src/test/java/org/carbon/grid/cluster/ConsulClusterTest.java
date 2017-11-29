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

package org.carbon.grid.cluster;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ConsulClusterTest {
    @Test
    public void testBasic() throws IOException {
        try (ConsulCluster cluster = new ConsulCluster(9999)) {
            assertEquals(String.valueOf(ConsulCluster.MIN_NODE_ID), cluster.myNodeId);
        }
    }

    @Test
    public void testBasicMultipleClusters() throws IOException {
        try (ConsulCluster cluster123 = new ConsulCluster(7777)) {
            try (ConsulCluster cluster456 = new ConsulCluster(8888)) {
                try (ConsulCluster cluster789 = new ConsulCluster(9999)) {
                    assertEquals(String.valueOf(ConsulCluster.MIN_NODE_ID), cluster123.myNodeId);
                    assertEquals(String.valueOf(ConsulCluster.MIN_NODE_ID + 1), cluster456.myNodeId);
                    assertEquals(String.valueOf(ConsulCluster.MIN_NODE_ID + 2), cluster789.myNodeId);
                }
            }
        }
    }
}
