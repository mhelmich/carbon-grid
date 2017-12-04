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

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class NodeInfoTest {
    @Test
    public void testReadWriteMaster() {
        Set<Short> replicas = new HashSet<Short>() {{
            add((short)66);
            add((short)77);
            add((short)88);
            add((short)99);
        }};
        NodeInfo ni1 = new NodeInfo((short)15, replicas);
        String value1 = ni1.toConsulValue();

        NodeInfo ni2 = new NodeInfo(value1);
        assertEqualsNodeInfo(ni1, ni2);
    }

    @Test
    public void testReadWriteReplica() {
        NodeInfo ni1 = new NodeInfo((short)15, (short)19);
        String value1 = ni1.toConsulValue();

        NodeInfo ni2 = new NodeInfo(value1);
        assertEqualsNodeInfo(ni1, ni2);
    }

    @Test(expected = IllegalStateException.class)
    public void testReadWriteFail() {
        NodeInfo ni1 = new NodeInfo((short)15);
        String value1 = ni1.toConsulValue();
    }

    private void assertEqualsNodeInfo(NodeInfo ni1, NodeInfo ni2) {
        assertEquals(ni1.nodeId, ni2.nodeId);
        assertEquals(ni1.isLeader, ni2.isLeader);
        assertEquals(ni1.replicaIds, ni2.replicaIds);
        assertEquals(ni1.leaderId, ni2.leaderId);
    }
}
