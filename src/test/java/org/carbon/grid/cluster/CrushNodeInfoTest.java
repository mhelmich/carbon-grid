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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class CrushNodeInfoTest {
    @Test
    public void testReadWriteMaster() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Set<Short> replicas = new HashSet<Short>() {{
            add((short)66);
            add((short)77);
            add((short)88);
            add((short)99);
        }};
        Set<Short> leader = new HashSet<Short>() {{
            add((short)111);
            add((short)222);
            add((short)333);
            add((short)444);
        }};
        NodeInfo ni1 = new NodeInfo((short)15, "dc1", replicas, leader);
        String ni1Str = objectMapper.writeValueAsString(ni1);
        NodeInfo ni2 = objectMapper.readValue(ni1Str, NodeInfo.class);
        assertEqualsNodeInfo(ni1, ni2);
    }

    @Test
    public void testReadWriteReplica() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        NodeInfo ni1 = new NodeInfo((short)15, "dc1", Collections.emptySet(), new HashSet<Short>() {{ add((short)19); }});
        String ni1Str = objectMapper.writeValueAsString(ni1);
        NodeInfo ni2 = objectMapper.readValue(ni1Str, NodeInfo.class);
        assertEqualsNodeInfo(ni1, ni2);
    }

    private void assertEqualsNodeInfo(NodeInfo ni1, NodeInfo ni2) {
        assertEquals(ni1.nodeId, ni2.nodeId);
        assertEquals(ni1.replicaIds, ni2.replicaIds);
        assertEquals(ni1.leaderIds, ni2.leaderIds);
    }
}
