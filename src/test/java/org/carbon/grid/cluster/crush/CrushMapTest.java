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

package org.carbon.grid.cluster.crush;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CrushMapTest {
    @Test
    public void testBasicHierarchy() {
        Node root = buildNodeTree();
        CrushMap crt = CrushMap.builder()
                .addPlacementRule(CrushHierarchyLevel.DATA_CENTER, 2, i -> true)
                .addPlacementRule(CrushHierarchyLevel.NODE, 1, i -> true)
                .build();

        List<Short> nodeIds = crt.placeCacheLine(99L, root);
        assertEquals(2, nodeIds.size());
        assertTrue(nodeIds.indexOf((short)511) >= 0);
        assertTrue(nodeIds.indexOf((short)503) >= 0);
    }

    @Test
    public void testDisabledNodes() {
        Node root = new Node(CrushHierarchyLevel.ROOT);
        Node dc1 = new Node(CrushHierarchyLevel.DATA_CENTER);
        Node dc2 = new Node(CrushHierarchyLevel.DATA_CENTER);
        root.addChild(dc1);
        root.addChild(dc2);

        dc1.addChild(new Node(CrushHierarchyLevel.NODE, (short)500));
        Node node501 = new Node(CrushHierarchyLevel.NODE, (short)501);
        dc1.addChild(node501);
        dc1.addChild(new Node(CrushHierarchyLevel.NODE, (short)502));
        dc1.addChild(new Node(CrushHierarchyLevel.NODE, (short)503));

        dc2.addChild(new Node(CrushHierarchyLevel.NODE, (short)510));
        dc2.addChild(new Node(CrushHierarchyLevel.NODE, (short)511));
        dc2.addChild(new Node(CrushHierarchyLevel.NODE, (short)512));

        CrushMap crt = CrushMap.builder()
                .addPlacementRule(CrushHierarchyLevel.DATA_CENTER, 2, i -> true)
                .addPlacementRule(CrushHierarchyLevel.NODE, 1, i -> true)
                .build();

        List<Short> nodeIds = crt.placeCacheLine(111L, root);
        assertEquals(2, nodeIds.size());
        assertTrue(nodeIds.indexOf((short)511) >= 0);
        assertTrue(nodeIds.indexOf((short)501) >= 0);

        node501.setFull(true);
        nodeIds = crt.placeCacheLine(111L, root);
        assertEquals(2, nodeIds.size());
        assertTrue(nodeIds.indexOf((short)511) >= 0);
        assertTrue(nodeIds.indexOf((short)502) >= 0);
    }

    @Test
    public void testDoesntMatchPredicate() {
        Node root = new Node(CrushHierarchyLevel.ROOT);
        Node dc1 = new Node(CrushHierarchyLevel.DATA_CENTER);
        Node dc2 = new Node(CrushHierarchyLevel.DATA_CENTER);
        root.addChild(dc1);
        root.addChild(dc2);

        dc1.addChild(new Node(CrushHierarchyLevel.NODE, (short)500));
        Node node501 = new Node(CrushHierarchyLevel.NODE, (short)501);
        dc1.addChild(node501);
        dc1.addChild(new Node(CrushHierarchyLevel.NODE, (short)502));
        dc1.addChild(new Node(CrushHierarchyLevel.NODE, (short)503));

        dc2.addChild(new Node(CrushHierarchyLevel.NODE, (short)510));
        dc2.addChild(new Node(CrushHierarchyLevel.NODE, (short)511));
        dc2.addChild(new Node(CrushHierarchyLevel.NODE, (short)512));

        CrushMap crt = CrushMap.builder()
                .addPlacementRule(CrushHierarchyLevel.DATA_CENTER, 2, i -> true)
                .addPlacementRule(CrushHierarchyLevel.NODE, 1, i -> i.getNodeId() != 501)
                .build();

        List<Short> nodeIds = crt.placeCacheLine(123456789L, root);
        assertEquals(2, nodeIds.size());
        assertTrue(nodeIds.indexOf((short)511) >= 0);
        assertTrue(nodeIds.indexOf((short)502) >= 0);
    }

    @Test(expected = RuntimeException.class)
    public void testFailure() {
        Node root = buildNodeTree();
        CrushMap crt = CrushMap.builder()
                .addPlacementRule(CrushHierarchyLevel.DATA_CENTER, 2, i -> true)
                .addPlacementRule(CrushHierarchyLevel.NODE, 1, i -> false)
                .build();

        crt.placeCacheLine(99L, root);
    }

    private Node buildNodeTree() {
        Node root = new Node(CrushHierarchyLevel.ROOT);
        Node dc1 = new Node(CrushHierarchyLevel.DATA_CENTER);
        Node dc2 = new Node(CrushHierarchyLevel.DATA_CENTER);
        root.addChild(dc1);
        root.addChild(dc2);

        dc1.addChild(new Node(CrushHierarchyLevel.NODE, (short)500));
        dc1.addChild(new Node(CrushHierarchyLevel.NODE, (short)501));
        dc1.addChild(new Node(CrushHierarchyLevel.NODE, (short)502));
        dc1.addChild(new Node(CrushHierarchyLevel.NODE, (short)503));

        dc2.addChild(new Node(CrushHierarchyLevel.NODE, (short)510));
        dc2.addChild(new Node(CrushHierarchyLevel.NODE, (short)511));
        dc2.addChild(new Node(CrushHierarchyLevel.NODE, (short)512));

        return root;
    }
}
