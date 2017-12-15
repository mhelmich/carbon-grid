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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * These nodes make up a decision tree.
 * See this paper for ideas around crush replica placement:
 * https://ceph.com/wp-content/uploads/2016/08/weil-crush-sc06.pdf
 *
 * A node can have two modes:
 * - bucket: a bucket is an inner node in the decision tree
 *   that has multiple children and represents a grouping in the cluster
 *   (all nodes in a particular data center for example)
 * - leaf: a leaf has an idea and represents a single node in the cluster
 */
class CrushNode {
    private final HashMap<String, Integer> nameToChild;
    // only buckets will have children
    private final LinkedList<CrushNode> children;
    // statically seeded hash function
    // this hash function needs to produce consistent hashes
    private final HashFunction f = Hashing.murmur3_32(0);

    private final CrushHierarchyLevel type;
    // only for leaf nodes
    private final Short nodeId;

    // the state of a node
    private boolean isDead = false;
    private boolean isFull = false;

    CrushNode(CrushHierarchyLevel type) {
        this.type = type;
        this.nodeId = null;
        this.children = new LinkedList<>();
        this.nameToChild = new HashMap<>();
    }

    CrushNode(CrushHierarchyLevel type, Short nodeId) {
        this.type = type;
        this.nodeId = nodeId;
        this.children = null;
        this.nameToChild = null;
    }

    boolean isAvailable() {
        return !isFull && !isDead;
    }

    void addChild(CrushNode b) {
        addChild(UUID.randomUUID().toString(), b);
    }

    void addChild(String name, CrushNode b) {
        if (isLeaf()) throw new IllegalStateException("Can't add child to leaf node");
        children.add(b);
        nameToChild.put(name, children.size() - 1);
    }

    CrushNode getChildByName(String name) {
        if (isLeaf()) throw new IllegalStateException("Can't get child from leaf node");
        Integer idx = nameToChild.get(name);
        if (idx == null) {
            return null;
        } else {
            return children.get(idx);
        }
    }

    CrushHierarchyLevel getCrushHierarchyTag() {
        return type;
    }

    CrushNode selectChild(Long cacheLineId, int rPrime) {
        if (isLeaf()) throw new IllegalStateException("Can't select child from a leaf node");
        // selecting a child node today is done on basis of consistent hashing
        // other (better) placement methods are imaginable (especially when a node weight
        // as approximation of remaining space on the node is to be considered)
        // for more inspiration, see the original paper:
        // https://ceph.com/wp-content/uploads/2016/08/weil-crush-sc06.pdf
        // TODO -- change this from consistent hashing to rendez-vous hashing
        int hash = f.hashLong(cacheLineId).asInt();
        hash = Math.abs(hash);
        hash += rPrime;
        hash = (hash % children.size());
        return children.get(hash);
    }

    void setFull(boolean full) {
        this.isFull = full;
    }

    void setDead(boolean dead) {
        this.isDead = dead;
    }

    List<CrushNode> getChildren() {
        if (isLeaf()) throw new IllegalStateException("Can't get child from leaf node");
        return children;
    }

    boolean isLeaf() {
        return children == null || nameToChild == null;
    }

    short getNodeId() {
        if (!isLeaf()) throw new IllegalStateException("Can't get nodeId from non-leaf node");
        return nodeId;
    }

    @Override
    public String toString() {
        return "nodeId: " + nodeId + " children: " + children;
    }
}
