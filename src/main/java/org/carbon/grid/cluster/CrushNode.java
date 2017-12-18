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
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

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
    private final NonBlockingHashMap<String, Integer> nameToChild;
    // only buckets will have children
    private final CopyOnWriteArrayList<CrushNode> children;
    private final Selector selector = new ConsistentHashSelector();

    private final CrushHierarchyLevel type;
    // only for leaf nodes
    private final Short nodeId;
    private final String nodeName;

    // the state of a node
    private boolean isDead = false;
    private boolean isFull = false;

    CrushNode(CrushHierarchyLevel type, String nodeName) {
        this.type = type;
        this.nodeName = nodeName;
        this.nodeId = null;
        this.nameToChild = new NonBlockingHashMap<>();
        this.children = new CopyOnWriteArrayList<>();
    }

    CrushNode(CrushHierarchyLevel type, Short nodeId) {
        this.type = type;
        this.nodeId = nodeId;
        this.nodeName = String.valueOf(nodeId);
        this.nameToChild = null;
        this.children = null;
    }

    boolean isAvailable() {
        return !isFull && !isDead;
    }

    void addChild(CrushNode b) {
        addChild(b.getNodeName(), b);
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
        return selector.select(cacheLineId, rPrime, children);
    }

    boolean isFull() {
        return isFull;
    }

    boolean isDead() {
        return isDead;
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

    String getNodeName() {
        return nodeName;
    }

    @Override
    public String toString() {
        return "nodeId: " + nodeId + " nodeName: " + nodeName + " children: " + children;
    }

    interface Selector {
        CrushNode select(long cacheLineId, int rPrime, CopyOnWriteArrayList<CrushNode> children);
    }

    private static class ConsistentHashSelector implements Selector {
        // statically seeded hash function
        // this hash function needs to produce consistent hashes
        private final HashFunction f = Hashing.murmur3_32(2147483647);

        @Override
        public CrushNode select(long cacheLineId, int rPrime, CopyOnWriteArrayList<CrushNode> children) {
            int hash = f.hashLong(cacheLineId).asInt();
            hash = Math.abs(hash);
            // adding rPrime after hashing gives us a nice failover behavior
            // where in case nodes are unavailable, we just hop over one more step
            // to the next node
            hash += rPrime;
            hash = (hash % children.size());
            return children.get(hash);
        }
    }

    // TODO -- this needs a little more thinking
    // the addition of rPrime doesn't provide a predictable failover behavior anymore
    // maybe we're better off if the node chosen in case of unavailability is predictable
    private static class RendezVousHashSelector implements Selector {
        private final HashFunction f = Hashing.murmur3_32(2147483647);

        @Override
        public CrushNode select(long cacheLineId, int rPrime, CopyOnWriteArrayList<CrushNode> children) {
            int maxValue = Integer.MIN_VALUE;
            CrushNode theOneIWant = null;
            for (CrushNode cn : children) {
                int hash = f.newHasher()
                        .putLong(cacheLineId)
                        .putInt(rPrime)
                        .putString(cn.getNodeName(), Charset.defaultCharset())
                        .hash()
                        .asInt();

                if (hash > maxValue) {
                    maxValue = hash;
                    theOneIWant = cn;
                }
            }
            return theOneIWant;
        }
    }
}
