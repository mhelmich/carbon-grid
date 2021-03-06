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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * This class encapsulates much of the crush placement algorithm.
 * See https://ceph.com/wp-content/uploads/2016/08/weil-crush-sc06.pdf
 *
 * It acts as rule table store and at the same time is able to compute
 * crush replica placements from a decision tree and a cache line id.
 */
class CrushMap {
    private final static int NUM_ALLOWED_RETRIES = 5;
    // the list of rules of how the decision tree is to be walked
    private final List<Rule> rules;

    private CrushMap(List<Rule> rules) {
        this.rules = rules;
    }

    static Builder builder() {
        return new Builder();
    }

    List<Short> calculateReplicaNodes(short nodeId, CrushNode root) {
        return placeCacheLine((long)nodeId, root);
    }

    List<Short> placeCacheLine(long cacheLineId, CrushNode root) {
        List<CrushNode> nodes = Collections.singletonList(root);
        // roll over all rules which descends me all the way to the leaf
        for (Rule r : rules) {
            List<CrushNode> newNodes = new LinkedList<>();
            for (CrushNode x : nodes) {
                List<CrushNode> n = select(cacheLineId, x, r.numNodesToSelect, true, r.predicate);
                newNodes.addAll(n);
            }
            nodes = newNodes;
        }

        // convert everything to node ids
        return nodes.stream().map(CrushNode::getNodeId).collect(Collectors.toList());
    }

    static class Builder {
        private final LinkedList<Rule> rules = new LinkedList<>();

        private Builder() {}

        Builder addPlacementRule(CrushHierarchyLevel level, int numNodesToSelect, Predicate<CrushNode> predicate) {
            rules.add(new Rule(level, numNodesToSelect, predicate));
            return this;
        }

        CrushMap build() {
            return new CrushMap(rules);
        }
    }

    /**
     * This is what a crush rule looks like...
     */
    static class Rule {
        final CrushHierarchyLevel level;
        final int numNodesToSelect;
        final Predicate<CrushNode> predicate;

        private Rule(CrushHierarchyLevel level, int numNodesToSelect, Predicate<CrushNode> predicate) {
            this.level = level;
            this.numNodesToSelect = numNodesToSelect;
            this.predicate = predicate;
        }
    }

    // the crush algorithm code
    private List<CrushNode> select(Long cacheLineId, CrushNode parent, int numItemsToSelect, boolean firstN, Predicate<CrushNode> matchesType) {
        if (numItemsToSelect <= 0) return Collections.emptyList();
        if (parent.getChildren().size() < numItemsToSelect) throw new RuntimeException("Can't pick " + numItemsToSelect + " nodes out of " + parent.getChildren().size() + " available nodes.");
        List<CrushNode> selected = new LinkedList<>();

        int numFailures = 0;
        for (int r = 0; r < numItemsToSelect; r++) {
            boolean retryOnParent = false;
            CrushNode selectedNode;
            do {
                boolean retryOnX = false;
                CrushNode x = parent;
                do {
                    // rPrime is an addition to the hash the is generated off of
                    // the cache line id
                    // it controls which node is to be picked in case the chosen
                    // node is full or down
                    int rPrime = firstN
                            ? r + numFailures
                            : r + (numFailures * numItemsToSelect);
                    selectedNode = x.selectChild(cacheLineId, rPrime);
                    boolean nodeMatchesPredicate = matchesType.test(selectedNode);
                    boolean wasSelectedAlready = selected.contains(selectedNode);

                    // this code doesn't verify the type of a node
                    // I'm explicitly assuming that I want to traverse
                    // through the entire tree all the way to the leaf

                    if (selectedNode.isAvailable()
                            && !wasSelectedAlready
                            && nodeMatchesPredicate) {
                        // if all looks good, we pick this node
                        break;
                    } else if (!selectedNode.isAvailable()) {
                        numFailures++;
                        if (numFailures < NUM_ALLOWED_RETRIES) {
                            // we try to find a new child on this node
                            retryOnX = true;
                        } else {
                            // we track all the way back to the parent
                            retryOnParent = true;
                        }
                    } else if (wasSelectedAlready || !nodeMatchesPredicate) {
                        // we don't like the node for whatever reason
                        // it's either selected already or the predicate excludes it
                        // let's try again on this node
                        numFailures++;
                        retryOnX = true;
                    }

                    if (numFailures > NUM_ALLOWED_RETRIES) {
                        throw new RuntimeException("Too many errors during crush mapping");
                    }
                } while (retryOnX);
            } while (retryOnParent);
            selected.add(selectedNode);
        }
        return selected;
    }
}
