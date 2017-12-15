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

import java.io.Closeable;

public interface Cluster extends Closeable {
    /**
     * Cached state of the upness of this node inside the cluster.
     * Call it as often as you like. This boolean will be updated after every session renewal run.
     */
    boolean isUp();

    /**
     * The node id of this node.
     */
    short myNodeId();

    /**
     * Returns a id allocator that spits out globally unique cache line ids.
     */
    GloballyUniqueIdAllocator getIdAllocator();

    /**
     * This returns a replica supplier which in turn
     * can be used to get all replica ids for this node.
     * The supplier can be called for every usage and
     * the returning values don't need to be cached.
     */
    ReplicaSupplier getReplicaIds();
}
