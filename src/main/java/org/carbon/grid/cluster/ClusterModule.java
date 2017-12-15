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

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

public class ClusterModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(Cluster.class).to(ConsulCluster.class).in(Singleton.class);
        // this is ... well ... boiler plate code
        // it resolves a cyclic dependency between cluster and cache
        // to get around the cycle of death, I'm injecting a provider for the id
        // of the current node
        // the cache will access the provider only when the id is needed as opposed to during object creation
        bind(Short.class).annotatedWith(MyNodeId.class).toProvider(MyNodeIdProvider.class).in(Singleton.class);
        bind(GloballyUniqueIdAllocator.class).toProvider(GloballyUniqueIdAllocatorProvider.class).in(Singleton.class);
        bind(ReplicaSupplier.class).toProvider(ReplicaSupplierProvider.class).in(Singleton.class);
    }
}
