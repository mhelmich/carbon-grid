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

package org.carbon.grid.cache;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * The only purpose for this class to exist is to not have references of the cache to appear in cluster code.
 * A circular dependency injection problem makes this boiler place code necessary :/
 */
@Singleton
class PeerChangeConsumerImpl implements PeerChangeConsumer {
    private final InternalCache cache;

    @Inject
    PeerChangeConsumerImpl(InternalCache cache) {
        this.cache = cache;
    }

    @Override
    public void accept(Map<Short, InetSocketAddress> idsToAddr) {
        cache.handlePeerChange(idsToAddr);
    }
}
