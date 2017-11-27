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

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

class CompletableFutureUtil {
    private CompletableFutureUtil() {}

    // wraps a single future templatized with MessageType into the generic
    // void future that is passed to the cache
    static CompletableFuture<Void> wrap(CompletableFuture<MessageType> future) {
        return CompletableFuture.allOf(future);
    }

    // generates a future that completes if all futures passed in have completed
    static CompletableFuture<Void> waitForAllMessages(Collection<CompletableFuture<MessageType>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]));
    }

    // generates a future that completes if either
    // a) all passed in futures have completed
    // or b) messages with the specified types have completed
    // or c) if a single future completes with an exception
    static CompletableFuture<Void> waitForAllMessagesOrSpecifiedList(Collection<CompletableFuture<MessageType>> futures, MessageType... messagesToWaitFor) {
        // generates a future that completes when all child futures have completed
        CompletableFuture<Void> promise = waitForAllMessages(futures);

        // build of message type counter map
        NonBlockingHashMap<MessageType, AtomicInteger> typeToCount = new NonBlockingHashMap<>();
        for (MessageType type : messagesToWaitFor) {
            typeToCount.putIfAbsent(type, new AtomicInteger(0));
            typeToCount.get(type).incrementAndGet();
        }

        for (CompletableFuture<MessageType> f : futures) {
            f.whenComplete((messageType, xcp) -> {
                if (xcp == null) {
                    AtomicInteger count = typeToCount.get(messageType);
                    if (count != null) {
                        if (count.decrementAndGet() <= 0) {
                            typeToCount.remove(messageType);
                        }
                    }
                    if (typeToCount.isEmpty()) {
                        // if the map is empty complete the parent future early
                        promise.complete(null);
                    }
                } else {
                    // if a single future completes with an error,
                    // complete the parent future with an error as well
                    promise.completeExceptionally(xcp);
                }
            });
        }

        return promise;
    }
}
