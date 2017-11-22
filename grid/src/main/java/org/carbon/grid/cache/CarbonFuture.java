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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class CarbonFuture implements Future<Void> {
    private final CompletableFuture<Void> latch = new CompletableFuture<>();
    private NonBlockingHashMap<MessageType, AtomicInteger> typeToCount;

    CarbonFuture() {
        this.typeToCount = null;
    }

    CarbonFuture(MessageType... messagesToWaitFor) {
        typeToCount = new NonBlockingHashMap<>();
        for (MessageType type : messagesToWaitFor) {
            typeToCount.putIfAbsent(type, new AtomicInteger(0));
            typeToCount.get(type).incrementAndGet();
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return latch.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return latch.isCancelled();
    }

    @Override
    public boolean isDone() {
        return latch.isDone();
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        return latch.get();
    }

    @Override
    public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return latch.get(timeout, unit);
    }

    public boolean completeExceptionally(Throwable ex) {
        return latch.completeExceptionally(ex);
    }

    public boolean complete(MessageType type) {
        if (typeToCount == null) {
            return latch.complete(null);
        } else {
            AtomicInteger count = typeToCount.get(type);
            if (count != null) {
                if (count.decrementAndGet() <= 0) {
                    typeToCount.remove(type);
                }
            }

            return typeToCount.isEmpty() && latch.complete(null);
        }
    }
}
