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

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

class CompositeFuture implements Future<Void> {

    private final Collection<Future<Void>> futures;

    CompositeFuture(Collection<Future<Void>> futures) {
        this.futures = futures;
    }

    CompositeFuture(Collection<Future<Void>> futures, MessageType... messagesToWaitFor) {
        this.futures = futures;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return futures.stream().map(f -> f.cancel(mayInterruptIfRunning)).reduce(true, (a,b) -> a && b);
    }

    @Override
    public boolean isCancelled() {
        return futures.stream().map(Future::isCancelled).reduce(true, (a, b) -> a && b);
    }

    @Override
    public boolean isDone() {
        return futures.stream().map(Future::isDone).reduce(true, (a, b) -> a && b);
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        for (Future<Void> f : futures) {
            f.get();
        }
        return null;
    }

    @Override
    public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        long millisToWait = MILLISECONDS.convert(timeout, unit);
        for (Future<Void> f : futures) {
            long before = System.currentTimeMillis();
            f.get(millisToWait, TimeUnit.MILLISECONDS);
            millisToWait = millisToWait - (System.currentTimeMillis() - before);
            if (millisToWait <= 0) throw new TimeoutException();
        }
        return null;
    }
}
