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

import com.orbitz.consul.ConsulException;
import com.orbitz.consul.async.ConsulResponseCallback;
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.option.ImmutableQueryOptions;
import com.orbitz.consul.option.QueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.math.BigInteger;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

class ConsulValueWatcher<T> implements Closeable {
    private final static Logger logger = LoggerFactory.getLogger(ConsulValueWatcher.class);
    private final AtomicBoolean watcherIsRunning = new AtomicBoolean(true);
    private final AtomicReference<BigInteger> latestIndex = new AtomicReference<>(null);
    private final AtomicReference<List<T>> latestResponseList = new AtomicReference<>(null);

    private final ScheduledExecutorService executorService;
    private final ConsulResponseProcessor<T> responseProcessor;
    private final ConsulRequestCallback<T> requestCallback;

    private final ConsulResponseCallback<List<T>> responseCallback = new ConsulResponseCallback<List<T>>() {
        @Override
        public void onComplete(ConsulResponse<List<T>> consulResponse) {
            if (consulResponse.isKnownLeader()) {
                if (watcherIsRunning.get()) {
                    logger.info("onComplete with latest {} and response {}", latestIndex.get(), consulResponse.getIndex());
                    latestIndex.set(consulResponse.getIndex());
                    List<T> responseList = consulResponse.getResponse();
                    if (responseList != null && !responseList.equals(latestResponseList.get())) {
                        latestResponseList.set(responseList);
                        responseProcessor.accept(responseList);
                    }
                    runNodeHealthListener();
                }
            } else {
                onFailure(new ConsulException("Consul cluster has no elected leader"));
            }
        }

        @Override
        public void onFailure(Throwable throwable) {
            if (watcherIsRunning.get()) {
                if (!SocketTimeoutException.class.equals(throwable.getClass())) {
                    logger.error("", throwable);
                }
                executorService.schedule(() -> runNodeHealthListener(), 3, TimeUnit.SECONDS);
            }
        }
    };

    ConsulValueWatcher(ScheduledExecutorService executorService, ConsulRequestCallback<T> requestCallback, ConsulResponseProcessor<T> responseProcessor) {
        this.executorService = executorService;
        this.requestCallback = requestCallback;
        this.responseProcessor = responseProcessor;
        executorService.submit(this::runNodeHealthListener);
    }

    private void runNodeHealthListener() {
        if (watcherIsRunning.get()) {
            requestCallback.accept(latestIndex.get(), responseCallback);
        }
    }

    @Override
    public void close() {
        watcherIsRunning.set(false);
    }

    static QueryOptions generateBlockingQueryOptions(BigInteger index, int blockSeconds) {
        return generateBlockingQueryOptions(index, blockSeconds, QueryOptions.BLANK);
    }

    private static QueryOptions generateBlockingQueryOptions(BigInteger index, int blockSeconds, QueryOptions queryOptions) {
        if (index == null) {
            return QueryOptions.BLANK;
        } else {
            return ImmutableQueryOptions.builder()
                    .from(QueryOptions.blockSeconds(blockSeconds, index).build())
                    .token(queryOptions.getToken())
                    .consistencyMode(queryOptions.getConsistencyMode())
                    .near(queryOptions.getNear())
                    .datacenter(queryOptions.getDatacenter())
                    .build();
        }
    }

    @FunctionalInterface
    interface ConsulRequestCallback<V> extends BiConsumer<BigInteger, ConsulResponseCallback<List<V>>> { }

    @FunctionalInterface
    interface ConsulResponseProcessor<V> extends Consumer<List<V>> { }
}
