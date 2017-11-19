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

package org.carbon.grid;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class generates sequence ids for messages on basis of the node
 * the message is sent to.
 * At the same time, a node is able to ask whether a message id is
 * the next message id to process.
 */
class SequenceGenerator {
    private final static NonBlockingHashMap<Short, AtomicInteger> nodeIdToSequence = new NonBlockingHashMap<>();

    int next(short nodeId) {
        AtomicInteger count = nodeIdToSequence.get(nodeId);
        if (count == null) {
            nodeIdToSequence.putIfAbsent(nodeId, new AtomicInteger(Integer.MIN_VALUE));
            count = nodeIdToSequence.get(nodeId);
        }

        int newId = count.incrementAndGet();
        if (newId == Integer.MAX_VALUE) {
            // the only way the value cannot be "current" is when a different
            // thread swept in and "incremented" the integer
            // and since the only way the int can be incremented from here
            // is flowing over, the result is irrelevant to us
            count.compareAndSet(newId, Integer.MIN_VALUE);
        }

        // this also means newId is never MIN_VALUE
        return newId;
    }

    boolean isNextFor(short nodeId, int sequence) {
        AtomicInteger count = nodeIdToSequence.get(nodeId);
        return count == null || count.get() + 1 == sequence;
    }
}
