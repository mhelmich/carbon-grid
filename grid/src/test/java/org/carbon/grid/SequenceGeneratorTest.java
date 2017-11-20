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

import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SequenceGeneratorTest {
    @Test
    public void testSequence() {
        short nodeId = (short)123;
        SequenceGenerator gen = new SequenceGenerator();
        int seq1 = gen.nextSequenceForMessageToSend(nodeId);
        int seq2 = gen.nextSequenceForMessageToSend(nodeId);
        assertEquals(seq1 + 1, seq2);
//        assertTrue(gen.isNextFor(nodeId, Integer.MAX_VALUE));
//        assertFalse(gen.isNextFor((short)987, 555));
    }

    @Test
    public void testConcurrent() throws ExecutionException, InterruptedException {
        int numThreads = 31;
        int numSequences = 3000;
        short s = 123;
        Set<Integer> set = Collections.newSetFromMap(new ConcurrentHashMap<>(numSequences, 0.75f, numThreads));
        SequenceGenerator gen = new SequenceGenerator();
        Set<Future<?>> futures = new HashSet<>();
        ExecutorService es = Executors.newFixedThreadPool(numThreads);
        try {

            for (int j = 0; j < numThreads; j++) {
                futures.add(es.submit(() -> {
                    Random r = new Random();
                    for (int i = 0; i < numSequences; i++) {
                        try {
                            Thread.sleep(r.nextInt(2));
                        } catch (InterruptedException e) {
                            fail();
                        }
                        int sq = gen.nextSequenceForMessageToSend(s);
                        if (set.contains(sq)) {
                            fail();
                        } else {
                            set.add(sq);
                        }
                    }
                }));
            }

            for (Future<?> f : futures) {
                f.get();
            }
            assertEquals(numThreads * numSequences, set.size());
        } finally {
            es.shutdown();
        }
    }

    @Test
    public void testMarkSeen() {
        SequenceGenerator gen = new SequenceGenerator();
        short node = 321;
//        assertTrue(gen.isNextFor(node, Integer.MAX_VALUE));
//        assertFalse(gen.isNextFor(node, Integer.MIN_VALUE));
//        assertFalse(gen.isNextFor(node, Integer.MIN_VALUE + 1));
//        gen.markRequestHandled(node, Integer.MAX_VALUE);
//
//        assertFalse(gen.isNextFor(node, Integer.MAX_VALUE));
//        assertTrue(gen.isNextFor(node, Integer.MIN_VALUE));
//        assertFalse(gen.isNextFor(node, Integer.MIN_VALUE + 1));
//        gen.markRequestHandled(node, Integer.MIN_VALUE);
//
//        assertFalse(gen.isNextFor(node, Integer.MAX_VALUE));
//        assertFalse(gen.isNextFor(node, Integer.MIN_VALUE));
//        assertTrue(gen.isNextFor(node, Integer.MIN_VALUE + 1));
    }
}
