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

import com.google.common.annotations.VisibleForTesting;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class generates sequence ids for messages on basis of the node
 * the message is sent from.
 * At the same time, a node is able to ask whether a message id is
 * the next message id to process.
 */
class SequenceGenerator {
    // I chose this odd sequence seed mostly to ease my unit testing
    // however, it doesn't matter what the seed is ... it breaks over anyways
    private final static int startingSequence = Integer.MAX_VALUE - 1;
    private final NonBlockingHashMap<Short, AtomicInteger> sentMessages = new NonBlockingHashMap<>();
    private final NonBlockingHashMap<Short, AtomicInteger> handledRequests = new NonBlockingHashMap<>();
    private final NonBlockingHashMap<Short, AtomicInteger> handledResponses = new NonBlockingHashMap<>();

    // this code is a little bit dicey as it
    // should guarantee to only give you subsequent numbers
    // and on top of that doesn't block
    int nextSequenceForMessageToSend(short receiverNodeId) {
        sentMessages.putIfAbsent(receiverNodeId, new AtomicInteger(startingSequence));
        AtomicInteger seq = sentMessages.get(receiverNodeId);

        int newSequence = next(seq.get());
        while (!seq.compareAndSet(prev(newSequence), newSequence)) {
            newSequence = next(seq.get());
        }

        return newSequence;
    }

    boolean isNextFor(Message msg) {
        if (Message.Request.class.isInstance(msg)) {
            return isNextFor((Message.Request)msg);
        } else {
            return isNextFor((Message.Response)msg);
        }
    }

    boolean isNextFor(Message.Request msg) {
        return isNextForRequests(msg.getSender(), msg.getMessageSequenceNumber());
    }

    boolean isNextFor(Message.Response msg) {
        return isNextForResponsess(msg.getSender(), msg.getMessageSequenceNumber());
    }

    @VisibleForTesting
    boolean isNextForRequests(short senderNodeId, int observedSequence) {
        return isNext(handledRequests, senderNodeId, observedSequence);
    }

    @VisibleForTesting
    boolean isNextForResponsess(short senderNodeId, int observedSequence) {
        return isNext(handledResponses, senderNodeId, observedSequence);
    }

    private boolean isNext(NonBlockingHashMap<Short, AtomicInteger> map, short senderNodeId, int observedSequence) {
        AtomicInteger seq = map.putIfAbsent(senderNodeId, new AtomicInteger(startingSequence));
        seq = (seq == null) ? map.get(senderNodeId) : seq;
        int lastObserved = seq.get();
        int lastObservedPlusOne = next(lastObserved);
        return lastObservedPlusOne == observedSequence;
    }

    void markRequestHandled(short senderNodeId, int seenSequence) {
        AtomicInteger seq = handledRequests.get(senderNodeId);
        boolean hasAdded = seq.compareAndSet(prev(seenSequence), seenSequence);
        assert hasAdded;
    }

    void markResponseHandled(short senderNodeId, int seenSequence) {
        AtomicInteger seq = handledResponses.get(senderNodeId);
        boolean hasAdded = seq.compareAndSet(prev(seenSequence), seenSequence);
        assert hasAdded;
    }

    private int next(int observedSequence) {
        return observedSequence == Integer.MAX_VALUE ? Integer.MIN_VALUE : observedSequence + 1;
    }

    private int prev(int observedSequence) {
        return observedSequence == Integer.MIN_VALUE ? Integer.MAX_VALUE : observedSequence - 1;
    }
}
