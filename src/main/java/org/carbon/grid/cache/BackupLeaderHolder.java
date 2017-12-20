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

import com.google.common.collect.ImmutableMap;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

import java.io.Closeable;
import java.util.Map;

class BackupLeaderHolder implements Closeable {
    private final NonBlockingHashMapLong<CacheLine> backUp = new NonBlockingHashMapLong<>();
    private final short leaderId;
    private long leaderEpoch = Long.MIN_VALUE;

    BackupLeaderHolder(short leaderId) {
        this.leaderId = leaderId;
    }

    long getLeaderEpoch() {
        return leaderEpoch;
    }

    synchronized void put(long newLeaderEpoch, CacheLine line) {
        if (leaderEpoch < newLeaderEpoch) {
            leaderEpoch = newLeaderEpoch;
            CacheLine oldLine = backUp.get(line.getId());
            if (oldLine == null) {
                backUp.put(line.getId(), line);
            } else {
                if (oldLine.getVersion() < line.getVersion()) {
                    // release the buffer so that we don't have worry about that anywhere else
                    oldLine.releaseData();
                    backUp.put(line.getId(), line);
                } else {
                    // destroy the buffer since we don't want to take care of this
                    // higher up the stack
                    line.releaseData();
                }
            }
        } else {
            // destroy the buffer since we don't want to take care of this
            // higher up the stack
            line.releaseData();
        }
    }

    Map<Long, CacheLine> getBackup() {
        // defensive copy
        // keep in mind that we don't increase the reference count on the buffers
        // so technically the backup holder still owns them
        return ImmutableMap.copyOf(backUp);
    }

    @Override
    public String toString() {
        return "leaderId: " + leaderId + " leaderEpoch: " + leaderEpoch;
    }

    @Override
    public void close() {
        backUp.forEach((id, line) -> line.releaseData());
    }
}
