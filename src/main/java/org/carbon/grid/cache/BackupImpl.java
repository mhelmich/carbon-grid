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

import com.google.inject.Singleton;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;

@Singleton
class BackupImpl implements Backup {
    private final NonBlockingHashMapLong<BackupLeaderHolder> backupHolders = new NonBlockingHashMapLong<>();

    @Override
    public void backUp(short leaderId, long leaderEpoch, CacheLine line) {
        BackupLeaderHolder holder = backupHolders.get(leaderId);
        if (holder == null) {
            synchronized (backupHolders) {
                if (backupHolders.get(leaderId) == null) {
                    holder = new BackupLeaderHolder(leaderId);
                    backupHolders.put(leaderId, holder);
                } else {
                    holder = backupHolders.get(leaderId);
                }
            }
        }
        holder.put(leaderEpoch, line);
    }

    @Nullable
    @Override
    public Long getLeaderEpochFor(short leaderId) {
        BackupLeaderHolder holder = backupHolders.get(leaderId);
        if (holder == null) {
            return null;
        } else {
            return holder.getLeaderEpoch();
        }
    }

    @Override
    public Map<Long, CacheLine> getCacheLinesForLeader(short leaderId) {
        BackupLeaderHolder holder = backupHolders.get(leaderId);
        if (holder == null) {
            return Collections.emptyMap();
        } else {
            return holder.getBackup();
        }
    }

    @Override
    public void stopBackupFor(short leaderId) {
        synchronized (backupHolders) {
            BackupLeaderHolder holder = backupHolders.remove(leaderId);
            if (holder != null) {
                holder.close();
            }
        }
    }
}
