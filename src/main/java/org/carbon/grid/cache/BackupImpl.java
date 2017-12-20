package org.carbon.grid.cache;

import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;

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
}
