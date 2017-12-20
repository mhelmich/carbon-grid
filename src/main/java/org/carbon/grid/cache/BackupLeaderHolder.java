package org.carbon.grid.cache;

import com.google.common.collect.ImmutableMap;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

import java.io.Closeable;
import java.io.IOException;
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
        if (leaderEpoch <= newLeaderEpoch) {
            leaderEpoch = newLeaderEpoch;
            CacheLine oldLine = backUp.get(line.getId());
            if (oldLine == null) {
                backUp.put(line.getId(), line);
            } else {
                if (oldLine.getVersion() < line.getVersion()) {
                    backUp.put(line.getId(), line);
                }
            }
        }
    }

    Map<Long, CacheLine> getBackup() {
        return ImmutableMap.copyOf(backUp);
    }

    @Override
    public String toString() {
        return "leaderId: " + leaderId + " leaderEpoch: " + leaderEpoch;
    }

    @Override
    public void close() throws IOException {
        backUp.forEach((id, line) -> line.releaseData());
    }
}
