package org.carbon.grid.cache;

import javax.annotation.Nullable;
import java.util.Map;

public interface Backup {

    void backUp(short leaderId, long leaderEpoch, CacheLine line);

    @Nullable
    Long getLeaderEpochFor(short leaderId);

    Map<Long, CacheLine> getCacheLinesForLeader(short leaderId);
}
