package org.carbon.grid.cache;

import io.netty.buffer.ByteBuf;
import org.carbon.grid.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BackupImplTest extends BaseTest {
    private final short leaderNodeId = 555;

    @Test
    public void testBasic() {
        Backup backup = new BackupImpl();
        CacheLine line = newRandomCacheLine();
        Long leaderEpoch = 123L;
        backup.backUp(leaderNodeId, leaderEpoch, line);
        assertEquals(leaderEpoch, backup.getLeaderEpochFor(leaderNodeId));
        assertEquals(1, backup.getCacheLinesForLeader(leaderNodeId).size());
        assertEquals(line.getVersion(), backup.getCacheLinesForLeader(leaderNodeId).get(line.getId()).getVersion());

        CacheLine line2 = newRandomCacheLine();
        line2.setVersion(line.getVersion() - 10);
        backup.backUp(leaderNodeId, leaderEpoch - 7, line);
        assertEquals(leaderEpoch, backup.getLeaderEpochFor(leaderNodeId));
        assertEquals(1, backup.getCacheLinesForLeader(leaderNodeId).size());
        assertEquals(line.getVersion(), backup.getCacheLinesForLeader(leaderNodeId).get(line.getId()).getVersion());
    }

    private CacheLine newRandomCacheLine() {
        ByteBuf buffer = newRandomBuffer();

        return new CacheLine(
                random.nextLong(),
                random.nextInt(10000),
                leaderNodeId,
                CacheLineState.INVALID,
                buffer
        );
    }
}
