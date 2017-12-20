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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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

        // shouldn't update because of lower epoch
        CacheLine line2 = newRandomCacheLine(line.getId());
        line2.setVersion(line.getVersion() - 10);
        backup.backUp(leaderNodeId, leaderEpoch - 7, line2);
        assertEquals(leaderEpoch, backup.getLeaderEpochFor(leaderNodeId));
        assertEquals(1, backup.getCacheLinesForLeader(leaderNodeId).size());
        assertEquals(line.getVersion(), backup.getCacheLinesForLeader(leaderNodeId).get(line.getId()).getVersion());
        assertEquals(Unpooled.EMPTY_BUFFER, line2.resetReaderAndGetReadOnlyData());

        // shouldn't update because of lower version
        CacheLine line3 = newRandomCacheLine(line.getId());
        line3.setVersion(line.getVersion() - 10);
        Long newLeaderEpoch = leaderEpoch + 7;
        backup.backUp(leaderNodeId, newLeaderEpoch, line3);
        assertEquals(newLeaderEpoch, backup.getLeaderEpochFor(leaderNodeId));
        assertEquals(1, backup.getCacheLinesForLeader(leaderNodeId).size());
        assertEquals(line.getVersion(), backup.getCacheLinesForLeader(leaderNodeId).get(line.getId()).getVersion());
        assertEquals(Unpooled.EMPTY_BUFFER, line3.resetReaderAndGetReadOnlyData());

        // updates!!!
        CacheLine line4 = newRandomCacheLine(line.getId());
        int newVersion = line.getVersion() + 10;
        line4.setVersion(newVersion);
        newLeaderEpoch++;
        backup.backUp(leaderNodeId, newLeaderEpoch, line4);
        assertEquals(newLeaderEpoch, backup.getLeaderEpochFor(leaderNodeId));
        assertEquals(1, backup.getCacheLinesForLeader(leaderNodeId).size());
        assertEquals(newVersion, backup.getCacheLinesForLeader(leaderNodeId).get(line.getId()).getVersion());
        assertEquals(1, line4.resetReaderAndGetReadOnlyData().refCnt());
        // old buffer has been released
        assertEquals(Unpooled.EMPTY_BUFFER, line.resetReaderAndGetReadOnlyData());

        backup.stopBackupFor(leaderNodeId);
        assertEquals(0, backup.getCacheLinesForLeader(leaderNodeId).size());
    }

    private CacheLine newRandomCacheLine(long id) {
        ByteBuf buffer = newRandomBuffer();

        return new CacheLine(
                id,
                random.nextInt(10000),
                leaderNodeId,
                CacheLineState.INVALID,
                buffer
        );
    }

    private CacheLine newRandomCacheLine() {
        return newRandomCacheLine(random.nextLong());
    }
}
