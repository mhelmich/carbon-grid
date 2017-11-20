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
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;

public class GridCommunicationsTest {
    @Test
    public void testBacklog() throws IOException {
        short sender = 444;
        long lineId = 1234567890;
        InternalCache cacheMock = Mockito.mock(InternalCache.class);
        try (GridCommunications comm = new GridCommunications(sender, 4444, cacheMock)) {
            Message.ACK ack1 = new Message.ACK(Integer.MAX_VALUE, sender, lineId);
            Message.ACK ack2 = new Message.ACK(Integer.MIN_VALUE, sender, lineId);
            comm.handleMessage(ack2);
            assertEquals(1, comm.getBacklogMap().size());
            comm.handleMessage(ack1);
            assertEquals(0, comm.getBacklogMap().size());
        }
    }

    @Test
    public void testBacklogSkipOne() throws IOException {
        short sender = 444;
        long lineId = 1234567890;
        InternalCache cacheMock = Mockito.mock(InternalCache.class);
        try (GridCommunications comm = new GridCommunications(sender, 4444, cacheMock)) {
            Message.ACK ack1 = new Message.ACK(Integer.MAX_VALUE, sender, lineId);
            Message.ACK ack2 = new Message.ACK(Integer.MIN_VALUE, sender, lineId);
            Message.ACK ack3 = new Message.ACK(Integer.MIN_VALUE + 1, sender, lineId);
            comm.handleMessage(ack3);
            assertEquals(1, comm.getBacklogMap().size());
            comm.handleMessage(ack1);
            assertEquals(1, comm.getBacklogMap().size());
            comm.handleMessage(ack2);
            assertEquals(0, comm.getBacklogMap().size());
        }
    }

    @Test
    public void testBacklogConcurrent() throws IOException {
        short sender = 444;
        long lineId = 1234567890;
        InternalCache cacheMock = Mockito.mock(InternalCache.class);
        doAnswer(inv -> {
            Message.Response resp = inv.getArgumentAt(0, Message.Response.class);
            if (resp.getMessageSequenceNumber() == Integer.MAX_VALUE) {
                // wait for other message added to the backlog
            }
            return null;
        }).when(cacheMock).handleResponse(any(Message.Response.class));
        try (GridCommunications comm = new GridCommunications(sender, 4444, cacheMock)) {
            Message.ACK ack1 = new Message.ACK(Integer.MAX_VALUE, sender, lineId);
            Message.ACK ack2 = new Message.ACK(Integer.MIN_VALUE, sender, lineId);
            comm.handleMessage(ack2);
            comm.handleMessage(ack1);
        }
    }
}
