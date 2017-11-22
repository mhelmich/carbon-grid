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
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class MessageSerializationTest {
    private static final Message.DeserializationMessageFactory messageFactory = new Message.DeserializationMessageFactory();
    private final Random random = new Random();

    @Test
    public void testSerialization() throws IOException {
        short sender = 12345;
        long lineId = 1234567890;
        int messageSequence = 1234512345;

        Message.ACK ack = new Message.ACK(messageSequence, sender, lineId);
        runTest(ack);

        Message.PUT put = new Message.PUT(messageSequence, sender, lineId, 19, null);
        runTest(put);

        ByteBuf buf = newRandomBuffer();
        try {
            put = new Message.PUT(messageSequence, sender, lineId, 19, buf);
            runTest(put);
        } finally {
            buf.release();
        }

        Set<Short> sharers = new HashSet<Short>() {{
            add((short)17);
            add((short)19);
            add((short)23);
        }};
        Message.PUTX putx = new Message.PUTX(messageSequence, sender, lineId, -19, sharers, null);
        runTest(putx);

        buf = newRandomBuffer();
        try {
            putx = new Message.PUTX(messageSequence, sender, lineId, -19, sharers, buf);
            runTest(putx);
        } finally {
            buf.release();
        }

        Message.GET get = new Message.GET(sender, lineId);
        runTest(get);

        Message.GETX getx = new Message.GETX(sender, lineId);
        runTest(getx);

        Message.OWNER_CHANGED ownerChanged = new Message.OWNER_CHANGED(messageSequence, sender, lineId, (short)115, MessageType.GET);
        runTest(ownerChanged);

        Message.INV inv = new Message.INV(sender, lineId);
        runTest(inv);

        Message.INVACK invAck = new Message.INVACK(messageSequence, sender, lineId);
        runTest(invAck);
    }

    private void runTest(Message msgToSerialize) throws IOException {
        ByteBuf buf = serialize(msgToSerialize);
        try {
            Message deserializedMsg = deserialize(buf);
            assertEquals(msgToSerialize.calcMessagesByteSize(), buf.capacity());
            assertEquals(msgToSerialize, deserializedMsg);
            assertEquals(msgToSerialize.hashCode(), deserializedMsg.hashCode());
        } finally {
            buf.release();
        }
    }

    private ByteBuf serialize(Message msg) throws IOException {
        ByteBuf outBites = PooledByteBufAllocator.DEFAULT.directBuffer(msg.calcMessagesByteSize());
        try (MessageOutput out = new MessageOutput(outBites)) {
            msg.write(out);
        }
        return outBites;
    }

    private Message deserialize(ByteBuf inBites) throws IOException {
        MessageType requestMessageType = MessageType.fromByte(inBites.readByte());
        Message message = messageFactory.createMessageShellForType(requestMessageType);

        try (MessageInput in = new MessageInput(inBites)) {
            message.read(in);
        }

        return message;
    }

    private ByteBuf newRandomBuffer() {
        byte[] bites = new byte[1024];
        random.nextBytes(bites);
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(1024);
        buffer.writeBytes(bites);
        return buffer;
    }
}
