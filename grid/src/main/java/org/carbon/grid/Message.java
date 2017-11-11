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

import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

abstract class Message {

    // NEVER CHANGE THE ORDINALS!!!
    // first parameter is the ordinal
    // the second parameter is the message byte size after reading the first type byte
    enum MessageType {
        PUT((byte)0, 999),
        GET((byte)1, 10),
        ACK((byte)2, 0),
        INVALIDATE((byte)3, 999),
        INVALIDATE_ACK((byte)4, 999),
        BACKUP((byte)5, 999),
        BACUP_ACK((byte)6, 999);

        private final static Map<Byte, MessageType> lookUp = new HashMap<>(MessageType.values().length);
        static {
            Set<Byte> ordinals = new HashSet<>(MessageType.values().length);
            for (MessageType type : MessageType.values()) {
                lookUp.put(type.ordinal, type);

                if (!ordinals.add(type.ordinal)) {
                    throw new RuntimeException("Can't add ordinal " + type.ordinal + " twice!");
                }
            }
        }

        final byte ordinal;
        final int byteSize;
        MessageType(byte ordinal, int byteSize) {
            this.ordinal = ordinal;
            this.byteSize = byteSize;
        }

        static MessageType fromByte(byte b) {
            return lookUp.get(b);
        }
    }

    static Message newMessage(MessageType type, ByteBuf bites) {
        switch (type) {
//            case GET:
//                return new GET(bites.readShort(), bites.readLong());
            default:
                throw new RuntimeException("Unknown message type");
        }
    }

}
