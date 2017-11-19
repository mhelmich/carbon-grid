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

package org.carbon.grid.netty;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * NEVER CHANGE THE ORDINALS!!!
 * first parameter is the ordinal
 * the second parameter is the message byte size after reading the first type byte
 */
enum NettyMessageType {
    PUT((byte)0),
    GET((byte)1),
    ACK((byte)2),
    INVALIDATE((byte)3),
    INVALIDATE_ACK((byte)4),
    BACKUP((byte)5),
    BACUP_ACK((byte)6),
    RESEND((byte)7),
    GETX((byte)8),
    PUTX((byte)9),
    OWNER_CHANGED((byte)10)
    ;

    final static Map<Byte, NettyMessageType> byteToType;
    static {
        Set<Byte> ordinals = new HashSet<>(NettyMessageType.values().length);
        Map<Byte, NettyMessageType> tmpByteToType = new HashMap<>(NettyMessageType.values().length);
        for (NettyMessageType type : NettyMessageType.values()) {
            tmpByteToType.put(type.ordinal, type);

            if (!ordinals.add(type.ordinal)) {
                throw new RuntimeException("Can't add ordinal " + type.ordinal + " twice!");
            }
        }

        byteToType = ImmutableMap.copyOf(tmpByteToType);
    }

    final byte ordinal;
    NettyMessageType(byte ordinal) {
        this.ordinal = ordinal;
    }
}
