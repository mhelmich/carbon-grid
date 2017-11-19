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

import com.google.common.collect.ImmutableSet;

import java.util.Set;

class NettyMessageFactory {
    NettyMessageType getMessageTypeFromByte(byte b) {
        return NettyMessageType.byteToType.get(b);
    }

    @SuppressWarnings("deprecation")
    NettyMessage getMessageForType(NettyMessageType type) {
        switch (type) {
            case ACK:
                return new NettyMessage.ACK();
            case PUT:
                return new NettyMessage.PUT();
            case PUTX:
                return new NettyMessage.PUTX();
            case GET:
                return new NettyMessage.GET();
            case GETX:
                return new NettyMessage.GETX();
            case OWNER_CHANGED:
                return new NettyMessage.OWNER_CHANGED();
            default:
                throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    private static final Set<NettyMessageType> needLock = ImmutableSet.of(
            NettyMessageType.GETX,
            NettyMessageType.PUTX,
            NettyMessageType.INVALIDATE,
            NettyMessageType.PUT
    );

    boolean needsLock(NettyMessageType type) {
        return needLock.contains(type);
    }
}
