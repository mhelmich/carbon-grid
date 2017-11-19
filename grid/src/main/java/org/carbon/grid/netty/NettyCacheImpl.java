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

import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NettyCacheImpl {
    private final static Logger logger = LoggerFactory.getLogger(NettyCacheImpl.class);
    // the cache lines I own
    private final NonBlockingHashMapLong<NettyCacheLine> owned = new NonBlockingHashMapLong<>();
    // the cache lines I'm sharing and somebody else owns
    private final NonBlockingHashMapLong<NettyCacheLine> shared = new NonBlockingHashMapLong<>();

    final short myNodeId;
    final GridComm comms;

    NettyCacheImpl(int myNodeId, int myPort) {
        this.myNodeId = (short)myNodeId;
        this.comms = new GridCommImpl(myNodeId, myPort, this);
    }
}
