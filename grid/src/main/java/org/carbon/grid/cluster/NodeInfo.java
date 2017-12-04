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

package org.carbon.grid.cluster;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class NodeInfo implements Serializable {
    private final static String SEPARATOR = ";";
    private final static String SET_SEPARATOR = ",";
    final short nodeId;
    // tri-state boolean => true, false, null are meaningful values
    final Boolean isMaster;
    final Set<Short> replicaIds;
    final short masterId;

    NodeInfo(String s) {
        String[] tokens = s.split(SEPARATOR);
        assert tokens.length == 3;
        this.nodeId = Short.valueOf(tokens[0]);
        // isMaster can't be null as we have 3 tokens
        this.isMaster = Boolean.valueOf(tokens[1]);
        if (isMaster) {
            String[] replicaIdsStr = tokens[2].split(SET_SEPARATOR);
            Set<Short> replicaIdsTmp = new HashSet<>(replicaIdsStr.length);
            for (String idStr : replicaIdsStr) {
                replicaIdsTmp.add(Short.valueOf(idStr));
            }
            this.replicaIds = ImmutableSet.copyOf(replicaIdsTmp);
            this.masterId = -1;
        } else {
            this.replicaIds = Collections.emptySet();
            this.masterId = Short.valueOf(tokens[2]);
        }
    }

    NodeInfo(short nodeId) {
        this(nodeId, null, Collections.emptySet(), (short)-1);
    }

    NodeInfo(short nodeId, Set<Short> replicaIds) {
        this(nodeId, true, replicaIds, (short)-1);
    }

    NodeInfo(short nodeId, short masterId) {
        this(nodeId, false, Collections.emptySet(), masterId);
    }

    private NodeInfo(short nodeId, Boolean isMaster, Set<Short> replicaIds, short masterId) {
        this.nodeId = nodeId;
        this.isMaster = isMaster;
        this.replicaIds = ImmutableSet.copyOf(replicaIds);
        this.masterId = masterId;
    }

    String toConsulValue() {
        StringBuilder sb = new StringBuilder();
        sb.append(nodeId).append(SEPARATOR).append(isMaster).append(SEPARATOR);
        if (isMaster == null) {
            throw new IllegalStateException("Can't publish node info with isMaster == null");
        } else if (isMaster) {
            sb.append(StringUtils.join(replicaIds, SET_SEPARATOR));
        } else {
            sb.append(masterId);
        }
        return sb.toString();
    }
}
