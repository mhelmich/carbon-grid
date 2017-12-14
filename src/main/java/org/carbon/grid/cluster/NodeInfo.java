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
import java.util.HashSet;
import java.util.Set;

class NodeInfo implements Serializable {
    private final static String SEPARATOR = ";";
    private final static String SET_SEPARATOR = ",";
    final short nodeId;
    final String dataCenter;
    final Set<Short> replicaIds;
    final short leaderId;

    NodeInfo(String s) {
        String[] tokens = s.split(SEPARATOR);
        assert tokens.length == 4;
        this.nodeId = Short.valueOf(tokens[0]);
        this.dataCenter = tokens[1];
        String[] replicaIdsStr = tokens[2].split(SET_SEPARATOR);
        Set<Short> replicaIdsTmp = new HashSet<>(replicaIdsStr.length);
        for (String idStr : replicaIdsStr) {
            if (!idStr.isEmpty()) {
                replicaIdsTmp.add(Short.valueOf(idStr));
            }
        }
        this.replicaIds = ImmutableSet.copyOf(replicaIdsTmp);
        this.leaderId = Short.valueOf(tokens[3]);
    }

    NodeInfo(short nodeId, String dataCenter, Set<Short> replicaIds, short leaderId) {
        this.nodeId = nodeId;
        this.dataCenter = dataCenter;
        this.replicaIds = ImmutableSet.copyOf(replicaIds);
        this.leaderId = leaderId;
    }

    NodeInfo(short nodeId, String dataCenter, Set<Short> replicaIds, int leaderId) {
        this(nodeId, dataCenter, replicaIds, (short) leaderId);
    }

    String toConsulValue() {
        return String.valueOf(nodeId) +
                SEPARATOR +
                dataCenter +
                SEPARATOR +
                StringUtils.join(replicaIds, SET_SEPARATOR) +
                SEPARATOR +
                leaderId;
    }

    @Override
    public String toString() {
        return toConsulValue();
    }
}
