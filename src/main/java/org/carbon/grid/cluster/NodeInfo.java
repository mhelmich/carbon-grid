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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Set;

@JsonSerialize(using = NodeInfo.NodeInfoSerializer.class)
@JsonDeserialize(using = NodeInfo.NodeInfoDeserializer.class)
class NodeInfo implements Serializable {
    final short nodeId;
    final String dataCenter;
    final Set<Short> replicaIds;
    final Set<Short> leaderIds;

    NodeInfo(short nodeId, String dataCenter, List<Short> replicaIds, List<Short> leaderIds) {
        this(nodeId, dataCenter, ImmutableSet.copyOf(replicaIds), ImmutableSet.copyOf(leaderIds));
    }

    NodeInfo(short nodeId, String dataCenter, Set<Short> replicaIds, Set<Short> leaderIds) {
        this.nodeId = nodeId;
        this.dataCenter = dataCenter;
        this.replicaIds = replicaIds;
        this.leaderIds = leaderIds;
    }

    @Override
    public String toString() {
        return "nodeId: " + nodeId + " dataCenter: " + dataCenter + " leaderIds: " + leaderIds + " replicaIds: " + replicaIds;
    }

    static class NodeInfoSerializer extends StdSerializer<NodeInfo> {
        NodeInfoSerializer() {
            super(NodeInfo.class);
        }

        @Override
        public void serialize(NodeInfo nodeInfo, JsonGenerator jgen, SerializerProvider serializerProvider) throws IOException {
            jgen.writeStartObject();
            jgen.writeNumberField("nodeId", nodeInfo.nodeId);
            jgen.writeStringField("dataCenter", nodeInfo.dataCenter);
            jgen.writeArrayFieldStart("leaderIds");
            for (Short leaderId : nodeInfo.leaderIds) {
                jgen.writeNumber(leaderId);
            }
            jgen.writeEndArray();
            jgen.writeArrayFieldStart("replicaIds");
            for (Short replicaId : nodeInfo.replicaIds) {
                jgen.writeNumber(replicaId);
            }
            jgen.writeEndArray();
            jgen.writeEndObject();
        }
    }

    static class NodeInfoDeserializer extends StdDeserializer<NodeInfo> {
        NodeInfoDeserializer() {
            super(NodeInfo.class);
        }

        @Override
        public NodeInfo deserialize(JsonParser jp, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            JsonNode jn = jp.getCodec().readTree(jp);
            short nodeId = (short) jn.get("nodeId").asInt();
            String dataCenter = jn.get("dataCenter").asText();

            ArrayNode leaderIds = (ArrayNode) jn.get("leaderIds");
            ImmutableSet.Builder<Short> leaderBuilder = ImmutableSet.builder();
            for (int i = 0; i < leaderIds.size(); i++) {
                short id = (short) leaderIds.get(i).asInt();
                leaderBuilder.add(id);
            }

            ArrayNode replicaIds = (ArrayNode) jn.get("replicaIds");
            ImmutableSet.Builder<Short> replicaBuilder = ImmutableSet.builder();
            for (int i = 0; i < replicaIds.size(); i++) {
                short id = (short) replicaIds.get(i).asInt();
                replicaBuilder.add(id);
            }

            return new NodeInfo(nodeId, dataCenter, replicaBuilder.build(), leaderBuilder.build());
        }
    }
}