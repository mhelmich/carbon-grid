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

import java.net.InetSocketAddress;

/**
 * This interface mostly defines handler and dispatcher methods.
 */
public interface InternalCache extends Cache {
    /**
     * This method is being called by all messages that are received by a client.
     */
    void handleResponse(Message.Response response);

    /**
     * This method is called for any incoming message (aka each message that is received by the server handler).
     * The request is taken off the wire, deserialized into a POJO, and passed to this method.
     * When null is returned nothing will be sent in response!
     */
    Message.Response handleRequest(Message.Request request);

    /**
     * This method is called whenever the health status of nodes in the cluster changes.
     * The responsibilities of this method include to diff the existing state with the state
     * that is delivered.
     * One way of dealing with this is that stash all nodes in a map nodeId -> addr. This way
     * the new state overrides the old and everything's good.
     */
    void handlePeerChange(short nodeId, InetSocketAddress addr);
}
