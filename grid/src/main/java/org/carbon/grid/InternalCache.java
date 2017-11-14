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

/**
 * This interface mostly defines handler and dispatcher methods.
 */
interface InternalCache extends Cache {
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
}
