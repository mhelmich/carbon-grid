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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CacheImpl implements Cache {
    private final static Logger logger = LoggerFactory.getLogger(CacheImpl.class);

    @Override
    public void handleResponse(Message.Response response) {
        switch (response.type) {
            case ACK:
                handleACK(response);
                return;
            default:
                throw new RuntimeException("Unknown type " + response.type);
        }
    }

    @Override
    public Message.Response handleRequest(Message.Request request) {
        switch (request.type) {
            case GET:
                return handleGET(request);
            default:
                throw new RuntimeException("Unknown type " + request.type);
        }
    }

    private Message.Response handleGET(Message.Request request) {
        logger.info("cache handler get: {}", request);
        return new Message.ACK(request);
    }

    private void handleACK(Message.Response response) {
        logger.info("cache handler ack: {}", response);
    }
}
