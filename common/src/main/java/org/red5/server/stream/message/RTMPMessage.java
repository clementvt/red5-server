/*
 * RED5 Open Source Media Server - https://github.com/Red5/ Copyright 2006-2023 by respective authors (see below). All rights reserved. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless
 * required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

package org.red5.server.stream.message;

import org.red5.server.messaging.AbstractMessage;
import org.red5.server.net.rtmp.RTMPType;
import org.red5.server.net.rtmp.event.IRTMPEvent;
import org.red5.server.stream.IStreamData;
import org.red5.server.stream.PlayEngine;

/**
 * RTMP message
 *
 * @author mondain
 */
public class RTMPMessage extends AbstractMessage {

    private final IRTMPEvent body;

    /**
     * Creates a new rtmp message.
     *
     * @param body
     *            value to set for property 'body'
     */
    private RTMPMessage(IRTMPEvent body) {
        this.body = body;
        this.body.setSourceType(body.getSourceType());
        this.setMessageType(RTMPType.valueOf(body.getDataType()));
    }

    /**
     * Creates a new rtmp message.
     *
     * @param body
     *            value to set for property 'body'
     * @param eventTime
     *            updated timestamp
     */
    private RTMPMessage(IRTMPEvent body, int eventTime) {
        this.body = body;
        this.body.setTimestamp(eventTime);
        this.body.setSourceType(body.getSourceType());
        this.setMessageType(RTMPType.valueOf(body.getDataType()));
    }

    /**
     * Return RTMP message body
     *
     * @return Value for property 'body'.
     */
    public IRTMPEvent getBody() {
        return body;
    }

    /**
     * Builder for RTMPMessage.
     *
     * @param body
     *            event data
     * @return Immutable RTMPMessage
     */
    public static RTMPMessage build(IRTMPEvent body) {
        return new RTMPMessage(body);
    }

    /**
     * Builder for RTMPMessage.
     *
     * @param body
     *            event data
     * @param eventTime
     *            time value to set on the event body
     * @return Immutable RTMPMessage
     */
    public static RTMPMessage build(IRTMPEvent body, int eventTime) {
        return new RTMPMessage(body, eventTime);
    }

    public void pushMessage(PlayEngine engine) {
        RTMPMessage preparedMessage = this;
        IRTMPEvent body = preparedMessage.getBody();
        if (!(body instanceof IStreamData)) {
            engine.onInvalidStreamData(body);
            return;
        }
        if (engine.dropIfPaused(preparedMessage)) {
            return;
        }
        preparedMessage = engine.prepareStreamMessage(preparedMessage);
        if (preparedMessage == null) {
            return;
        }
        engine.sendMessage(preparedMessage);
    }
}
