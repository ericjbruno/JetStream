/*
 * This file is part of JetStreamQ.
 *    
 * Copyright 2010-2013 Allure Technology, Inc. All Rights Reserved.
 * www.alluretechnology.com
 *
 * JetStreamQ is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 2 of the License.
 *
 * JetStreamQ is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with JetStreamQ. If not, see <http://www.gnu.org/licenses/>.
 */
package com.allure.JetStream;

import com.allure.JetStream.network.JetStreamNetMessage;

public class QueueRequest {

    public JetStreamQueue queue;
    public JetStreamNetMessage netMsg;
    public int prevClient;
    public String origMessageId;
    public int clientPort;
    public int clientLocalPort;
    public int sendCountdownTick;

    private QueueRequest() {
    }

    public QueueRequest(JetStreamQueue queue, JetStreamNetMessage netMsg) {
        this.queue = queue;
        this.netMsg = netMsg;
        this.prevClient = 0;
        this.origMessageId = "";
        this.clientPort = 0;
        this.clientLocalPort = 0;
        this.sendCountdownTick = 4; // two seconds
    }

    public QueueRequest(JetStreamQueue queue, JetStreamNetMessage netMsg,
            String origMessageId, int clientPort, int clientLocalPort) {
        this.queue = queue;
        this.netMsg = netMsg;
        this.prevClient = 0;
        this.origMessageId = origMessageId;
        this.clientPort = clientPort;
        this.clientLocalPort = clientLocalPort;
        this.sendCountdownTick = 4; // two seconds
    }
}
