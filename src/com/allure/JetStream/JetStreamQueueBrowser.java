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

import java.util.Enumeration;
import javax.jms.*;

public class JetStreamQueueBrowser implements QueueBrowser {

    private Queue queue = null;
    private String messageSelector = "";
    private Enumeration enumer = null;

    public JetStreamQueueBrowser(Queue queue) {
        this.queue = queue;
    }

    public JetStreamQueueBrowser(Queue queue, String messageSelector) {
        this.queue = queue;
        this.messageSelector = messageSelector;
    }

    public void setQueue(Queue queue) {
        this.queue = queue;
    }

    ///////////////////////////////////////////////////////////////////////////
    // QueueBrowser
    public void close() throws JMSException {
        // Closes the QueueBrowser. 
        queue = null;
        enumer = null;
        messageSelector = "";
    }

    public Enumeration getEnumeration() throws JMSException {
        // Gets an enumeration for browsing the current queue messages in the order they would be received. 
        JetStreamQueue jsQueue = (JetStreamQueue) queue;
        enumer = jsQueue.getMessages();
        return enumer;
    }

    public String getMessageSelector() throws JMSException {
        // Gets this queue browser's message selector expression. 
        return messageSelector;
    }

    public Queue getQueue() throws JMSException {
        // Gets the queue associated with this queue browser. 
        return queue;
    }
}
