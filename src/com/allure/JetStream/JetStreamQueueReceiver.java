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

import javax.jms.*;

public class JetStreamQueueReceiver extends JetStreamMessageConsumer implements QueueReceiver {

    public JetStreamQueueReceiver() {
    }

    public JetStreamQueueReceiver(Queue queue, String messageSelector) {
        this.messageSelector = messageSelector;
        this.destination = queue;
    }

    //@Override
    public boolean onMessage(Message message) throws Exception {
        if (JetStream.fineLevelLogging) {
            logger.info("JetStreamQueueReceiver.onMessage() called, deferring to base class");
        }
        return super.onMessage(message);
    }

    ///////////////////////////////////////////////////////////////////////////
    // MessageConsumer
    @Override
    public void close() throws JMSException {
        // Closes the message consumer. 
        if (JetStream.fineLevelLogging) {
            logger.info("JetStreamQueueReceiver.close() - closing queue receiver");
        }
        super.close();
        messageListener = null;
        destination = null;

    }

    ///////////////////////////////////////////////////////////////////////////
    // QueueReceiver
    public Queue getQueue() throws JMSException {
        // Gets the Queue associated with this queue receiver 
        return (Queue) destination;
    }
}
