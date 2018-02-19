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

public class JetStreamQueueSender extends JetStreamMessageProducer implements QueueSender {

    public JetStreamQueueSender() {
    }

    public JetStreamQueueSender(Queue queue) {
        destination = queue;
    }

    public static void _send(Queue queue, Message message, int deliveryMode,
            int priority, long timeToLive) throws Exception {
        // Sends a message to a queue for an unidentified message producer, 
        // specifying delivery mode, priority and time to live. 
        //
        JetStreamQueue jsQueue = (JetStreamQueue) queue;
        if (jsQueue != null) {
            jsQueue.send(message);
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // MessageProducer
    @Override
    public void close() throws JMSException {
        if (JetStream.fineLevelLogging) {
            logger.info("JetStreamQueueSender - closing queue sender");
        }
        super.close();
        destination = null;
    }

    ///////////////////////////////////////////////////////////////////////////
    // QueueSender
    public Queue getQueue() throws JMSException {
        // Gets the queue associated with this QueueSender. 
        return (Queue) destination;
    }

    @Override
    public void send(Message message) throws JMSException {
        // Sends a message to the queue
        //
        super.send(destination, message);
    }

    @Override
    public void send(Message message, int deliveryMode,
            int priority, long timeToLive) throws JMSException {
        // Sends a message to the queue, specifying delivery mode, 
        // priority, and time to live
        //
        super.send(destination, message, deliveryMode, priority, timeToLive);
    }

    public void send(Queue queue, Message message) throws JMSException {
        // Sends a message to a queue for an unidentified message producer. 
        super.send(queue, message);
    }

    public void send(Queue queue, Message message, int deliveryMode,
            int priority, long timeToLive) throws JMSException {
        // Sends a message to a queue for an unidentified message producer, 
        // specifying delivery mode, priority and time to live. 
        //
        super.send(queue, message, deliveryMode, priority, timeToLive);
    }
}
