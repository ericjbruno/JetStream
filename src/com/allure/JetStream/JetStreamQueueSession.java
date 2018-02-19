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

public class JetStreamQueueSession extends JetStreamSession implements QueueSession {

    public JetStreamQueueSession(Connection conn, boolean transacted, int ackMode) {
        this.conn = conn;
        this.transacted = transacted;
        this.ackMode = ackMode;
    }

    public static QueueBrowser _createBrowser(Queue queue, String messageSelector) {
        // Creates a QueueBrowser object to peek at the messages on the specified queue using a message selector. 
        return new JetStreamQueueBrowser(queue, messageSelector);
    }

    public static Queue _createQueue(String queueName) throws JMSException {
        try {
            // Creates a queue identity given a Queue name
            return (Queue) JetStream.getInstance().createQueue(queueName);
        }
        catch (Exception e) {
            throw new JMSException(e.getMessage());
        }
    }

    public static TemporaryQueue _createTemporaryQueue() throws JMSException {
        try {
            // Creates a TemporaryQueue object.
            String tempQueueName = "LJMS_TEMP_QUEUE_" + System.currentTimeMillis();
            return new JetStreamTemporaryQueue(tempQueueName);
        }
        catch (Exception e) {
            throw new JMSException(e.getMessage());
        }
    }

    public static QueueReceiver _createReceiver(Queue queue, String messageSelector, Connection conn) throws JMSException {
        JetStreamQueue jsQueue = (JetStreamQueue) queue;
        JetStreamQueueReceiver rec =
                new JetStreamQueueReceiver(jsQueue, messageSelector);
        jsQueue.addReceiver(rec, conn);

        return rec;
    }

    public static QueueSender _createSender(Queue queue, Connection conn) throws JMSException {
        // Creates a QueueSender object to send messages to the specified queue. 
        //
        JetStreamQueue LJMSQueue = (JetStreamQueue) queue;
        JetStreamQueueSender sender = new JetStreamQueueSender(LJMSQueue);
        LJMSQueue.addSender(sender, conn);

        return sender;
    }

    ///////////////////////////////////////////////////////////////////////////
    // QueueSession
    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        return super.createBrowser(queue, null);
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException {
        return super.createBrowser(queue, messageSelector);
    }

    @Override
    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        return (QueueReceiver) super.createConsumer(queue);
    }

    @Override
    public QueueReceiver createReceiver(Queue queue, String messageSelector) throws JMSException {
        return (QueueReceiver) super.createConsumer(queue, messageSelector);
    }

    public QueueSender createSender(Queue queue) throws JMSException {
        return (QueueSender) super.createProducer(queue);
    }

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        return super.createTemporaryQueue();
    }

    @Override
    public Queue createQueue(String queueName) throws JMSException {
        return super.createQueue(queueName);
    }
}
