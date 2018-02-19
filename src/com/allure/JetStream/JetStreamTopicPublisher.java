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

public class JetStreamTopicPublisher extends JetStreamMessageProducer implements TopicPublisher {
    //private JetStreamLog logger = new JetStreamLog("JetStreamTopicPublisher");

    private JetStreamTopicPublisher() {
    }

    public JetStreamTopicPublisher(Topic topic) {
        destination = topic;
    }

    public static void _publish(Topic topic, Message message, int deliveryMode, int priority, long timeToLive) throws Exception {
        // Publishes a message to a topic for an unidentified message producer, 
        // specifying delivery mode, priority and time to live. 
        //
        JetStreamTopic jsTopic = (JetStreamTopic) topic;
        jsTopic.publish(message);
    }

    ///////////////////////////////////////////////////////////////////////////
    // MessageProducer
    @Override
    public void close() throws JMSException {
        if (JetStream.fineLevelLogging) {
            logger.info("JetStreamTopicPublisher - closing topic publisher");
        }
        super.close();
        destination = null;
    }

    ///////////////////////////////////////////////////////////////////////////
    // TopicPublisher
    public Topic getTopic() throws JMSException {
        // Gets the topic associated with this TopicPublisher. 
        return (Topic) destination;
    }

    public void publish(Message message) throws JMSException {
        super.send(destination, message);
    }

    public void publish(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        super.send(destination, message, deliveryMode, priority, timeToLive);
    }

    public void publish(Topic topic, Message message) throws JMSException {
        // Publishes a message to a topic for an unidentified message producer. 
        //
        super.send(topic, message);
    }

    public void publish(Topic topic, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        super.send(topic, message, deliveryMode, priority, timeToLive);
    }
}
