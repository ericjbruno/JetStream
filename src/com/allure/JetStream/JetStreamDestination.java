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

import java.util.*;
import java.util.logging.Logger;
import javax.jms.*;

public class JetStreamDestination implements Destination {

    public static final int TOPIC = 1;
    public static final int QUEUE = 2;
    protected int destType = -1;
    protected Logger logger = JetStream.getInstance().getLogger();
    protected Vector<ProducerNode> producers = new Vector<ProducerNode>();
    protected Vector<ConsumerNode> consumers = new Vector<ConsumerNode>();
    protected String name = "";
    protected boolean deleted = false;

    public class ProducerNode {

        public MessageProducer _obj = null;
        public Connection _conn = null;

        public ProducerNode(MessageProducer sender, Connection conn) {
            _obj = sender;
            _conn = conn;
        }
    }

    public class ConsumerNode {

        public MessageConsumer _obj = null;
        public Connection _conn = null;

        public ConsumerNode(MessageConsumer consumer, Connection conn) {
            _obj = consumer;
            _conn = conn;
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Methods
    public Vector<ProducerNode> getProducers() throws JMSException {
        return producers;
    }

    public Vector<ConsumerNode> getConsumers() throws JMSException {
        return consumers;
    }

    public String getName() {
        return name;
    }

    public void addProducer(MessageProducer prod, Connection conn) throws JMSException {
        if (deleted) {
            throw new JMSException("Cannot use deleted topic");
        }

        ProducerNode prodNode = new ProducerNode(prod, conn);
        producers.add(prodNode);
    }

    public void addConsumer(MessageConsumer cons, Connection conn) throws JMSException {
        if (deleted) {
            throw new JMSException("Cannot use deleted topic");
        }

        if (JetStream.fineLevelLogging) {
            logger.info("JetStreamDestination.addConsumer()");
        }

        ConsumerNode consNode = new ConsumerNode(cons, conn);
        consumers.add(consNode);
    }

    public void removeProducer(MessageProducer producer) throws JMSException {
        int count = producers.size();
        for (int i = 0; i < count; i++) {
            ProducerNode prodNode = producers.elementAt(i);
            if (prodNode._obj == producer) {
                try {
                    if (JetStream.fineLevelLogging) {
                        logger.info("Removing producer for destination " + name);
                    }

                    producers.removeElementAt(i);

                    return;
                }
                catch (Exception e) {
                }
            }
        }
    }

    public void removeConsumer(MessageConsumer consumer) throws JMSException {
        JetStream jetstream = JetStream.getInstance();
        int count = consumers.size();
        for (int i = 0; i < count; i++) {
            ConsumerNode consNode = consumers.elementAt(i);
            if (consNode._obj == consumer) {
                try {
                    if (JetStream.fineLevelLogging) {
                        logger.info("Removing consumer for destination " + name);
                    }

                    consumers.removeElementAt(i);

                    // If the consumer that is being removed is a topic
                    // subscriber, check to see if it's a durable subscriber
                    //
                    if (consumer instanceof TopicSubscriber) {
                        if (jetstream.isServer()) {
                            JetStreamTopic topic = (JetStreamTopic) this;
                            topic.removeDurableSubscriber(
                                    (JetStreamTopicSubscriber) consumer);
                        }
                        else {
                            // Send a message to the server
                            jetstream.netMsgSender.sendRemoveDurableSubMsg(getName(), "");
                        }
                    }

                    return;
                }
                catch (Exception e) {
                }
            }
        }
    }
}
