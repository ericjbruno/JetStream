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

import com.allure.JetStream.network.JetStreamSocketClient;
import java.util.*;
import java.io.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import javax.jms.*;

public class JetStreamTopic extends JetStreamDestination implements Topic {

    protected HashMap durableSubscriptions = new HashMap();
    protected JetStream jetStream;
    private ConcurrentHashMap<String, DurableSubRecord> durableSubs =
            new ConcurrentHashMap<String, DurableSubRecord>();

    ///////////////////////////////////////////////////////////////////////////
    // Methods
    protected JetStreamTopic() {
        this.destType = JetStreamDestination.TOPIC;
        jetStream = JetStream.getInstance();
    }

    public JetStreamTopic(String topicName) {
        this();
        name = topicName;
    }

    public void addPublisher(TopicPublisher pub, Connection conn) throws JMSException {
        //logger.trace();
        super.addProducer(pub, conn);
        if (JetStream.fineLevelLogging) {
            logger.info("JetStreamTopic: publisher added for topic " + name);
        }
    }

    public void addSubscriber(TopicSubscriber sub, Connection conn) throws JMSException {
        //logger.trace();
        super.addConsumer(sub, conn);
        if (JetStream.fineLevelLogging) {
            logger.info("JetStreamTopic: subscriber added for topic " + name);
        }
    }

    public void addDurableSubscriber(JetStreamSocketClient client, String subname) {
        //logger.trace();
        if (deleted) {
            logger.severe("Topic is deleted and cannot be used");
            return;
        }

        if (!JetStream.getInstance().isServer()) {
            logger.severe("We're not the server");
            return;
        }

        try {
            // First, check to see if this is a durable subscriber coming back online
            DurableSubRecord sub = durableSubs.get(subname);
            if (sub != null) {
                // Remap to existing record and deliver saved messages
                sub.setClient(client);
                return;
            }

            // Add a new durable subscription record
            sub = new DurableSubRecord(client, this, subname);
            sub.setActive(true); // subscriber is listening (active)
            durableSubs.put(subname, sub);
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
        }
    }

    public void addDurableSubscriber(JetStreamTopicSubscriber consumer, String subname) {
        //logger.trace();
        if (deleted) {
            logger.severe("Topic is deleted and cannot be used");
            return;
        }

        if (!JetStream.getInstance().isServer()) {
            logger.severe("We're not the server");
            return;
        }

        try {
            // First, check to see if this is a durable subscriber coming back online
            DurableSubRecord sub = durableSubs.get(subname);
            if (sub != null) {
                // Remap to existing record and deliver saved messages
                sub.setConsumer(consumer);
                return;
            }

            // Add a new durable subscription record
            sub = new DurableSubRecord(consumer, this, subname);
            sub.setActive(true); // subscriber is listening (active)
            durableSubs.put(subname, sub);
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
        }
    }

    public boolean removeDurableSubscriber(JetStreamSocketClient client) {
        //logger.trace();

        // Iterate through the durable subscriber list to see if this
        // subscriber is on it. If so, we must start saving messages for it
        //
        for (DurableSubRecord sub : durableSubs.values()) {
            try {
                if (sub.client == client) {
                    sub.setClient(null);
                    sub.setActive(false); // client no longer listening
                    return true;
                }
            }
            catch (Exception e) {
                logger.log(Level.SEVERE, "Exception", e);
            }
        }

        return false;
    }

    public boolean removeDurableSubscriber(JetStreamTopicSubscriber consumer) {
        //logger.trace();

        // Iterate through the durable subscriber list to see if this
        // subscriber is on it. If so, we must start saving messages for it
        //
        for (DurableSubRecord sub : durableSubs.values()) {
            try {
                if (sub.consumer == consumer) {
                    sub.setConsumer(null);
                    sub.setActive(false); // client no longer listening
                    return true;
                }
            }
            catch (Exception e) {
                logger.log(Level.SEVERE, "Exception", e);
            }
        }

        return false;
    }

    public void unsubscribe(String name) {
        //logger.trace();
        if (JetStream.fineLevelLogging) {
            logger.info("unsubscribe from durable topic: " + name);
        }

        DurableSubRecord sub = durableSubs.get(name);
        if (sub != null) {
            durableSubs.remove(name);
            sub.unsubscribe();
        }
    }

    public void publish(Message message) throws IOException, JMSException {
        //logger.trace();

        if (deleted) {
            throw new JMSException("Cannot use deleted topic");
        }

        if (message == null) {
            if (JetStream.fineLevelLogging) {
                logger.info("JetStreamTopic:publish() message object is null");
            }
            return;
        }

        try {
            JetStreamMessage jsMessage = (JetStreamMessage) message;
            jsMessage.setDestinationName(name);
            jsMessage.setMessageType(JetStreamMessage.PUB_SUB_MESSAGE);
            jsMessage.setJMSDestination(this);
            jsMessage.setSent(true);

            // Notify local clients first
            localNotify(jsMessage);

            // Next, notify network clients
            jetStream.netMsgSender.sendNetPubSubMsg(jsMessage);
            if (jetStream.isServer()) {
                JetStream.processTopic();
            }

            // Save for durable subscribers
            //
            saveDurableMessage(jsMessage);
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
        }
    }

    public int numConsumers() {
        if (consumers != null) {
            return consumers.size();
        }
        return 0;
    }

    public void localNotify(Message message) {
        //logger.trace();
        if (consumers != null && consumers.size() > 0) {
            try {
                // Notify local clients via a pooled worker thread
                notifySubscribers(message);
            }
            catch (Exception e) {
            }
        }
    }

    // TODO: Remove synchronized but make it thread safe internally
    protected synchronized void notifySubscribers(Message message) {
        //logger.info("notifySubscribers");
        try {
            // Iterate all of the subscribers for this topic. For each one
            // whose Connection is started send this message
            //
            for (ConsumerNode consumer : consumers) {
                message.setJMSRedelivered(false);
                JetStreamConnection conn = (JetStreamConnection) consumer._conn;
                if (conn.isStarted()) {
                    // Wrap the call to each subscriber in a try-catch so that an
                    // exception in one subscriber will not stop us from sending the
                    // message to the other subscribers
                    //
                    try {
                        JetStreamTopicSubscriber sub =
                                (JetStreamTopicSubscriber) consumer._obj;
                        sub.onMessage(message);
                    }
                    catch (Exception e) {
                        //message.setJMSRedelivered( true );
                        if (JetStream.fineLevelLogging) {
                            logger.log(Level.INFO, "Client Exception", e);
                        }
                    }
                }
                else {
                    //if ( JetStream.fineLevelLogging )
                    logger.info("Skipping - connection not started");
                }
            }
        }
        catch (JMSException je) {
            logger.log(Level.INFO, "JMSException: ", je);
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
        }
    }

    protected void saveDurableMessage(Message message) {
        if (JetStream.fineLevelLogging) {
            logger.info("saveDurableMessage called");
        }

        if (durableSubs.isEmpty()) {
            return;
        }

        // Save the message for future delivery to durable subscribers
        for (DurableSubRecord sub : durableSubs.values()) {
            try {
                if (!sub.active) {
                    message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
                    sub.storage.internalSend(message, "", 0, 0, false);
                }
            }
            catch (Exception e) {
                logger.log(Level.SEVERE, "Exception saving durable message:", e);
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Topic interface
    public String getTopicName() throws JMSException {
        return name;
    }
}
