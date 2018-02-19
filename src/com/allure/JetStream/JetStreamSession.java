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

import java.util.Vector;
import java.util.HashMap;
import java.util.logging.Logger;
import javax.jms.*;

public class JetStreamSession implements Session {

    protected Logger logger = Logger.getLogger("JetStreamSession");
    protected boolean active = true;
    protected Vector producers = new Vector();
    protected Vector consumers = new Vector();
    protected int ackMode = AUTO_ACKNOWLEDGE;
    protected boolean transacted = false;
    protected boolean noLocal = false;
    protected Connection conn = null;
    // Store references to the topic objects with durable subscriptions
    //
    private HashMap durableSubscriptions = new HashMap();

    public JetStreamSession() {
        logger.setParent(JetStream.getInstance().getLogger());
    }

    public JetStreamSession(Connection conn, boolean transacted, int ackMode) {
        this();
        this.conn = conn;

        if (ackMode == CLIENT_ACKNOWLEDGE) {
            this.ackMode = CLIENT_ACKNOWLEDGE;
        }
        else {
            this.ackMode = AUTO_ACKNOWLEDGE;
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Session
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        //Creates a MessageConsumer for the specified destination. 
        return createConsumer(destination, null, false);
    }

    public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
        //Creates a MessageConsumer for the specified destination, using a message selector. 
        return createConsumer(destination, messageSelector, false);
    }

    public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) throws JMSException {
        if (JetStream.fineLevelLogging) {
            logger.info("JetStreamSession.createConsumer() called");
        }

        if (destination == null) {
            logger.info("createConsumer() - destination is null");
            return null;
        }

        if (!active) {
            logger.info("createConsumer() - Session is closed");
            return null;
        }

        this.noLocal = noLocal;

        //Creates MessageConsumer for the specified destination, using a message selector. 
        MessageConsumer consumer = null;
        if (destination instanceof Queue) {
            consumer = JetStreamQueueSession._createReceiver(
                    (Queue) destination, messageSelector, conn);
        }
        else if (destination instanceof Topic) {
            consumer = JetStreamTopicSession._createSubscriber(
                    (Topic) destination, messageSelector, noLocal, conn);
        }

        consumers.addElement(consumer);

        return consumer;
    }

    public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException {
        return this.createDurableSubscriber(topic, name, null, false);
    }

    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
        //Creates a durable subscriber to the specified topic.
        TopicSubscriber topicSub = JetStreamTopicSession._createDurableSubscriber(topic, name, null, false, conn);

        JetStreamTopic jsTopic = (JetStreamTopic) topic;

        durableSubscriptions.put(name, jsTopic);

        consumers.addElement(topicSub);

        return topicSub;
    }

    public MessageProducer createProducer(Destination destination) throws JMSException {
//        if ( destination == null ) {
//            logger.info("createProducer() - destination is null");
//            return null;
//        }

        if (!active) {
            logger.info("createProducer() - Session is closed");
            return null;
        }

        //Creates a MessageProducer to send messages to the specified destination. 
        MessageProducer producer = null;
        if (destination == null) {
            producer = JetStreamTopicSession._createPublisher(
                    null, conn);
        }
        else if (destination instanceof Queue) {
            producer = JetStreamQueueSession._createSender(
                    (Queue) destination, conn);
        }
        else if (destination instanceof Topic) {
            producer = JetStreamTopicSession._createPublisher(
                    (Topic) destination, conn);
        }

        producers.addElement(producer);
        ((JetStreamMessageProducer) producer).setSession(this);

        return producer;
    }

    public Queue createQueue(String queueName) throws JMSException {
        //Creates a queue identity given a Queue name. 
        //System.out.println("*** createQueue called, " + queueName);
        return JetStreamQueueSession._createQueue(queueName);
    }

    public Topic createTopic(String topicName) throws JMSException {
        //System.out.println("*** createTopic called, " + topicName);

        //Creates a topic identity given a Topic name. 
        return JetStreamTopicSession._createTopic(topicName);
    }

    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        // Creates a QueueBrowser object to peek at the messages on the specified queue. 
        return JetStreamQueueSession._createBrowser(queue, null);
    }

    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException {
        // Creates a QueueBrowser object to peek at the messages on the specified queue using a message selector. 
        return JetStreamQueueSession._createBrowser(queue, messageSelector);
    }

    public TemporaryQueue createTemporaryQueue() throws JMSException {
        // Creates a TemporaryQueue object. 
        return JetStreamQueueSession._createTemporaryQueue();
    }

    public TemporaryTopic createTemporaryTopic() throws JMSException {
        // Creates a TemporaryTopic object. 
        return JetStreamTopicSession._createTemporaryTopic();
    }

    public void commit() throws JMSException {
        // Commits all messages done in this transaction and releases any locks currently held. 
    }

    public BytesMessage createBytesMessage() throws JMSException {
        // Creates a BytesMessage object.
        JetStreamBytesMessage msg = new JetStreamBytesMessage();
        if (this.ackMode == AUTO_ACKNOWLEDGE) {
            msg.acknowledge();
        }

        return msg;
    }

    public MapMessage createMapMessage() throws JMSException {
        // Creates a MapMessage object.
        JetStreamMapMessage msg = new JetStreamMapMessage();
        if (this.ackMode == AUTO_ACKNOWLEDGE) {
            msg.acknowledge();
        }

        return msg;
    }

    public Message createMessage() throws JMSException {
        // Creates a Message object.
        JetStreamMessage msg = new JetStreamMessage();
        if (this.ackMode == AUTO_ACKNOWLEDGE) {
            msg.acknowledge();
        }

        return msg;
    }

    public ObjectMessage createObjectMessage() throws JMSException {
        // Creates an ObjectMessage object. 
        JetStreamObjectMessage msg = new JetStreamObjectMessage();
        if (this.ackMode == AUTO_ACKNOWLEDGE) {
            msg.acknowledge();
        }

        return msg;
    }

    public ObjectMessage createObjectMessage(java.io.Serializable obj) throws JMSException {
        // Creates an initialized ObjectMessage object. 
        JetStreamObjectMessage msg = new JetStreamObjectMessage(obj);
        if (this.ackMode == AUTO_ACKNOWLEDGE) {
            msg.acknowledge();
        }

        return msg;
    }

    public StreamMessage createStreamMessage() throws JMSException {
        // Creates a StreamMessage object. 
        JetStreamStreamMessage msg = new JetStreamStreamMessage();
        if (this.ackMode == AUTO_ACKNOWLEDGE) {
            msg.acknowledge();
        }

        return msg;
    }

    public TextMessage createTextMessage() throws JMSException {
        // Creates a TextMessage object. 
        JetStreamTextMessage msg = new JetStreamTextMessage();
        if (this.ackMode == AUTO_ACKNOWLEDGE) {
            msg.acknowledge();
        }

        return msg;
    }

    public TextMessage createTextMessage(String text) throws JMSException {
        JetStreamTextMessage msg = new JetStreamTextMessage(text);
        if (this.ackMode == AUTO_ACKNOWLEDGE) {
            msg.acknowledge();
        }

        return msg;
    }

    public MessageListener getMessageListener() throws JMSException {
        // Returns the session's distinguished message listener (optional). 
        return null;
    }

    public boolean getTransacted() throws JMSException {
        // Indicates whether the session is in transacted mode. 
        return transacted;
    }

    public int getAcknowledgeMode() throws JMSException {
        // Returns the acknowledgement mode of the session. 
        // The acknowledgement mode is set at the time that the session is created. 
        // If the session is transacted, the acknowledgement mode is ignored
        return ackMode;
    }

    public void recover() throws JMSException {
        // Stops message delivery in this session, and restarts message delivery with the oldest unacknowledged message. 
    }

    public void rollback() throws JMSException {
        // Rolls back any messages done in this transaction and releases any locks currently held. 
    }

    public void run() {
        // Optional operation, intended to be used only by Application Servers, not by ordinary JMS clients. 
    }

    public void setMessageListener(MessageListener listener) throws JMSException {
        // Sets the session's distinguished message listener (optional). 
        // Not implemented in Lightweight JMS Provider
    }

    public void unsubscribe(String subname) throws JMSException, InvalidDestinationException {
        // Unsubscribes a durable subscription that has been created by a client.
        //
        if (JetStream.fineLevelLogging) {
            logger.info("unsubscribe() called with name " + subname);
        }

        JetStreamTopic topic = (JetStreamTopic) durableSubscriptions.get(subname);
        /*
         if ( topic == null )
         throw new InvalidDestinationException("Invalid subscription name");
         */

        // Iteratate through all subscribers and look for this one by name
        //
        boolean foundSubscriber = false;
        int cnt = consumers.size();
        for (int i = 0; i < cnt; i++) {
            MessageConsumer cons =
                    (MessageConsumer) consumers.elementAt(i);

            if (cons instanceof JetStreamTopicSubscriber) {
                JetStreamTopicSubscriber sub = (JetStreamTopicSubscriber) cons;
                if (subname.equals(sub.getSubscriptionName())) {
                    if (JetStream.fineLevelLogging) {
                        logger.info("Setting subscriber as NON-durable");
                    }

                    foundSubscriber = true;
                    sub.setDurable(false);
                    sub.setSubscriptionName("");
                    durableSubscriptions.remove(subname);
                }
            }
        }

        if (foundSubscriber) {
            if (JetStream.getInstance().isServer()) {
                topic.unsubscribe(subname);
            }
            else {
                JetStream.getInstance().netMsgSender.sendUnsubscribeMsg(topic.getName(), subname);
            }
        }
        else {
            logger.info("No durable subscriber found, or in wrong state");
        }
    }

    public void close() throws JMSException {
        if (JetStream.fineLevelLogging) {
            logger.info("JetStreamSession.close() - closing session");
        }

        // Close the session
        //
        active = false;

        int cnt = producers.size();
        if (JetStream.fineLevelLogging) {
            logger.info("JetStreamSession.close() - producers: " + cnt);
        }
        for (int i = 0; i < cnt; i++) {
            MessageProducer prod =
                    (MessageProducer) producers.elementAt(i);

            try {
                prod.close();
            }
            catch (Exception e) {
            }
        }

        cnt = consumers.size();

        if (JetStream.fineLevelLogging) {
            logger.info("JetStreamSession.close() - consumers: " + cnt);
        }

        for (int i = 0; i < cnt; i++) {
            MessageConsumer cons =
                    (MessageConsumer) consumers.elementAt(i);

            try {
                cons.close();
            }
            catch (Exception e) {
            }
        }

        producers = new Vector();
        consumers = new Vector();
        conn = null;
    }
}
