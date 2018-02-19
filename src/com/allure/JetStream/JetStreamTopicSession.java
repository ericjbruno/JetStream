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

import java.util.Collection;
import java.util.Iterator;
import javax.jms.*;

public class JetStreamTopicSession extends JetStreamSession implements TopicSession {

    public JetStreamTopicSession(Connection conn, boolean transacted, int ackMode) {
        this.conn = conn;
        this.transacted = transacted;
        this.ackMode = ackMode;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Static Class Methods
    public static TopicPublisher _createPublisher(Topic topic, Connection conn) throws JMSException {
        JetStreamTopicPublisher pub = null;

        //if ( topic != null )
        {
            // Creates a publisher for the specified topic
            //
            JetStreamTopic jsTopic = (JetStreamTopic) topic;
            pub = new JetStreamTopicPublisher(jsTopic);
            if (topic != null) {
                jsTopic.addPublisher(pub, conn);
            }
        }

        return pub;
    }

    public static TopicSubscriber _createSubscriber(Topic topic, Connection conn) throws JMSException {
        return _createSubscriber(topic, null, false, conn);
    }

    public static TopicSubscriber _createSubscriber(Topic topic, String messageSelector, boolean noLocal, Connection conn) throws JMSException {
        JetStreamTopicSubscriber sub = null;

        if (topic != null) {
            // Look for special "subscribe all" topic
            if ( topic.getTopicName().equals("***") ) {
                JetStream js = JetStream.getInstance();
                sub = new JetStreamTopicSubscriber(
                            (JetStreamTopic)topic,
                            messageSelector,
                            noLocal);
                js.storeMultiSubscriber(sub, (JetStreamConnection)conn);
                synchronized ( js._topics ) {
                    Iterator<JetStreamTopic> iter = 
                            js._topics.values().iterator();
                    while ( iter.hasNext() ) {
                        JetStreamTopic jsTopic = iter.next();
                        jsTopic.addSubscriber(sub, conn);
                    }
                }
            }
            else {
                // Creates a nondurable subscriber to the specified topic
                // using a message selector or specifying whether messages 
                // published by its own connection should be delivered to it
                //
                JetStreamTopic jsTopic = (JetStreamTopic)topic;

                sub = new JetStreamTopicSubscriber(
                            jsTopic,
                            messageSelector,
                            noLocal);

                jsTopic.addSubscriber(sub, conn);
            }
        }

        return sub;
    }
    
    public static TopicSubscriber _createDurableSubscriber(Topic topic,
            String name,
            Connection conn) throws JMSException {
        // Creates a durable subscriber to the specified topic.
        return _createDurableSubscriber(topic, name, null, false, conn);
    }

    public static TopicSubscriber _createDurableSubscriber(Topic topic,
            String subName,
            String messageSelector,
            boolean noLocal,
            Connection conn) throws JMSException {
        TopicSubscriber sub = null;

        if (topic != null) {
            // Creates a durable subscriber to the specified topic
            // using a message selector or specifying whether messages
            // published by its own connection should be delivered to it.
            //
            sub = _createSubscriber(topic, messageSelector, noLocal, conn);
            JetStreamTopicSubscriber jsSub = (JetStreamTopicSubscriber) sub;
            jsSub.setDurable(true);
            jsSub.setSubscriptionName(subName);

            // Make sure the connection is started before sending the
            // new dur sub notification. If not, then flag it so that
            // when the connection is started, we will send the notification
            // Otherwise, we will send the saved messages before the conn
            // is started and they will be ignored and lost
            //
            JetStreamConnection jsConn = (JetStreamConnection) conn;
            if (jsConn.isStarted()) {
                if (JetStream.getInstance().isServer()) {
                    JetStreamTopic jstopic = (JetStreamTopic) topic;
                    jstopic.addDurableSubscriber(jsSub, subName);
                }
                else {
                    JetStream.getInstance().netMsgSender.sendNewDurableSubMsg(topic.getTopicName(), subName);
                }
            }
            else {
                jsSub.sendNewDurableMsg = true;
            }
        }

        return sub;
    }

    public static TemporaryTopic _createTemporaryTopic() throws JMSException {
        // Creates a TemporaryTopic object
        String tempTopicName = "LJMS_TEMP_TOPIC_" + System.currentTimeMillis();
        return new JetStreamTemporaryTopic(tempTopicName);
    }

    public static Topic _createTopic(String topicName) throws JMSException {
        // Creates a topic identity given a Topic name
        //
        Topic topic = (Topic) JetStream.getInstance().createTopic(topicName);
        return topic;
    }

    ///////////////////////////////////////////////////////////////////////////
    // TopicSession
    public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException {
        return super.createDurableSubscriber(topic, name, null, false);
    }

    public TopicSubscriber createDurableSubscriber(Topic topic,
            String name,
            String messageSelector,
            boolean noLocal) throws JMSException {
        return super.createDurableSubscriber(topic, name, messageSelector, noLocal);
    }

    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        return (TopicPublisher) super.createProducer(topic);
    }

    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        return (TopicSubscriber) super.createConsumer(topic, null, false);
    }

    public TopicSubscriber createSubscriber(Topic topic, String messageSelector, boolean noLocal) throws JMSException {
        return (TopicSubscriber) super.createConsumer(topic, messageSelector, noLocal);
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        return super.createTemporaryTopic();
    }

    @Override
    public Topic createTopic(String topicName) throws JMSException {
        return super.createTopic(topicName);
    }

    @Override
    public void unsubscribe(String name) throws JMSException, InvalidDestinationException {
        super.unsubscribe(name);
    }
}
