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
import java.util.logging.Logger;
import javax.jms.*;

public class JetStreamConnection implements Connection {

    protected boolean started = false;
    protected Logger logger = Logger.getLogger("JetStreamConnection");
    protected final Object lock = new Object();
    protected Vector sessions = new Vector();
    protected String clientId = "LJMS_EJB";
    protected ExceptionListener exceptListener = null;
    protected ConnectionMetaData metaData = null;

    public JetStreamConnection() {
        logger.setParent(JetStream.getInstance().getLogger());
    }

    public boolean isStarted() {
        synchronized (lock) {
            return started;
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Connection
    public Session createSession(boolean trans, int ackMode) throws JMSException {
        synchronized (lock) {
            //System.out.println("*** createSession called, " + trans +", "+ackMode);

            JetStreamSession session =
                    new JetStreamSession(this, trans, ackMode);

            // Store this session for this connection
            //
            sessions.addElement(session);

            return session;
        }
    }

    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        try {
            // Because we're using instanceof, the downcast is safe
            ///
            if (destination instanceof javax.jms.Queue) {
                return JetStreamQueueConnection._createConnectionConsumer(
                        (Queue) destination, messageSelector, sessionPool, maxMessages);
            }
            else if (destination instanceof Topic) {
                return JetStreamTopicConnection._createConnectionConsumer(
                        (Topic) destination, messageSelector, sessionPool, maxMessages);
            }
        }
        catch (Exception e) {
        }

        return null;
    }

    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        //Creates a connection consumer for this connection (optional operation).

        try {
            return JetStreamTopicConnection._createDurableConnectionConsumer(topic, subscriptionName, messageSelector, sessionPool, maxMessages);
        }
        catch (Exception e) {
        }

        return null;
    }

    public String getClientID() throws JMSException {
        synchronized (lock) {
            return clientId;
        }
    }

    public ExceptionListener getExceptionListener() throws JMSException {
        synchronized (lock) {
            return exceptListener;
        }
    }

    public ConnectionMetaData getMetaData() throws JMSException {
        synchronized (lock) {
            return metaData;
        }
    }

    public void setClientID(String clientId) throws JMSException {
        synchronized (lock) {
            this.clientId = clientId;
        }
    }

    public void setExceptionListener(ExceptionListener listener) throws JMSException {
        synchronized (lock) {
            this.exceptListener = listener;
        }
    }

    public void start() throws JMSException {
        JetStream jetstream = JetStream.getInstance();

        synchronized (lock) {
            //System.out.println("*** start called");

            started = true;
            for (int s = 0; s < sessions.size(); s++) {
                JetStreamSession session =
                        (JetStreamSession) sessions.elementAt(s);
                for (int c = 0; c < session.consumers.size(); c++) {
                    JetStreamMessageConsumer consumer =
                            (JetStreamMessageConsumer) session.consumers.elementAt(c);
                    if (consumer instanceof JetStreamQueueReceiver) {
                        JetStreamQueue q = (JetStreamQueue) consumer.destination;
                        jetstream.netMsgSender.sendNewQueueListenerMsg(q.name, -1);
                    }
                    else {
                        JetStreamTopic t =
                                (JetStreamTopic) consumer.destination;
                        JetStreamTopicSubscriber sub =
                                (JetStreamTopicSubscriber) consumer;

                        // Determine if we need to send a new subsriber message
                        if (sub.sendNewDurableMsg) {
                            if (JetStream.getInstance().isServer()) {
                                JetStreamTopic jstopic = (JetStreamTopic) t;
                                jstopic.addDurableSubscriber(
                                        sub, sub.subscriptionName);
                            }
                            else {
                                jetstream.netMsgSender.sendNewDurableSubMsg(
                                        t.getTopicName(), sub.subscriptionName);
                            }
                            sub.sendNewDurableMsg = false;
                        }
                    }
                }
            }
        }
    }

    public void stop() throws JMSException {
        synchronized (lock) {
            started = false;
        }
    }

    public void close() throws JMSException {
        synchronized (lock) {
            if (JetStream.fineLevelLogging) {
                logger.info("JetStreamConnection - closing connection");
            }

            started = false;

            // Stop all sessions for this connection
            //
            int cnt = sessions.size();
            for (int i = 0; i < cnt; i++) {
                JetStreamSession session =
                        (JetStreamSession) sessions.elementAt(i);

                try {
                    session.close();
                }
                catch (Exception e) {
                }
            }

            // Clear all connection specific data
            //
            sessions = new Vector();
            clientId = "LJMS_EJB";
            exceptListener = null;
            metaData = null;
        }
    }
}
