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

public class JetStreamTopicConnection extends JetStreamConnection implements TopicConnection {

    public JetStreamTopicConnection() {
    }

    public static ConnectionConsumer _createConnectionConsumer(Topic topic,
            String messageSelector,
            ServerSessionPool sessionPool,
            int maxMessages) throws JMSException {

        return null;
    }

    public static ConnectionConsumer _createDurableConnectionConsumer(Topic topic,
            String subscriptionName,
            String messageSelector,
            ServerSessionPool sessionPool,
            int maxMessages) throws JMSException {

        return null;
    }

    ///////////////////////////////////////////////////////////////////////////
    // TopicConnection
    public TopicSession createTopicSession(boolean trans, int ackMode) throws JMSException {
        synchronized (lock) {
            JetStreamTopicSession topicSession =
                    new JetStreamTopicSession(this, trans, ackMode);

            // Store this session for this connection
            //
            sessions.addElement(topicSession);

            return topicSession;
        }
    }

    public ConnectionConsumer createConnectionConsumer(Topic topic,
            String messageSelector,
            ServerSessionPool sessionPool,
            int maxMessages) throws JMSException {
        return super.createConnectionConsumer(topic, messageSelector, sessionPool, maxMessages);
    }
}
