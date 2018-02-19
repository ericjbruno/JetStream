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

public class JetStreamTopicSubscriber extends JetStreamMessageConsumer implements TopicSubscriber {

    protected Topic topic = null;
    protected boolean noLocal = false;
    protected boolean durable = true;
    protected String subscriptionName = "";
    protected boolean sendNewDurableMsg = false;

    private JetStreamTopicSubscriber() {
    }

    public JetStreamTopicSubscriber(Topic topic) {
        this(topic, null, false);
    }

    public JetStreamTopicSubscriber(Topic topic, String messageSelector, boolean noLocal) {
        this.topic = topic;

        if (JetStream.fineLevelLogging) {
            StringBuffer sb = new StringBuffer();
            sb.append(this.toString());
            sb.append(" created for topic ");
            sb.append(topic);
            logger.info(sb.toString());
        }

        this.messageSelector = messageSelector;

        this.noLocal = noLocal;

        this.destination = topic;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public String getSubscriptionName() {
        return subscriptionName;
    }

    public void setSubscriptionName(String name) {
        subscriptionName = name;
    }

    //@Override
    public boolean onMessage(Message message) throws Exception {
        if (JetStream.fineLevelLogging) {
            logger.info("JetStreamTopicSubscriber.onMessage() called, deferring to base class");
        }

        if (skipDurSend(message)) {
            return false;
        }

        return super.onMessage(message);
    }

    public boolean skipDurSend(Message message) {
        /*
         //logger.info("skipDurSend");
         JetStreamMessage jsMsg = (JetStreamMessage)message;
         if ( jsMsg.durableName != null && ! jsMsg.durableName.isEmpty() ) {
         if ( ! jsMsg.durableName.equals(this.subscriptionName) )
         return true;
         }
         */

        return false;
    }

    ///////////////////////////////////////////////////////////////////////////
    // MessageConsumer
    @Override
    public void close() throws JMSException {
        // Closes the message consumer. 
        if (JetStream.fineLevelLogging) {
            logger.info("JetStreamTopicSubscriber - closing topic subscriber");
        }
        super.close();
        topic = null;
        this.messageListener = null;
        this.messageSelector = null;
    }

    ///////////////////////////////////////////////////////////////////////////
    // TopicSubscriber
    public boolean getNoLocal() throws JMSException {
        return noLocal;
    }

    public Topic getTopic() throws JMSException {
        return topic;
    }
}
