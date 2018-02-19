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

import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.*;

public class JetStreamMessageProducer implements MessageProducer {

    protected Logger logger = Logger.getLogger("JetStreamMessageProducer");
    protected int deliveryMode = DeliveryMode.NON_PERSISTENT;
    protected int priority = Message.DEFAULT_PRIORITY;
    protected long timeToLive = Message.DEFAULT_TIME_TO_LIVE;
    protected boolean disableMessageID = true;
    protected boolean disableMessageTimestamp = true;
    protected Destination destination = null;

    public JetStreamMessageProducer() {
        logger.setParent(JetStream.getInstance().getLogger());
    }

    public JetStreamMessageProducer(Destination destination) {
        this();
        this.destination = destination;
    }

    ///////////////////////////////////////////////////////////////////////////
    // MessageProducer
    public void send(Message message) throws JMSException {
        // Sends a message using the MessageProducer's default delivery mode, priority, and time to live. 
        this.send(destination,
                message,
                message.getJMSDeliveryMode(),
                message.getJMSPriority(),
                message.getJMSExpiration());
    }

    public void send(Destination dest, Message message) throws JMSException {
        // Sends a message to a destination for an unidentified message producer. 
        this.send(dest,
                message,
                message.getJMSDeliveryMode(),
                message.getJMSPriority(),
                message.getJMSExpiration());
    }

    public void send(Message message, int deliveryMode, int priority, long TTL) throws JMSException {
        // Sends a message to the destination, specifying delivery mode, priority, and time to live.
        this.send(destination, message, deliveryMode, priority, TTL);
    }

    public void send(Destination dest, Message message, int deliveryMode, int priority, long TTL) throws JMSException {
        try {
            //
            // Set the message header fields
            //
            message.setJMSExpiration(TTL);
            message.setJMSPriority(priority);
            message.setJMSDeliveryMode(deliveryMode);
            if (!disableMessageTimestamp) {
                message.setJMSTimestamp(System.currentTimeMillis());
            }
            //if ( ! disableMessageID )
            //    message.setJMSMessageID( JetStream.getNextMessageIDstr() );

            // Make sure that IO based messages are reset so that the
            // underlying IO Streams are closed and ready to be read
            //
            /*
             if ( message instanceof BytesMessage )
             ((BytesMessage)message).reset();
             else if ( message instanceof StreamMessage )
             ((StreamMessage)message).reset();
             */

            // Sends a message to a destination for an unidentified message producer, specifying delivery mode, priority and time to live. 
            if (dest instanceof Queue) {
                if (JetStream.fineLevelLogging) {
                    logger.info("JetStreamMessageProducer.send() - send message to queue");
                }
                Queue queue = (Queue) dest;
                if (queue != null) {
                    JetStreamQueueSender._send(queue, message, deliveryMode, priority, TTL);
                }
                else if (JetStream.fineLevelLogging) {
                    logger.info("JetStreamMessageProducer.send() - queue is null");
                }
            }
            else if (dest instanceof Topic) {
                if (JetStream.fineLevelLogging) {
                    logger.info("JetStreamMessageProducer.send() - send message to topic");
                }
                Topic topic = (Topic) dest;
                if (topic != null) {
                    JetStreamTopicPublisher._publish(topic, message, deliveryMode, priority, TTL);
                }
                else if (JetStream.fineLevelLogging) {
                    logger.info("JetStreamMessageProducer.send() - topic is null");
                }
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "JetStreamMessageProducer.send() Exception:", e);
        }
    }

    public Destination getDestination() throws JMSException {
        //Gets the destination associated with this Messag 
        return destination;
    }

    public int getDeliveryMode() throws JMSException {
        // Gets the producer's default delivery mode. 
        return deliveryMode;
    }

    public boolean getDisableMessageID() throws JMSException {
        // Gets an indication of whether message IDs are disabled. 
        return disableMessageID;
    }

    public boolean getDisableMessageTimestamp() throws JMSException {
        // Gets an indication of whether message timestamps are disabled. 
        return disableMessageTimestamp;
    }

    public int getPriority() throws JMSException {
        // Gets the producer's default priority. 
        return priority;
    }

    public long getTimeToLive() throws JMSException {
        // Gets the default length of time in milliseconds from its dispatch 
        // time that a produced message should be retained by the message system
        return timeToLive;
    }

    public void setDeliveryMode(int deliveryMode) throws JMSException {
        // Sets the producer's default delivery mode. 
        this.deliveryMode = deliveryMode;
    }

    public void setDisableMessageID(boolean value) throws JMSException {
        // Sets whether message IDs are disabled. 
        this.disableMessageID = value;
    }

    public void setDisableMessageTimestamp(boolean value) throws JMSException {
        // Sets whether message timestamps are disabled. 
        this.disableMessageTimestamp = value;
    }

    public void setPriority(int priority) throws JMSException {
        // Sets the producer's default priority. 
        this.priority = priority;
    }

    public void setTimeToLive(long timeToLive) throws JMSException {
        // Sets the default length of time in milliseconds from its dispatch 
        // time that a produced message should be retained by the message system. 
        this.timeToLive = timeToLive;
    }
    JetStreamSession session = null;

    public void setSession(JetStreamSession session) {
        this.session = session;
    }

    public void close() throws JMSException {
        // Closes the message producer. 
        if (JetStream.fineLevelLogging) {
            logger.info("JetStreamMessageProducer - closing MessageProducer");
        }

        if (destination != null) {
            ((JetStreamDestination) destination).removeProducer(this);
            destination = null;
        }

        if (session != null) {
            session.producers.remove(this);
            session = null;
        }
    }
}
