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

public class JetStreamMessageConsumer implements MessageConsumer {

    protected Logger logger = Logger.getLogger("JetStreamMessageConsumer");
    protected Destination destination;
    protected String messageSelector = null;
    protected MessageListener messageListener = null;
    protected Message message = null;
    protected boolean clientBlocking = false;
    protected Thread callingThread = null;

    public JetStreamMessageConsumer() {
        logger.setParent(JetStream.getInstance().getLogger());
    }

    public JetStreamMessageConsumer(Destination dest, String messageSelector) {
        this();
        destination = dest;
        this.messageSelector = messageSelector;
    }

    public synchronized boolean onMessage(Message message) throws Exception {
        if (messageListener != null) {
            // Asynchronous listener
            //
            if (JetStream.fineLevelLogging) {
                logger.info("JetStreamMessageConsumer: calling client onMessage()");
            }

            messageListener.onMessage(message);
            return true;
        }
        else if (clientBlocking) {
            // Synchronous blocking listener, store message and set flag
            //
            if (JetStream.fineLevelLogging) {
                logger.info("Storing message for queue " + destination);
            }

            if (this.message != null) {
                logger.info("Potential overwrite of prev msg");
            }

            callingThread = Thread.currentThread();
            this.message = message;

            try {
                // First, notify the blocking receiver thread
                //
                notifyAll();

                // Next, wait for the receiver to indicate it has the message
                try {
                    wait();
                }
                catch (Exception e) {
                }

                return true;
            }
            catch (Exception e) {
                logger.log(Level.SEVERE, "Exception", e);
            }
        }

        // No listener listing at this time, clear everything as listener
        // will request message in call to receive()
        this.message = null;
        this.callingThread = null;
        return false;
    }

    ///////////////////////////////////////////////////////////////////////////
    // MessageConsumer
    public void close() throws JMSException {
        // Closes the message consumer
        if (JetStream.fineLevelLogging) {
            logger.info("JetStreamQueueReceiver.close() - closing MessageConsumer");
        }
        ((JetStreamDestination) destination).removeConsumer(this);
        destination = null;
        messageListener = null;

    }

    public MessageListener getMessageListener() throws JMSException {
        // Gets the message consumer's MessageListener. 
        return messageListener;
    }

    public String getMessageSelector() throws JMSException {
        // Gets this message consumer's message selector expression. 
        return messageSelector;
    }

    public Message receive() throws JMSException {
        // Receives the next message produced for this message consumer. 
        return receive(-1);
    }

    public /*synchronized*/ Message receive(long timeout) throws JMSException {
        try {
            // Receives the next message that arrives within the specified timeout interval.
            //
            if (JetStream.fineLevelLogging) {
                StringBuffer sb = new StringBuffer();
                sb.append(this.toString());
                sb.append(" for destination ");
                sb.append(destination.toString());
                sb.append(" blocking...");
                logger.info(sb.toString());
            }

            while (true) {
                // If we're the server, check for, dequeue, and return the next
                // queued message
                //
                boolean tryAgain = false;
                if (JetStream.getInstance().isServer()) {
                    Message msg = getNextMessage(destination);
                    if (msg != null) {
                        message = null;
                        return msg;
                    }
                }
                //else
                {
                    // Send new q listener message and wait for next message
                    JetStreamDestination jsDest = (JetStreamDestination) destination;
                    if (jsDest.destType == -1) {
                        if (jsDest instanceof JetStreamQueue) {
                            jsDest.destType = JetStreamDestination.QUEUE;
                        }
                    }
                    if (jsDest.destType == JetStreamDestination.QUEUE) {
                        JetStreamQueue q = (JetStreamQueue) destination;
                        if (q.midFlight) {
                            tryAgain = true;
                        }
                        else {
                            // See if there's a way to optimize this safely
                            JetStream js = JetStream.getInstance();
                            js.netMsgSender.sendNewQueueListenerMsg(q.name, -1);
                        }
                    }
                }

                clientBlocking = true;

                // Go into blocking wait
                if (tryAgain) {
                    synchronized (this)  {
                        wait(1);
                    }
                }
                else {
                    if (timeout == -1) {
                        boolean spin = true;
                        while (spin) {
                            // There was nothing in the queue at the time
                            // of the call to receive, but under some timing
                            // conditions, a message may be enqueued, and
                            // notify called, between the check above, and
                            // the wait here, resulting in a hung client
                            // listener. Therefore, added a timeout to check
                            // for a non-empty queue every so often to not
                            // get stuck
                            synchronized (this)  {
                                wait(1000);
                            }
                            JetStreamQueue q = (JetStreamQueue) destination;
                            if (message != null || !q.isEmpty()) {
                                // There's a message waiting. Loop around
                                if (message == null) {
                                    message = q.getNextQueuedMessage();
                                }
                                spin = false;
                            }
                            else {
                                //System.out.println("Spinning for q " + q.getName());
                                JetStream js = JetStream.getInstance();
                                js.netMsgSender.sendNewQueueListenerMsg(q.name, -1);
                            }
                        }
                    }
                    else {
                        synchronized (this)  {
                            wait(timeout);
                        }
                    }
                }

                if (message == null) {
                    continue;
                }

                // Save message and null out global message var
                Message msg = message;
                message = null;
                synchronized (this)  {
                    notifyAll();
                }
                return msg;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            clientBlocking = false;
        }

        return null;
    }

    protected Message getNextMessage(Destination destination) {
        JetStreamDestination jsDest = (JetStreamDestination) destination;
        if (jsDest.destType == -1) {
            if (jsDest instanceof JetStreamQueue) {
                jsDest.destType = JetStreamDestination.QUEUE;
            }
            else {
                jsDest.destType = JetStreamDestination.TOPIC;
            }
        }

        switch (jsDest.destType) {
            case JetStreamDestination.QUEUE: {
                JetStreamQueue q = (JetStreamQueue) jsDest;
                synchronized (q) {
                    if (q.isSuspended()) {
                        return null;
                    }

                    if (q.isEmpty()) {
                        return null;
                    }
                    Message msg = q.getNextQueuedMessage();
                    q.setSuspended(true);
                    JetStreamMessage jsMsg = (JetStreamMessage) msg;
                    try {
                        q.onMessageSent(true, jsMsg.msgId);
                        q.setSuspended(false);
                        JetStream.getInstance().netMsgSender.
                                checkToSendDeferredQResponse(
                                jsMsg, true);
                    }
                    catch (Exception e) {
                    }

                    if (msg != null) {
                        return msg;
                    }
                }
            }
            break;

            case JetStreamDestination.TOPIC:
                // For topics, messages are delivered as they arrive
                // so there's really nothing waiting for a receiver
                break;
        }

        return null;
    }

    public synchronized Message receiveNoWait() throws JMSException {
        // Receives the next message if one is immediately available. 
        //
        //Message msg = message;
        //message = null;
        return receive(100);
    }

    public void setMessageListener(MessageListener listener) throws JMSException {
        messageListener = listener;

        if (!JetStream.getInstance().isServer()) {
            return;
        }

        if (destination instanceof JetStreamQueue) {
            JetStream.processQueue((JetStreamQueue) destination);
        }
        else {
            JetStreamTopicSubscriber sub = (JetStreamTopicSubscriber) this;
            if (sub.durable) {
                if (JetStream.getInstance().isServer()) {
                    JetStreamTopic jstopic = (JetStreamTopic) sub.topic;
                    jstopic.addDurableSubscriber(
                            sub, sub.subscriptionName);
                }
                else {
                    JetStream.getInstance().netMsgSender.sendNewDurableSubMsg(
                            sub.topic.getTopicName(), sub.subscriptionName);
                }
            }
        }
    }
}
