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

import javax.jms.*;

public class DurableSubRecord {

    JetStreamSocketClient client = null; // for remote subscribers
    JetStreamTopicSubscriber consumer = null; // for local subscribers
    JetStreamTopic topic = null;
    String subscription = "";
    JetStreamQueue storage = null; // holds messages when receiver is offline
    JetStreamQueueReceiver receiver = null;
    boolean active = false; // if true, listener is listening

    private DurableSubRecord() {
    }

    public DurableSubRecord(JetStreamSocketClient client,
            JetStreamTopic topic,
            String subscription) {
        this.consumer = null;
        this.client = client;
        this.topic = topic;
        this.subscription = subscription;
        try {
            setupQueue();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public DurableSubRecord(JetStreamTopicSubscriber consumer,
            JetStreamTopic topic,
            String subscription) {
        this.client = null;
        this.consumer = consumer;
        this.topic = topic;
        this.subscription = subscription;
        try {
            setupQueue();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void setupQueue() throws Exception {
        // Create the queue for offline storage
        storage = (JetStreamQueue) JetStream.getInstance().
                createQueue(topic.getName() + "_" + subscription);
        storage.disableHA(); // No high-avail for this queue
        receiver = new JetStreamQueueReceiver(storage, null);
    }

    public void setClient(JetStreamSocketClient client) throws Exception {
        this.client = client;
        if (client != null) {
            setActive(true);
        }
    }

    public void setConsumer(JetStreamTopicSubscriber consumer) throws Exception {
        this.consumer = consumer;
        if (consumer != null) {
            setActive(true);
        }
    }

    public void setActive(boolean flag) throws Exception {
        if (this.active == flag) {
            return; // no change, no action
        }
        active = flag;
        if (flag) {
            if (client == null && consumer == null) {
                return;
            }

            // Deliver the exising queued messages
            if (!storage.isEmpty()) {
                // Manually dequeue the messages and publish them to client
                while (!storage.isEmpty()) {
                    JetStreamMessage msg = storage.getNextQueuedMessage();
                    if (msg == null) {
                        continue;
                    }
                    msg.destination = topic;
                    msg.destinationName = topic.name;
                    msg.messageType = JetStreamMessage.PUB_SUB_MESSAGE;
                    msg.durableName = subscription;
                    msg.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
                    int sent = 0;
                    if (client != null) {
                        sent = JetStream.getInstance().netMsgSender.sendNetPubSubMsg(msg, client);
                    }
                    else {
                        sent = JetStream.getInstance().netMsgSender.sendNetPubSubMsg(msg, consumer);
                    }

                    // If the message wasn't delivered, the receiver
                    // wasn't really there, or its MessageListener was null
                    // We'll try again when either of these are resolved
                    //
                    if (sent == 0) {
                        active = false;
                        break;
                    }

                    storage.onMessageSent(true, msg.getMsgId());
                    JetStream.processTopic();
                }
                JetStream.processTopic();
            }
        }
        else {
            // Remove the queue listener so that messages persist there
            if (receiver != null) {
                storage.removeConsumer(receiver);
            }
        }
    }

    public void unsubscribe() {
        // Drain the queue, then remove it
        while (!storage.isEmpty()) {
            JetStreamMessage msg = storage.getNextQueuedMessage();
            if (msg != null) {
                // This method dequeues the message, and removes it
                // from persistent storage
                storage.onMessageSent(true, msg.getMsgId());
            }
        }
    }
}
