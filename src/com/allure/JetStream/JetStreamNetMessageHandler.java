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
import com.allure.JetStream.network.*;

public class JetStreamNetMessageHandler {

    protected Logger logger = null;
    protected JetStream jetstream = null;

    private JetStreamNetMessageHandler() {
    }

    public JetStreamNetMessageHandler(JetStream jetstream) {
        this.jetstream = jetstream;
        logger = jetstream.getLogger();
    }

    public void onHeartbeat(String hostName, String hostAddr, long serverStartTime) {
        //logger.trace();

        // Locate the referenced server
        JetStream.RemoteServer remote = jetstream.serversByAddress.get(hostAddr);

        if (remote == null) {
            StringBuffer sb = new StringBuffer("Heartbeat from server not in list:");
            sb.append("\n Name: ");
            sb.append(hostName);
            sb.append("\n Addr: ");
            sb.append(hostAddr);
            getLogger().info(sb.toString());
            return;
        }

        if (JetStream.fineLevelLogging) {
            StringBuffer sb = new StringBuffer("Heartbeat from server ");
            sb.append(hostName);
            getLogger().info(sb.toString());
        }

        // Store the server start time
        if (serverStartTime > 0) {
            remote.link.serverStartTime = serverStartTime;
        }

        // Update this server's last heartbeat time
        remote.link.lastHeartbeat = System.currentTimeMillis();
    }

    public void onTextMessage(String destName, JetStreamTextMessage textMsg,
            long netMsgId, int port, int localPort) {
        //logger.trace();
        onMessage(destName, textMsg, netMsgId, port, localPort);
    }

    public void onObjectMessage(String destName, JetStreamObjectMessage objMsg,
            long netMsgId, int port, int localPort) {
        //logger.fine("enter");
        onMessage(destName, objMsg, netMsgId, port, localPort);
    }

    public void onMapMessage(String destName, JetStreamMapMessage mapMsg,
            long netMsgId, int port, int localPort) {
        //logger.fine("enter");
        onMessage(destName, mapMsg, netMsgId, port, localPort);
    }

    public void onBytesMessage(String destName, JetStreamBytesMessage bytesMsg,
            long netMsgId, int port, int localPort) {
        //logger.fine("enter");
        onMessage(destName, bytesMsg, netMsgId, port, localPort);
    }

    public void onMessage(String destName, JetStreamMessage jsMsg,
            long netMsgId, int port, int localPort) {
        //logger.info("enter");
        try {
            byte messageType = jsMsg.getMessageType();

            switch (messageType) {
                case JetStreamMessage.PUB_SUB_MESSAGE:
                    if (JetStream.fineLevelLogging) {
                        getLogger().info("Pub/Sub message received");
                    }

                    JetStreamTopic jsTopic =
                            (JetStreamTopic) jetstream.createTopic(destName);

                    // If we are the server, send this net message to others
                    //
                    if (jetstream.isServer()) {
                        int sent = jetstream.netMsgSender.serverSendNetPubSubMsg(jsMsg, port, localPort);
                        if (sent > 0) {
                            JetStream.processTopic(); // Inform worker threads to send
                        }
                        if (jsTopic != null) {
                            jsTopic.saveDurableMessage(jsMsg);
                        }
                    }

                    // TODO: Try the local delivery first

                    // Have the topic's worker thread process the message internally
                    // This was intentionaly separated from localDispatch to
                    // ensure that the message was placed on the outbound dispatch
                    // queue for ALL remote clients first to avoid rentrancy

                    if (jsTopic != null) {
                        jsTopic.localNotify(jsMsg);
                    }

                    break;

                case JetStreamMessage.QUEUE_MESSAGE:
                    if (JetStream.fineLevelLogging) {
                        getLogger().info("Queue message received for " + destName);
                    }
                    boolean received = false;
                    JetStreamQueue jsQueue = null;
                    if (jetstream.isServer()) {
                        //
                        // Queue the message and make it appear to have originated
                        // from a local queue produer server
                        //
                        if (JetStream.fineLevelLogging) {
                            getLogger().info("Queue message received by server for dest: " + destName);
                        }

                        // If this message came from a remote client, then defer
                        // the queue response until we're sure we have a listener
                        // or not. If it's from a local or in-proc client, then
                        // send a response right away
                        //
                        if (jsMsg.remoteClient) {
                            if (JetStream.fineLevelLogging) {
                                getLogger().info("Deferring the QUEUE_RESPONSE for netMsgId=" + netMsgId + ", dest: " + destName);
                            }
                            jsMsg.sendDeferredQResponse = true;
                            jsMsg.deferredNetMsgId = netMsgId;
                            jsMsg.deferredPort = port;
                            jsMsg.deferredLocalPort = localPort;
                            jetstream.enQueueMessage(destName, jsMsg, netMsgId, port, localPort);
                            break;
                        }
                        else {
                            jetstream.enQueueMessage(destName, jsMsg, netMsgId, port, localPort);
                            received = true;
                        }

                    }
                    else {
                        if (jetstream._queues.containsKey(destName)) {
                            jsQueue =
                                    (JetStreamQueue) jetstream.createQueue(destName);
                            jsQueue.midFlight = true;
                            received = jsQueue.localNotify(jsMsg);
                        }

                        if (JetStream.fineLevelLogging) {
                            if (received) {
                                getLogger().info("Queue message received - delivered");
                            }
                            else {
                                getLogger().info("Queue message received - no local clients");
                            }
                        }
                    }

                    // Send back a message whether the message was received or not
                    //
                    jetstream.netMsgSender.sendNetQueueResponseMsg(
                            received, destName, netMsgId, port, localPort);

                    if (jsQueue != null) {
                        jsQueue.midFlight = false;
                    }

                    break;

                default:
                    getLogger().severe("Unidentified message type: " + messageType);
                    break;
            }
        }
        catch (Exception e) {
            getLogger().log(Level.SEVERE, "Exception:", e);
        }
    }
    public long prevNetMsgId;

    public void onQueueResponseMessage(String queueName, long netMsgId,
            boolean received,
            int clientPort, int clientLocalPort) {
        //this.endIntervalTimer(true);
        //logger.trace();
        if (JetStream.fineLevelLogging) {
            StringBuffer sb = new StringBuffer();
            sb.append("--> Received QueueResponse for ");
            sb.append(queueName);
            sb.append(", netMsgId=");
            sb.append(netMsgId);
            sb.append(", port=");
            sb.append(clientPort);
            sb.append(", localPort=");
            sb.append(clientLocalPort);
            if (received) {
                sb.append(", MSG RECEIVED");
            }
            else {
                sb.append(", MSG NOT RECEIVED");
            }
            getLogger().info(sb.toString());
        }

        try {
            JetStreamQueue queue =
                    (JetStreamQueue) jetstream.createQueue(queueName);

            // Remove this request from the outstanding timer queue
            QueueRequest qReq = queue.removeOutstandingQueueRequest(netMsgId);
            if (qReq == null) {
                StringBuffer sb =
                        new StringBuffer("QueueRequest object not found. Queue=");
                sb.append(queueName);
                sb.append(", netMsgId=");
                sb.append(netMsgId);
                sb.append(", prevNetMsgId=");
                sb.append(prevNetMsgId);
                sb.append(", clientPort=");
                sb.append(clientPort);
                getLogger().severe(sb.toString());
                getLogger().log(Level.SEVERE, "Informational stack trace:", new Exception("QueueRequest object not found"));
                prevNetMsgId = netMsgId;
                return;
            }

            prevNetMsgId = netMsgId;

            if (received) {
                onQueueMsgReceived(netMsgId, qReq, clientPort, clientLocalPort);
                return;
            }

            // The queue message was not processed by the consumer. If we are
            // the server, send it to the next client
            //
            if (jetstream.isServer()) {
                long newNetMsgId = jetstream.getNetMsgSeqNumber();
                qReq.netMsg.netMsgId = newNetMsgId;

                // Attempt delivery to the next client. If there are no more
                // then tell the queue it can continue sending other messages
                //
                boolean sent = jetstream.netMsgSender.sendToNextClient(qReq, qReq.netMsg.remoteClient);
                if (!sent) {
                    // No more receivers to receive this message
                    //
                    JetStreamMessage msg = qReq.queue.onMessageSent(false, "");

                    // Signal the queue so that the sender returns
                    // from the original call to 'send'
                    synchronized (queue) {
                        queue.setSuspended(false);
                        queue.notify();
                    }

                    // Check if a deferred queue response needs to be sent
                    jetstream.netMsgSender.checkToSendDeferredQResponse(msg, false);
                }
            }
        }
        catch (Exception e) {
            getLogger().log(Level.SEVERE, "Exception:", e);
        }
    }

    /*
     private void dumpQueueRequests(QueueRequest qReq) {
     synchronized ( this.outstandingSends ) {
     StringBuffer sb = new StringBuffer("outstandingSends keys:");
     //Enumeration<Long> keys = outstandingSends.keys();
     Iterator<Long> keys = outstandingSends.keySet().iterator();
     //while ( keys.hasMoreElements() ) {
     while ( keys.hasNext() ) {
     //Long key = keys.nextElement();
     Long key = keys.next();
     sb.append("\nkey:");
     sb.append(key);
     }
     logger.info(sb.toString());

     sb = new StringBuffer("outstandingSends values:");
     Collection<QueueRequest> values = outstandingSends.values();
     Iterator<QueueRequest> iter = values.iterator();
     while ( iter.hasNext() ) {
     QueueRequest val = iter.next();
     sb.append("\nvalue's netMsgId:");
     sb.append(val.netMsg.netMsgId);
     }
     logger.info(sb.toString());
     }
     }
     */
    private void onQueueMsgReceived(long netMsgId, QueueRequest qReq,
            int clientPort, int clientLocalPort) {
        JetStreamMessage msg =
                qReq.queue.onMessageSent(true, qReq.origMessageId);

        // Since we found a receiver for the message, optimize the queue
        // to send subsequent messages to this client first
        if (jetstream.isServer() && !qReq.queue.isOptimized()) {
            jetstream.optimizeQueueReceivers(
                    qReq.queue, clientPort, clientLocalPort);
            qReq.queue.setOptimized(true);
        }

        signalQueue(qReq.queue);

        // Check if a deferred queue response needs to be sent
        jetstream.netMsgSender.checkToSendDeferredQResponse(msg, true);
    }

    public void onNewQueueListenerMsg(String queueName) {
        // Since we have a new queue client, trigger the queue
        // to begin sending to clients
        //
        if (JetStream.fineLevelLogging) {
            getLogger().info("NEW_QUEUE_CLIENT_MSG recevied for queue: " + queueName);
        }

        try {
            JetStreamQueue q =
                    (JetStreamQueue) JetStream.getInstance().createQueue(queueName);
            q.skipOrigSender = false;
            JetStream.processQueue(q);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        if (jetstream.isServer()) {
            // Send a message to remote servers that this server
            // has a new queue listener
            if (jetstream.rcm != null) {
                jetstream.rcm.sendNewQueueListnerMsg(queueName);
            }
        }
    }

    public void onNewDurableSubMsg(JetStreamSocketClient client,
            String destname, String subname) {
        if (JetStream.fineLevelLogging) {
            StringBuffer sb = new StringBuffer("NEW_DURABLE_SUB_MSG recevied for topic: ");
            sb.append(destname);
            sb.append(", subscription name='");
            sb.append(subname);
            sb.append("'");
            getLogger().info(sb.toString());
        }

        JetStreamTopic topic = (JetStreamTopic) jetstream.createTopic(destname);
        topic.addDurableSubscriber(client, subname);
    }

    public void onRemoveDurableSubMsg(JetStreamSocketClient client,
            String destname, String subname) {
        if (JetStream.fineLevelLogging) {
            StringBuffer sb = new StringBuffer("DURABLE_SUB_REMOVE_MESSAGE recevied for topic: ");
            sb.append(destname);
            sb.append(", subscription name='");
            sb.append(subname);
            sb.append("'");
            getLogger().info(sb.toString());
        }

        JetStreamTopic topic = (JetStreamTopic) jetstream.createTopic(destname);
        topic.removeDurableSubscriber(client); // TODO: Need to add subname
    }

    public void onUnsubscribeMsg(JetStreamSocketClient client,
            String destname, String subname) {
        if (JetStream.fineLevelLogging) {
            StringBuffer sb = new StringBuffer("DURABLE_SUB_UNSUBSCRIBE_MESSAGE recevied for topic: ");
            sb.append(destname);
            sb.append(", subscription name='");
            sb.append(subname);
            sb.append("'");
            getLogger().info(sb.toString());
        }

        JetStreamTopic topic = (JetStreamTopic) jetstream.createTopic(destname);
        topic.unsubscribe(subname); // TODO: Possible add client to be sure
    }

    protected void signalQueue(JetStreamQueue queue) {
        // Signal the queue so that the sender returns
        // from the original call to 'send''
        synchronized (queue) {
            queue.setSuspended(false);
            queue.notify();
        }

        if (jetstream.isServer()) {
            if (!queue.isEmpty()) {
                JetStream.processQueue(queue);
            }
        }
    }

    private Logger getLogger() {
        if (logger == null) {
            logger = this.jetstream.getLogger();
        }
        return logger;
    }
}
