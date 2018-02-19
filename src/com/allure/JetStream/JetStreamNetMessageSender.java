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
import java.util.*;
import java.io.*;
import javax.jms.*;
import com.allure.JetStream.network.*;

public class JetStreamNetMessageSender {

    protected Logger logger = null;
    protected JetStream jetstream = null;

    public JetStreamNetMessageSender(JetStream jetstream) {
        this.jetstream = jetstream;
        logger = jetstream.getLogger();
    }

    public int sendNetPubSubMsg(JetStreamMessage msg) throws IOException, JMSException {
        //logger.info("sendNetPubSubMsg");
        return sendNetPubSubMsg(msg, 0, 0, null, null);
    }

    public int sendNetPubSubMsg(JetStreamMessage msg, JetStreamSocketClient client)
            throws IOException, JMSException {
        //logger.info("sendNetPubSubMsg");
        return sendNetPubSubMsg(msg, 0, 0, client, null);
    }

    public int sendNetPubSubMsg(JetStreamMessage msg, JetStreamTopicSubscriber consumer)
            throws IOException, JMSException {
        //logger.info("sendNetPubSubMsg");
        return sendNetPubSubMsg(msg, 0, 0, null, consumer);
    }

    public int sendNetPubSubMsg(JetStreamMessage msg,
            int discludePort,
            int discludeLocalPort,
            JetStreamSocketClient client,
            JetStreamTopicSubscriber consumer) throws IOException, JMSException {
        if (JetStream.fineLevelLogging) {
            StringBuffer sb = new StringBuffer("Received pub/sub");
            sb.append(", txt=");
            sb.append(((TextMessage) msg).getText());
            sb.append(", id=");
            sb.append(msg.getMsgId());
            sb.append(", thread=");
            sb.append(Thread.currentThread());
            logger.info(sb.toString());
        }

        if (jetstream.isServer()) {
            // If client is not null, this message should be sent to them only
            if (client != null) {
                return serverSendNetPubSubMsg(msg, client);
            }
            else if (consumer != null) {
                return serverSendNetPubSubMsg(msg, consumer);
            }
            else {
                return serverSendNetPubSubMsg(msg, discludePort, discludeLocalPort);
            }
        }

        return clientSendNetPubSubMsg(msg, discludePort, discludeLocalPort);
    }

    public int clientSendNetPubSubMsg(JetStreamMessage msg,
            int discludePort,
            int discludeLocalPort) throws IOException, JMSException {
        // Now that we're outside the synchronized loop, dispatch the message
        JetStreamNetMessage netMsg =
                new JetStreamNetMessage(msg.getDestinationName(),
                JetStreamNetMessage.PUB_SUB_MESSAGE,
                msg);

        byte[] bytes = serializeNetObject(netMsg);

        // Walk the list of clients and send the message to each
        //
        int dispatchCount = 0;
        synchronized (jetstream._netClients) {
            for (JetStreamSocketClient client : jetstream._netClients) {
                if (client.port == discludePort
                        && client.localPort == discludeLocalPort) {
                    // Skip original sender
                    continue;
                }

                if (client.remoteClient && msg.remoteClient) {
                    // This message came from a remote server so
                    // do NOT propogate to any remote servers
                    continue;
                }

                // Make sure that an exception sending a message to one client
                // doesn't stop the propogation to the rest of them
                try {
                    boolean sent = client.sendNetMsg(bytes);
                    if (sent) {
                        dispatchCount++;
                    }
                    else {
                        logger.info("Detected failed client");
                    }
                }
                catch (Exception e) {
                    logger.log(Level.SEVERE, "Exception:", e);
                }
            }
        }
        return dispatchCount;
    }

    public int serverSendNetPubSubMsg(JetStreamMessage msg,
            JetStreamSocketClient client) throws IOException, JMSException {
        if (!jetstream.isServer()) {
            return 0;
        }

        // Dispatch the message to the given client only
        JetStreamNetMessage netMsg =
                new JetStreamNetMessage(msg.getDestinationName(),
                JetStreamNetMessage.PUB_SUB_MESSAGE,
                msg);

        byte[] bytes = serializeNetObject(netMsg);
        boolean throttle = jetstream.dispatchPubSubMessage(bytes, client);

        // If there are too many outstanding pub/sub requests
        // to be sent for this topic, we will need to wait until
        // the worker threads catch up. The throttle flag indicates
        // we should wait until notified to start sending again
        if (throttle) {
            jetstream.throttleSender();
        }

        return 1;
    }

    public int serverSendNetPubSubMsg(JetStreamMessage msg,
            JetStreamTopicSubscriber consumer) throws IOException, JMSException {
        if (jetstream.isServer()) {
            try {
                if (consumer.messageListener != null) {
                    consumer.onMessage(msg);
                    return 1;
                }
            }
            catch (Exception e) {
                logger.log(Level.INFO, "Client Exception:", e);
            }
        }

        return 0;
    }

    public int serverSendNetPubSubMsg(JetStreamMessage msg,
            int discludePort,
            int discludeLocalPort) throws IOException, JMSException {
        if (!jetstream.isServer()) {
            return 0;
        }

        // Walk the list of clients and send the message to each
        //
        int dispatchCount = 0;
        Vector<JetStreamSocketClient> psClients = new Vector<JetStreamSocketClient>();
        synchronized (jetstream._netClients) {
            for (JetStreamSocketClient client : jetstream._netClients) {
                if (client.port == discludePort
                        && client.localPort == discludeLocalPort) {
                    // Skip original sender
                    continue;
                }

                if (client.remoteClient && msg.remoteClient) {
                    // This message came from a remote server so
                    // do NOT propogate to any remote servers
                    continue;
                }

                // Store the client and send the message after this loop
                // to avoid synchronization issues
                psClients.add(client);
                dispatchCount++;
            }
        }

        if (psClients.isEmpty()) {
            return 0;
        }

        // Now that we're outside the synchronized loop, dispatch the message
        JetStreamNetMessage netMsg =
                new JetStreamNetMessage(msg.getDestinationName(),
                JetStreamNetMessage.PUB_SUB_MESSAGE,
                msg);

        byte[] bytes = serializeNetObject(netMsg);

        for (JetStreamSocketClient client : psClients) {
            boolean throttle = jetstream.dispatchPubSubMessage(bytes, client);
        }

        if (JetStream.fineLevelLogging) {
            logger.info("Done dispatching msg " + bytes);
        }

        return dispatchCount;
    }

    public int serverSendNetPubSubMsgExt(JetStreamMessage msg,
            int discludePort,
            int discludeLocalPort)
            throws IOException, JMSException {
        if (!jetstream.isServer()) {
            return 0;
        }

        // Walk the list of clients and send the message to each
        //
        int dispatchCount = 0;
        JetStreamNetMessage netMsg = null;
        byte[] bytes = null;

        //synchronized ( _netClients )
        {
            for (JetStreamSocketClient client : jetstream._netClients) {
                if (client.port == discludePort
                        && client.localPort == discludeLocalPort) {
                    // Skip original sender
                    continue;
                }

                if (client.remoteClient && msg.remoteClient) {
                    // This message came from a remote server so
                    // do NOT propogate to any remote servers
                    continue;
                }

                // Store the client and send the message after this loop
                // to avoid synchronization issues
                if (netMsg == null) {
                    netMsg =
                            new JetStreamNetMessage(msg.getDestinationName(),
                            JetStreamNetMessage.PUB_SUB_MESSAGE,
                            msg);

                    bytes = serializeNetObject(netMsg);
                }

                jetstream.dispatchPubSubMessage(bytes, client);

                dispatchCount++;
            }
        }

        if (JetStream.fineLevelLogging) {
            logger.info("Done dispatching msg");
        }

        return dispatchCount;
    }

    public boolean sendNetQueueMsg(
            JetStreamMessage msg, JetStreamQueue queue,
            String origMessageId,
            int discludePort, int discludeLocalPort) {
        if (JetStream.fineLevelLogging) {
            StringBuffer sb = new StringBuffer();
            sb.append(msg);
            sb.append(", Queue=");
            sb.append(queue);
            sb.append(", msgid=");
            sb.append(origMessageId);
            sb.append(", port=");
            sb.append(discludePort);
            sb.append(", localPort=");
            sb.append(discludeLocalPort);
            sb.append(", msg.destName=");
            sb.append(msg.getDestinationName());
            logger.info(sb.toString());
        }

        try {
            JetStreamNetMessage netMsg =
                    new JetStreamNetMessage(
                    msg.getDestinationName(),
                    JetStreamNetMessage.QUEUE_MESSAGE,
                    msg);

            netMsg.setId(jetstream.getNetMsgSeqNumber());
            netMsg.clientPort = msg.clientPort;
            netMsg.clientLocalPort = msg.clientLocalPort;

            // If queue is not null, then this is a call from the Queue
            // sender directly. Otherwise, it's a call from the server in
            // response to a queue message being sent to it to distribute
            //
            QueueRequest queueReq =
                    new QueueRequest(queue, netMsg, origMessageId,
                    discludePort, discludeLocalPort);


            boolean sent = sendToNextClient(queueReq, msg.remoteClient);
            if (sent) {
                return true;
            }

            // If we received a message from a remote server, but we
            // have no interested local queue listeners, then remove the
            // message. We'll get a special HA_STORE message to save this
            // in case the remote server fails
            if (msg.remoteClient) {
                queueReq.queue.onMessageSent(false, msg.msgId);
            }

            // Reached the end of the client list and there were no
            // receivers for this message. Check if a deferred queue
            // response needs to be sent
            checkToSendDeferredQResponse(msg, false);
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception:", e);
        }

        return false;
    }

    public void checkToSendDeferredQResponse(JetStreamMessage msg,
            boolean delivered) {
        //logger.trace();

        if (!jetstream.isServer()) {
            return;
        }

        if (msg == null) {
            return;
        }

        // Check if a deferred queue response needs to be sent
        if (msg.sendDeferredQResponse) {
            if (JetStream.fineLevelLogging) {
                logger.info("Sending deferred QUEUE_RESPONSE:" + msg.deferredNetMsgId);
            }

            sendNetQueueResponseMsg(delivered,
                    msg.getDestinationName(),
                    msg.deferredNetMsgId,
                    msg.deferredPort,
                    msg.deferredLocalPort);
        }
    }

    public void sendNetQueueResponseMsg( 
            boolean received, String destName, long netMsgId,
            int senderPort, int senderLocalPort) {
        if (JetStream.fineLevelLogging) {
            //logger.trace();
            StringBuffer sb = new StringBuffer();
            if (received) {
                sb.append("Message received, ");
            }
            else {
                sb.append("Message NOT received, ");
            }
            sb.append("netMsgId: ");
            sb.append(netMsgId);
            sb.append(", port: ");
            sb.append(senderPort);
            sb.append(", localPort: ");
            sb.append(senderLocalPort);
            sb.append(", dest: ");
            sb.append(destName);
            logger.info(sb.toString());
        }

        try {
            JetStreamNetMessage netMessage = new JetStreamNetMessage(
                    destName,
                    JetStreamNetMessage.QUEUE_RESPONSE_MESSAGE,
                    senderPort);

            netMessage.setId(netMsgId);
            netMessage.setReceived(received);

            int count = jetstream._netClients.size();
            for (int i = 0; i < count; i++) {
                try {
                    JetStreamSocketClient client =
                            (JetStreamSocketClient) jetstream._netClients.elementAt(i);

                    if (client.port == senderPort
                            && client.localPort == senderLocalPort) {
                        if (JetStream.fineLevelLogging) {
                            StringBuffer sb = new StringBuffer();
                            sb.append("<-- Sending QueueResponse, netMsgId=");
                            sb.append(netMessage.netMsgId);
                            sb.append(", remote=");
                            sb.append(client.remoteClient);
                            sb.append(", client: ");
                            sb.append(client.toString());
                            logger.info(sb.toString());
                        }

                        client.sendNetMsg(netMessage);
                        return; 
                    }
                }
                catch (Exception e) {
                }
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception:", e);
        }
    }

    public void sendNewQueueListenerMsg(String queueName, int discludePort) {
        //logger.trace();

        try {
            if (jetstream.isServer()) {
                // Send UDP message to remote server(s)
                if (jetstream.rcm != null) {
                    jetstream.rcm.sendNewQueueListnerMsg(queueName);
                }
            }
            else {
                // Send message to local server
                JetStreamNetMessage netMessage = new JetStreamNetMessage(
                        queueName,
                        JetStreamNetMessage.NEW_QUEUE_LISTENER_MESSAGE,
                        discludePort);

                synchronized (jetstream._netClients) {
                    for (JetStreamSocketClient client : jetstream._netClients) {
                        try {
                            if (client.port != discludePort) {
                                client.sendNetMsg(netMessage);
                                if (JetStream.fineLevelLogging) {
                                    logger.info("Sending NEW_QUEUE_LISTENER_MESSAGE for queue " + queueName);
                                }
                            }
                        }
                        catch (Exception e) {
                        }
                    }
                }
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception:", e);
        }
    }

    public void sendNewDurableSubMsg(String destname, String subName) {
        if (jetstream.isServer()) {
            return;
        }

        try {
            // Send message to local server
            JetStreamNetMessage netMessage = new JetStreamNetMessage(
                    destname,
                    subName,
                    JetStreamNetMessage.NEW_DURABLE_SUB_MESSAGE,
                    0);

            synchronized (jetstream._netClients) {
                for (JetStreamSocketClient client : jetstream._netClients) {
                    try {
                        // Send only to local clients other than ourselves
                        if (!client.remoteClient) {
                            client.sendNetMsg(netMessage);
                            if (JetStream.fineLevelLogging) {
                                logger.info("Sending NEW_DURABLE_SUB_MESSAGE for topic " + destname);
                            }
                        }
                    }
                    catch (Exception e) {
                    }
                }
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception:", e);
        }
    }

    public void sendRemoveDurableSubMsg(String destname, String subName) {
        if (jetstream.isServer()) {
            return;
        }

        try {
            // Send message to local server
            JetStreamNetMessage netMessage = new JetStreamNetMessage(
                    destname,
                    subName,
                    JetStreamNetMessage.DURABLE_SUB_REMOVE_MESSAGE,
                    0);

            synchronized (jetstream._netClients) {
                for (JetStreamSocketClient client : jetstream._netClients) {
                    try {
                        // Send only to local clients other than ourselves
                        if (!client.remoteClient) {
                            client.sendNetMsg(netMessage);
                            if (JetStream.fineLevelLogging) {
                                logger.info("Sending DURABLE_SUB_REMOVE_MESSAGE for topic " + destname);
                            }
                        }
                    }
                    catch (Exception e) {
                    }
                }
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception:", e);
        }
    }

    public void sendUnsubscribeMsg(String destname, String subName) {
        if (jetstream.isServer()) {
            return;
        }

        try {
            // Send message to local server
            JetStreamNetMessage netMessage = new JetStreamNetMessage(
                    destname,
                    subName,
                    JetStreamNetMessage.DURABLE_SUB_UNSUBSCRIBE_MESSAGE,
                    0);

            synchronized (jetstream._netClients) {
                for (JetStreamSocketClient client : jetstream._netClients) {
                    try {
                        // Send only to local clients other than ourselves
                        if (!client.remoteClient) {
                            client.sendNetMsg(netMessage);
                            if (JetStream.fineLevelLogging) {
                                logger.info("Sending DURABLE_SUB_UNSUBSCRIBE_MESSAGE for topic " + destname);
                            }
                        }
                    }
                    catch (Exception e) {
                    }
                }
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception:", e);
        }
    }

    public void sendHeartbeat(JetStreamSocketClient remote) {
        //logger.trace();

        if (JetStream.fineLevelLogging) {
            logger.info("Sending SERVER_HEARTBEAT_MESSAGE to " + remote.hostName);
        }

        try {
            JetStreamNetMessage heartbeat = new JetStreamNetMessage(
                    "" + jetstream.startTime,
                    JetStreamNetMessage.SERVER_HEARTBEAT_MESSAGE,
                    0); // 0 means don't disclude any servers

            remote.sendNetMsg(heartbeat);
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception:", e);
        }
    }

    protected boolean sendToNextClient(QueueRequest queueReq, boolean msgFromRemote) {
        //logger.trace();
        if (JetStream.fineLevelLogging) {
            //logger.fine("enter");
            StringBuffer sb = new StringBuffer();
            sb.append("JetStream: in sendToNextClient()");
            sb.append("\n    type=");
            sb.append(queueReq.netMsg.getTypeStr());
            sb.append("\n    name=");
            sb.append(queueReq.queue.name);
            sb.append("\n    messageId=");
            sb.append(queueReq.netMsg.messageId);
            sb.append("\n    netMsgId=");
            sb.append(queueReq.netMsg.netMsgId);
            sb.append("\n    client count = ");
            sb.append(jetstream._netClients.size());
            logger.info(sb.toString());
        }

        boolean sent = false;

        try {
            JetStreamQueue queue = queueReq.queue;

            if (queueReq.prevClient < queue.clientList.size()) {
                JetStreamSocketClient client =
                        queue.clientList.elementAt(queueReq.prevClient);

                // Make sure we don't send this message to the original sender
                //
                if (client.port == queueReq.clientPort
                        && client.localPort == queueReq.clientLocalPort) {
                    // Skip to the next client, if there is one
                    if (JetStream.fineLevelLogging) {
                        logger.info("Skipping original sender");
                    }

                    queueReq.prevClient++;
                    if (queueReq.prevClient < queue.clientList.size()) {
                        client = queue.clientList.elementAt(queueReq.prevClient);
                    }
                    else {
                        return false; // end of client list
                    }
                }

                // If the message arrived from a remote client (another
                // computer) then don't propagate it beyond local (on the
                // same computer) and in-proc clients
                //
                if (msgFromRemote) {
                    if (JetStream.fineLevelLogging) {
                        logger.info("SendToNextClient() - message from JetStream server");
                    }

                    while (true) {
                        if (!client.remoteClient) {
                            break;
                        }

                        // Skip to the next client, if there is one
                        if (JetStream.fineLevelLogging) {
                            logger.info("Skipping remote client");
                        }

                        queueReq.prevClient++;
                        if (queueReq.prevClient < queue.clientList.size()) {
                            client = queue.clientList.elementAt(queueReq.prevClient);
                        }
                        else {
                            return false; // end of client list
                        }
                    }
                }

                //
                // Send the network message
                //
                if (JetStream.fineLevelLogging) {
                    logger.info("    sending to a client");
                }

                client.lastSend = System.currentTimeMillis();

                queue.addOutstandingQueueRequest(queueReq, queueReq.netMsg.netMsgId);

                sent = client.sendNetMsg(queueReq.netMsg);
                if (sent) {
                    queueReq.prevClient++;
                }
                else {
                    // The message could not be sent to this client. Close
                    // what's left of the client connection and remove it
                    //
                    queue.removeOutstandingQueueRequest(queueReq.netMsg.netMsgId);
                    logger.info("Detected failed client - restarting queue");
                    jetstream.handleFailedClient(client);

                    queueReq.prevClient = 0;
                    return sendToNextClient(queueReq, msgFromRemote);
                }
            }
            else {
                if (JetStream.fineLevelLogging) {
                    logger.info("    No more clients to send to");
                }
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception:", e);
        }

        return sent;
    }

    public byte[] serializeNetObject(JetStreamNetMessage netMsg) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);

        dos.writeUTF(netMsg.destName);
        dos.writeByte(netMsg.type);
        dos.writeByte(netMsg.messagePayloadType);
        dos.writeLong(netMsg.netMsgId);
        dos.writeUTF(netMsg.messageId);
        dos.writeBoolean(netMsg.messageReceived);
        dos.writeInt(netMsg.clientPort);
        dos.writeInt(netMsg.clientLocalPort);
        dos.writeBoolean(netMsg.remoteClient);
        dos.writeByte(netMsg.deliveryMode);
        dos.writeByte(netMsg.priority);
        dos.writeLong(netMsg.expiration);
        if (netMsg.props != null) {
            ByteArrayOutputStream bstream = new ByteArrayOutputStream();
            ObjectOutputStream ostream = new ObjectOutputStream(bstream);
            ostream.writeObject(netMsg.props);
            ostream.flush();
            bstream.flush();
            byte[] bytes = bstream.toByteArray();
            bstream.reset();
            bstream.close();

            dos.writeLong(bytes.length);
            dos.write(bytes);
        }
        else {
            dos.writeLong(0);
        }
        if (netMsg.messagePayloadType == JetStreamNetMessage.STRING_OBJ) {
            //dos.writeUTF(netMsg.text);
            writeString(dos, netMsg.text);
        }
        else {
            if (netMsg.bytes != null) {
                dos.writeInt(netMsg.bytes.length);
                dos.write(netMsg.bytes);
            }
            else {
                dos.writeInt(0);
            }
        }
        dos.flush();
        byte[] bytes = bos.toByteArray();
        return bytes;
    }

    public JetStreamNetMessage deserializeNetObject(byte[] bytes) throws Exception {
        if (JetStream.fineLevelLogging) {
            logger.info("DESERIALIZE");
        }

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);

        try {
            if (JetStream.fineLevelLogging) {
                logger.info("--BYTES AVAILABLE: " + bis.available());
            }
            JetStreamNetMessage netMsg = new JetStreamNetMessage();
            netMsg.destName = dis.readUTF();
            netMsg.type = dis.readByte();
            netMsg.messagePayloadType = dis.readByte();
            netMsg.netMsgId = dis.readLong();
            netMsg.messageId = dis.readUTF();
            netMsg.messageReceived = dis.readBoolean();
            netMsg.clientPort = dis.readInt();
            netMsg.clientLocalPort = dis.readInt();
            netMsg.remoteClient = dis.readBoolean();
            netMsg.deliveryMode = dis.readByte();
            netMsg.priority = dis.readByte();
            netMsg.expiration = dis.readLong();
            long propsLen = dis.readLong();
            if (propsLen > 0) {
                byte[] propBytes = new byte[(int) propsLen];
                dis.read(propBytes);
                ByteArrayInputStream bstream = new ByteArrayInputStream(propBytes);
                ObjectInputStream ostream = new ObjectInputStream(bstream);
                netMsg.props = (CollectionContainer) ostream.readObject();
                ostream.close();
            }
            if (netMsg.messagePayloadType == JetStreamNetMessage.STRING_OBJ) //netMsg.text = dis.readUTF();
            {
                netMsg.text = readString(dis);
            }
            else {
                int sz = dis.readInt();
                if (sz > 0) {
                    netMsg.bytes = new byte[sz];
                    int read = dis.read(netMsg.bytes);
                }
                else {
                    netMsg.bytes = null;
                }
            }

            return netMsg;
        }
        catch (EOFException e) {
            logger.log(Level.SEVERE, "Exception:", e);
        }

        return null;
    }

    private void writeString(DataOutputStream dos, String str) throws IOException {
        int length = str.length();

        dos.writeInt(length);
        if (length > 0) {
            dos.writeChars(str);
        }
    }

    private String readString(DataInputStream dis) throws IOException {
        int res = dis.readInt();

        // No data to retrieve.  Used to signal terminating connection.
        if (res == 0) {
            return "";
        }

        StringBuffer buf = new StringBuffer(res);
        for (int i = 0; i < res; i++) {
            buf.append(dis.readChar());
        }

        String result = buf.toString();
        return result;
    }
}
