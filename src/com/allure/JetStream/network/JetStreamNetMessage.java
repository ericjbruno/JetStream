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
package com.allure.JetStream.network;

import com.allure.JetStream.*;
import java.io.*;
import javax.jms.*;

public class JetStreamNetMessage implements Serializable {

    private static final long serialVersionUID = 1L;
    // The types of messages
    public final static byte SERVER_HEARTBEAT_MESSAGE = 1;
    public final static byte PUB_SUB_MESSAGE = 10;
    public final static byte NEW_DURABLE_SUB_MESSAGE = 11;
    public final static byte DURABLE_SUB_REMOVE_MESSAGE = 12;
    public final static byte DURABLE_SUB_UNSUBSCRIBE_MESSAGE = 13;
    public final static byte QUEUE_MESSAGE = 20;
    public final static byte QUEUE_RESPONSE_MESSAGE = 21;
    public final static byte NEW_QUEUE_LISTENER_MESSAGE = 22;
    public final static byte HIGH_AVAIL_STORE_MESSAGE = 30;
    public final static byte HIGH_AVAIL_REMOVE_MESSAGE = 31;
    public final static byte HIGH_AVAIL_RECOVERY_MESSAGE = 32;
    public final static byte RECOVERED_PERSISTED_MESSAGES = 40;
    // The types of payloads the message can carry
    public final static byte STRING_OBJ = 1;
    public final static byte CUSTOM_OBJ = 2;
    public final static byte MAP_OBJ = 3;
    public final static byte BYTES_OBJ = 4;
    // Portions of a JMS message that must be delivered with the message
    //
    public byte type;
    public byte messagePayloadType;
    public String destName;
    //public String netMsgId = "";
    public long netMsgId = 0;
    public String messageId = "";
    public boolean messageReceived = true;
    public transient int clientPort;
    public transient int clientLocalPort;
    public boolean acked = false;
    public boolean remoteClient = false;
    public byte deliveryMode = DeliveryMode.PERSISTENT;
    public byte priority = Message.DEFAULT_PRIORITY;
    public long expiration = Message.DEFAULT_TIME_TO_LIVE;
    // Message payload (one or the other)
    public byte[] bytes = null;
    public String text = null;
    public CollectionContainer props = null;

    ////////////////////////////////////////////////////////////////////////
    public JetStreamNetMessage() {
    }

    public JetStreamNetMessage(String destName, int type, JetStreamMessage jsMsg)
            throws IOException, JMSException {
        this.destName = destName;
        this.type = (byte) type;
        this.acked = jsMsg.isAcked();
        this.deliveryMode = (byte) jsMsg.getJMSDeliveryMode();//deliveryMode;
        this.expiration = jsMsg.getJMSExpiration();//expiration;
        this.priority = (byte) jsMsg.getJMSPriority();//priority;
        this.clientPort = jsMsg.clientPort;
        this.clientLocalPort = jsMsg.clientLocalPort;
        this.messageId = jsMsg.getMsgId();//msgId;
        this.props = jsMsg.getProps();
        setRemoteClient(jsMsg.remoteClient);

        if (jsMsg != null) {
            // Determine the what type the message payload is
            //
            if (jsMsg instanceof JetStreamObjectMessage) {
                netObjectMessage(destName, type, (JetStreamObjectMessage) jsMsg);
            }
            else if (jsMsg instanceof JetStreamTextMessage) {
                netTextMessage(destName, type, (JetStreamTextMessage) jsMsg);
            }
            else if (jsMsg instanceof JetStreamMapMessage) {
                netMapMessage(destName, type, (JetStreamMapMessage) jsMsg);
            }
            else if (jsMsg instanceof JetStreamBytesMessage) {
                netBytesMessage(destName, type, (JetStreamBytesMessage) jsMsg);
            }
        }
    }

    public JetStreamNetMessage(String topic, int type, JetStreamObjectMessage jsObjMsg) throws IOException {
        netObjectMessage(topic, type, jsObjMsg);
    }

    public JetStreamNetMessage(String destName, int type, JetStreamTextMessage jsTextMsg) throws IOException, JMSException {
        netTextMessage(destName, type, jsTextMsg);
    }

    public JetStreamNetMessage(String destName, int type, int clientPort, String text) throws IOException, JMSException {
        netTextMessage(destName, type, clientPort, text);
    }

    public JetStreamNetMessage(String destName, int type, int clientPort) {
        netEmptyMessage(destName, type, clientPort);
    }

    public JetStreamNetMessage(String destName, String subName, int type, int clientPort) {
        netEmptyMessage(destName, subName, type, clientPort);
    }

    public JetStreamNetMessage(int type, int clientPort, String serverStartTime) throws IOException, JMSException {
        netEmptyMessage(serverStartTime, type, clientPort);
    }

    public void netEmptyMessage(String destName, int type, int clientPort) {
        this.messagePayloadType = STRING_OBJ;
        this.text = "";
        this.destName = destName;
        this.type = (byte) type;
        this.clientPort = clientPort;
        setType(type);
    }

    public void netEmptyMessage(String destName, String subName, int type, int clientPort) {
        this.messagePayloadType = STRING_OBJ;
        this.text = subName; // the durable subscriptio name
        this.destName = destName;
        this.type = (byte) type;
        this.clientPort = clientPort;
        setType(type);
    }

    public void netObjectMessage(String destName, int type, JetStreamObjectMessage jsObjMsg) throws IOException {
        this.destName = destName;
        this.type = (byte) type;
        this.messagePayloadType = CUSTOM_OBJ;
        this.clientPort = jsObjMsg.clientPort;

        if (jsObjMsg.obj != null) {
            // Serialize the object into an array of bytes. This must be
            // done because the application acting as the server may
            // not have any knowledge of this custom object. When the
            // server goes to re-distribute the object, it will get an
            // UnknownClassException exception. This problem is avoided 
            // by serializing the object into a byte array, and de-serializing
            // it just before delivery to the actual subscriber
            //
            ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
            ObjectOutputStream outStream = new ObjectOutputStream(outBytes);
            outStream.writeObject(jsObjMsg.obj);
            bytes = outBytes.toByteArray();
            outStream.close();
        }
        else {
            bytes = jsObjMsg.getBytes();
        }

        setType(type);
    }

    public void netBytesMessage(String destName, int type, JetStreamBytesMessage jsBytesMsg) throws IOException, JMSException {
        this.destName = destName;
        this.type = (byte) type;
        this.messagePayloadType = BYTES_OBJ;
        this.clientPort = jsBytesMsg.clientPort;
        bytes = jsBytesMsg.getData();
        setType(type);
    }

    public void netTextMessage(String destName, int type, JetStreamTextMessage jsTextMsg) throws IOException, JMSException {
        this.messagePayloadType = STRING_OBJ;
        this.text = jsTextMsg.getText();
        this.destName = destName;
        this.type = (byte) type;
        this.clientPort = jsTextMsg.clientPort;
        setType(type);
    }

    public void netTextMessage(String destName, int type, int clientPort, String text) throws IOException, JMSException {
        this.messagePayloadType = STRING_OBJ;
        this.text = text;
        this.destName = destName;
        this.type = (byte) type;
        this.clientPort = clientPort;
        setType(type);
    }

    public void netMapMessage(String destName, int type, JetStreamMapMessage jsMapMsg) throws IOException {
        this.destName = destName;
        this.type = (byte) type;
        this.messagePayloadType = MAP_OBJ;
        this.clientPort = jsMapMsg.clientPort;

        CollectionContainer data = jsMapMsg.getData();
        if (data != null) {
            // Serialize the object into an array of bytes. This must be
            // done because the application acting as the server may
            // not have any knowledge of this custom object. When the
            // server goes to re-distribute the object, it will get an
            // UnknownClassException exception. This problem is avoided
            // by serializing the object into a byte array, and de-serializing
            // it just before delivery to the actual subscriber
            //
            ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
            ObjectOutputStream outStream = new ObjectOutputStream(outBytes);
            outStream.writeObject(data);
            bytes = outBytes.toByteArray();
            outStream.close();
        }

        setType(type);
    }

    public void setClientPort(int port) {
        this.clientPort = port;
    }

    public void setClientLocalPort(int localPort) {
        this.clientLocalPort = localPort;
    }

    public void setRemoteClient(boolean remoteClient) {
        this.remoteClient = remoteClient;
    }

    public void setType(int type) {
        switch (type) {
            case PUB_SUB_MESSAGE:
            case NEW_DURABLE_SUB_MESSAGE:
            case DURABLE_SUB_REMOVE_MESSAGE:
            case DURABLE_SUB_UNSUBSCRIBE_MESSAGE:
            case QUEUE_MESSAGE:
            case QUEUE_RESPONSE_MESSAGE:
            case NEW_QUEUE_LISTENER_MESSAGE:
            case HIGH_AVAIL_STORE_MESSAGE:
            case HIGH_AVAIL_REMOVE_MESSAGE:
            case SERVER_HEARTBEAT_MESSAGE:
            case HIGH_AVAIL_RECOVERY_MESSAGE:
            case RECOVERED_PERSISTED_MESSAGES:
                this.type = (byte) type;
                break;

            default:
                this.type = PUB_SUB_MESSAGE;
        }
    }

    public int getType() {
        return type;
    }

    public String getTypeStr() {
        switch (type) {
            case PUB_SUB_MESSAGE:
                return "PUB_SUB_MESSAGE";
            case NEW_DURABLE_SUB_MESSAGE:
                return "NEW_DURABLE_SUB_MESSAGE";
            case DURABLE_SUB_REMOVE_MESSAGE:
                return "DURABLE_SUB_REMOVE_MESSAGE";
            case DURABLE_SUB_UNSUBSCRIBE_MESSAGE:
                return "DURABLE_SUB_UNSUBSCRIBE_MESSAGE";
            case QUEUE_MESSAGE:
                return "QUEUE_MESSAGE";
            case QUEUE_RESPONSE_MESSAGE:
                return "QUEUE_RESPONSE_MESSAGE";
            case NEW_QUEUE_LISTENER_MESSAGE:
                return "NEW_QUEUE_LISTENER_MESSAGE";
            case HIGH_AVAIL_STORE_MESSAGE:
                return "HIGH_AVAIL_STORAGE_MESSAGE";
            case HIGH_AVAIL_REMOVE_MESSAGE:
                return "HIGH_AVAIL_REMOVE_MESSAGE";
            case SERVER_HEARTBEAT_MESSAGE:
                return "SERVER_HEARTBEAT_MESSAGE";
            case HIGH_AVAIL_RECOVERY_MESSAGE:
                return "HIGH_AVAIL_RECOVERY_MESSAGE";
            default:
                return "UNKNOWN_MESSAGE";
        }
    }

    public void setId(long id) {
        this.netMsgId = id;
    }

    public void setReceived(boolean received) {
        this.messageReceived = received;
    }

    public int getClientPort() {
        return clientPort;
    }

    public int getClientLocalPort() {
        return clientLocalPort;
    }
}
