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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

import javax.jms.*;

import com.allure.JetStream.network.JetStreamNetMessage;

public class JetStreamMessage implements Message {

    private static final long serialVersionUID = 1L;
    // Message types
    public final static byte PUB_SUB_MESSAGE = JetStreamNetMessage.PUB_SUB_MESSAGE;
    public final static byte QUEUE_MESSAGE = JetStreamNetMessage.QUEUE_MESSAGE;
    // Message priorities
    public final static byte NORMAL_PRIORITY = 4;
    // Message states
    public final static byte PENDING_STATE = 1;   // Not yet sent
    public final static byte SENT_STATE = 2;      // Sent, not yet delivered
    public final static byte DELIVERED_STATE = 3; // Sent and delivered
    protected String destinationName = null;
    protected String correlationID = null;
    protected String id = null;
    protected String type = null;
    protected Destination destination = null;
    protected Destination replyTo = null;
    protected byte messageType = PUB_SUB_MESSAGE;
    protected byte deliveryMode = DeliveryMode.NON_PERSISTENT;
    protected byte priority = NORMAL_PRIORITY;
    protected byte state = PENDING_STATE;
    protected long expiration = 0;
    protected long tstamp = 0;
    protected String msgId = "";
    protected String durableName = null;
    protected boolean redelivered = false;
    protected CollectionContainer props = null;
    protected transient JetStreamSession session = null;
    protected String messageFilename = null;
    protected boolean ack = false;
    // Indicates this message was submitted for HA storage on other servers
    protected boolean sentHAStore = false;
    // Each Socket connection has a port and localport value. The only
    // way to uniquely (and easily) identify a network client is with
    // both values
    public int clientPort = 0;
    public int clientLocalPort = 0;
    // Track which clients are remote (on differnt computers). This is
    // important to be sure to not propogate a received message from a
    // remote client back to other remote clients. This is redundant (as
    // they will have received it from the sender themselves) and can cause
    // an infinate loop of message sending
    public boolean remoteClient = false;
    // When a queue message is received from a remote client, sending the queue
    // response must be deferred until all of the local and in-proc clients
    // have been sent the message. A response is then sent to the sender. This
    // flag is part of that deferral
    public boolean sendDeferredQResponse = false;
    public long deferredNetMsgId = 0;
    public int deferredPort = 0; // The port for the original sender
    public int deferredLocalPort = 0; // The local port for the original sender

    public JetStreamMessage() {
        msgId = JetStream.getInstance().getNewMessageID();
    }

    public JetStreamMessage(String msgId) {
        this.msgId = msgId;
    }

    public JetStreamMessage(JetStreamSession session) {
        this();
        this.session = session;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public void setDestinationName(String name) {
        this.destinationName = name;
    }

    public String getDestinationName() {
        return destinationName;
    }

    public void setFilename(String messageFilename) {
        this.messageFilename = messageFilename;
    }

    public String getFilename() {
        return this.messageFilename;
    }

    public void setMessageType(byte messageType) {
        this.messageType = messageType;
    }

    public byte getMessageType() {
        return messageType;
    }

    public void setSent(boolean sent) {
        if (sent) {
            state = SENT_STATE;
        }
    }

    public boolean getSent() {
        switch (state) {
            case PENDING_STATE:
                return false;

            case SENT_STATE:
            case DELIVERED_STATE:
            default:
                return true;
        }
    }

    public boolean isAcked() {
        return ack;
    }

    public CollectionContainer getProps() {
        return props;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Message
    public void acknowledge() throws JMSException {
        //Acknowledges all consumed messages of the session of this message. 
        this.ack = true;
    }

    public void clearBody() throws JMSException {
        //Clears out the message body. 
        state = PENDING_STATE;
        msgId = JetStream.getInstance().getNewMessageID();
    }

    public void clearProperties() throws JMSException {
        // Clears properties. 
        try {
            props.clear();
        }
        catch (Exception e) {
        }
        props = new CollectionContainer();
    }

    public String getJMSCorrelationID() throws JMSException {
        //Gets the correlation ID for the message. 
        return correlationID;
    }

    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        //Gets the correlation ID as an array of bytes for the message. 
        return correlationID.getBytes();
    }

    public int getJMSDeliveryMode() {
        //Gets the DeliveryMode value specified for this message. 
        return deliveryMode;
    }

    public Destination getJMSDestination() throws JMSException {
        //Gets the Destination object for this message. 
        return destination;
    }

    public long getJMSExpiration() throws JMSException {
        //Gets the message's expiration value. 
        return expiration;
    }

    public String getJMSMessageID() throws JMSException {
        //Gets the message ID. 
        return id;
    }

    public int getJMSPriority() throws JMSException {
        //Gets the message priority level. 
        return priority;
    }

    public boolean getJMSRedelivered() throws JMSException {
        //Gets an indication of whether this message is being redelivered. 
        return redelivered;
    }

    public Destination getJMSReplyTo() throws JMSException {
        //Gets the Destination object to which a reply to this message should be sent. 
        return replyTo;
    }

    public long getJMSTimestamp() throws JMSException {
        //Gets the message timestamp. 
        return tstamp;
    }

    public String getJMSType() throws JMSException {
        //Gets the message type identifier supplied by the client when the message was sent. 
        return type;
    }

    public void setJMSCorrelationID(String correlationID) throws JMSException {
        //Sets the correlation ID for the message. 
        this.correlationID = new String(correlationID);
    }

    public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException {
        //Sets the correlation ID as an array of bytes for the message. 
        this.correlationID = new String(correlationID);
    }

    public void setJMSDeliveryMode(int deliveryMode) throws JMSException {
        switch (deliveryMode) {
            case DeliveryMode.NON_PERSISTENT:
            case DeliveryMode.PERSISTENT:
                this.deliveryMode = (byte) deliveryMode;
                break;

            default:
                // Invalid delivery mode
                throw new JMSException("Invalid delivery mode");
        }
    }

    public void setJMSDestination(Destination destination) {
        //Sets the Destination object for this message. 
        this.destination = destination;
    }

    public void setJMSExpiration(long expiration) {
        // Sets the message's expiration value. This is only 
        // used for queues, as durable topic subscription is
        // not supported in LJMS

        if (expiration == 0) {
            // No expiration
            this.expiration = 0;
            return;
        }

        GregorianCalendar currentGMT = new GregorianCalendar();
        currentGMT.set(currentGMT.get(Calendar.YEAR) - 1900,
                currentGMT.get(Calendar.MONTH),
                currentGMT.get(Calendar.DAY_OF_MONTH),
                currentGMT.get(Calendar.HOUR_OF_DAY),
                currentGMT.get(Calendar.MINUTE),
                currentGMT.get(Calendar.SECOND));

        long gmt = currentGMT.getTime().getTime();
        this.expiration = expiration + gmt;
    }

    public void setJMSMessageID(String id) throws JMSException {
        //Sets the message ID. 
        this.id = new String(id);
    }

    public void setJMSPriority(int priority) throws JMSException {
        if ((priority > 9) || (priority < 0)) {
            return;
        }

        //Sets the priority level for this message. 
        this.priority = (byte) priority;
    }

    public void setJMSRedelivered(boolean redelivered) throws JMSException {
        //Specifies whether this message is being redelivered. 
        this.redelivered = redelivered;
    }

    public void setJMSReplyTo(Destination replyTo) throws JMSException {
        //Sets the Destination object to which a reply to this message should be sent. 
        this.replyTo = replyTo;
    }

    public void setJMSTimestamp(long tstamp) throws JMSException {
        // Sets the message timestamp. 
        if (state == SENT_STATE) {
            return;
        }

        this.tstamp = tstamp;
    }

    public void setJMSType(String type) throws JMSException {
        //Sets the message type. 
        this.type = type;
    }

    public void setLongProperty(String name, long value) throws JMSException, MessageNotWriteableException {
        if (state == SENT_STATE) {
            throw new MessageNotWriteableException("Cannot set properties after message is sent");
        }

        if (props == null) {
            clearProperties();
        }

        props.setLong(name, value);
    }

    public void setObjectProperty(String name, Object value) throws JMSException, MessageNotWriteableException {
        if (state == SENT_STATE) {
            throw new MessageNotWriteableException("Cannot set properties after message is sent");
        }

        if (props == null) {
            clearProperties();
        }

        props.setObject(name, value);
    }

    public void setShortProperty(String name, short value) throws JMSException, MessageNotWriteableException {
        if (state == SENT_STATE) {
            throw new MessageNotWriteableException("Cannot set properties after message is sent");
        }

        if (props == null) {
            clearProperties();
        }

        props.setShort(name, value);
    }

    public void setStringProperty(String name, String value) throws JMSException, MessageNotWriteableException {
        if (state == SENT_STATE) {
            throw new MessageNotWriteableException("Cannot set properties after message is sent");
        }

        if (props == null) {
            clearProperties();
        }

        props.setString(name, value);
    }

    public void setBooleanProperty(String name, boolean value) throws JMSException, MessageNotWriteableException {
        if (state == SENT_STATE) {
            throw new MessageNotWriteableException("Cannot set properties after message is sent");
        }

        if (props == null) {
            clearProperties();
        }

        props.setBoolean(name, value);
    }

    public void setByteProperty(String name, byte value) throws JMSException, MessageNotWriteableException {
        if (state == SENT_STATE) {
            throw new MessageNotWriteableException("Cannot set properties after message is sent");
        }

        if (props == null) {
            clearProperties();
        }

        props.setByte(name, value);
    }

    public void setDoubleProperty(String name, double value) throws JMSException, MessageNotWriteableException {
        if (state == SENT_STATE) {
            throw new MessageNotWriteableException("Cannot set properties after message is sent");
        }

        if (props == null) {
            clearProperties();
        }

        props.setDouble(name, value);
    }

    public void setFloatProperty(String name, float value) throws JMSException, MessageNotWriteableException {
        if (state == SENT_STATE) {
            throw new MessageNotWriteableException("Cannot set properties after message is sent");
        }

        if (props == null) {
            clearProperties();
        }

        props.setFloat(name, value);
    }

    public void setIntProperty(String name, int value) throws JMSException, MessageNotWriteableException {
        if (state == SENT_STATE) {
            throw new MessageNotWriteableException("Cannot set properties after message is sent");
        }

        if (props == null) {
            clearProperties();
        }

        props.setInt(name, value);
    }

    public boolean getBooleanProperty(String name) throws JMSException, MessageFormatException {
        try {
            return props.getBoolean(name);
        }
        catch (Exception e) {
            throw new MessageFormatException("Property does not exist");
        }
    }

    public byte getByteProperty(String name) throws JMSException, MessageFormatException {
        try {
            return props.getByte(name);
        }
        catch (Exception e) {
            throw new MessageFormatException("Property does not exist");
        }
    }

    public double getDoubleProperty(String name) throws JMSException, MessageFormatException {
        try {
            return props.getDouble(name);
        }
        catch (Exception e) {
            throw new MessageFormatException("Property does not exist");
        }
    }

    public float getFloatProperty(String name) throws JMSException, MessageFormatException {
        try {
            return props.getFloat(name);
        }
        catch (Exception e) {
            throw new MessageFormatException("Property does not exist");
        }
    }

    public int getIntProperty(String name) throws JMSException, MessageFormatException {
        try {
            return props.getInt(name);
        }
        catch (Exception e) {
            throw new MessageFormatException("Property does not exist");
        }
    }

    public long getLongProperty(String name) throws JMSException, MessageFormatException {
        try {
            return props.getLong(name);
        }
        catch (Exception e) {
            throw new MessageFormatException("Property does not exist");
        }
    }

    public Object getObjectProperty(String name) throws JMSException, MessageFormatException {
        //Returns the value of the Java object property with the specified name. 
        try {
            return props.getObject(name);
        }
        catch (Exception e) {
            throw new MessageFormatException("Property does not exist");
        }
    }

    public short getShortProperty(String name) throws JMSException, MessageFormatException {
        try {
            //Returns the value of the short property with the specified name.
            return props.getShort(name);
        }
        catch (Exception e) {
            throw new MessageFormatException("Property does not exist");
        }
    }

    public String getStringProperty(String name) throws JMSException, MessageFormatException {
        try {
            //Returns the value of the String property with the specified name. 
            return props.getString(name);
        }
        catch (Exception e) {
            throw new MessageFormatException("Property does not exist");
        }
    }

    public boolean propertyExists(String name) throws JMSException, MessageFormatException {
        if ( props == null )
            return false;
        //Indicates whether a property value exists. 
        return props.itemExists(name);
    }

    public Enumeration getPropertyNames() throws JMSException, MessageFormatException {
        try {
            //Returns an Enumeration of all the property names. 
            return props.getMapNames();
        }
        catch (Exception e) {
            return null;
        }
    }

    protected void writeString(DataOutputStream dos, String str) throws IOException {
        int length = str.length();
        dos.writeInt(length);
        if (length > 0) {
            dos.writeChars(str);
        }
    }

    protected String readString(DataInputStream dis) throws IOException {
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
