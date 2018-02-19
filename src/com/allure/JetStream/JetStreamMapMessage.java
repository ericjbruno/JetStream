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

import com.allure.JetStream.network.JetStreamNetMessage;
import java.io.*;
import java.util.*;
import javax.jms.*;

public class JetStreamMapMessage extends JetStreamMessage implements MapMessage {

    private static final long serialVersionUID = 1L;
    public CollectionContainer data = new CollectionContainer();

    public JetStreamMapMessage() {
    }

    public JetStreamMapMessage(byte[] bytes) {
        // De-serialize the object
        //
        try {
            if (bytes != null) {
                ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                ObjectInputStream inStream = new ObjectInputStream(bais);
                Serializable serObj = (Serializable) inStream.readObject();
                inStream.close();
                data = (CollectionContainer) serObj;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public JetStreamMapMessage(JetStreamNetMessage msg) {
        this(msg.bytes);

        setDestinationName(msg.destName);
        setMessageType(msg.type);
        this.deliveryMode = msg.deliveryMode;
        this.expiration = msg.expiration;
        this.priority = msg.priority;
        this.remoteClient = msg.remoteClient;
        this.props = msg.props;
    }

    ///////////////////////////////////////////////////////////////////////////
    // MapMessage
    @Override
    public void clearBody() throws JMSException {
        try {
            data.clear();
        }
        catch (Exception e) {
        }
        data = new CollectionContainer();
    }

    public CollectionContainer getData() {
        return data;
    }

    public boolean getBoolean(String name) throws JMSException {
        try {
            return data.getBoolean(name);
        }
        catch (Exception e) {
            throw new MessageFormatException(e.getMessage());
        }
    }

    public byte getByte(String name) throws JMSException {
        try {
            return data.getByte(name);
        }
        catch (Exception e) {
            throw new MessageFormatException(e.getMessage());
        }
    }

    public short getShort(String name) throws JMSException {
        try {
            return data.getShort(name);
        }
        catch (Exception e) {
            throw new MessageFormatException(e.getMessage());
        }
    }

    public int getInt(String name) throws JMSException {
        try {
            return data.getInt(name);
        }
        catch (Exception e) {
            throw new MessageFormatException(e.getMessage());
        }
    }

    public long getLong(String name) throws JMSException {
        try {
            return data.getLong(name);
        }
        catch (Exception e) {
            throw new MessageFormatException(e.getMessage());
        }
    }

    public float getFloat(String name) throws JMSException {
        try {
            return data.getFloat(name);
        }
        catch (Exception e) {
            throw new MessageFormatException(e.getMessage());
        }
    }

    public double getDouble(String name) throws JMSException {
        try {
            return data.getDouble(name);
        }
        catch (Exception e) {
            throw new MessageFormatException(e.getMessage());
        }
    }

    public char getChar(String name) throws JMSException {
        try {
            return data.getChar(name);
        }
        catch (Exception e) {
            throw new MessageFormatException(e.getMessage());
        }
    }

    public String getString(String name) throws JMSException {
        try {
            return data.getString(name);
        }
        catch (Exception e) {
            throw new MessageFormatException(e.getMessage());
        }
    }

    public byte[] getBytes(String name) throws JMSException {
        try {
            return data.getBytes(name);
        }
        catch (Exception e) {
            throw new MessageFormatException(e.getMessage());
        }
    }

    public Object getObject(String name) throws JMSException {
        try {
            return data.getObject(name);
        }
        catch (Exception e) {
            throw new MessageFormatException(e.getMessage());
        }
    }

    public Enumeration getMapNames() throws JMSException {
        try {
            return data.getMapNames();
        }
        catch (Exception e) {
            throw new MessageFormatException(e.getMessage());
        }
    }

    public void setBoolean(String name, boolean value) throws JMSException {
        data.setBoolean(name, value);
    }

    public void setByte(String name, byte value)
            throws JMSException {
        data.setByte(name, value);
    }

    public void setShort(String name, short value)
            throws JMSException {
        data.setShort(name, value);
    }

    public void setChar(String name, char value)
            throws JMSException {
        data.setChar(name, value);
    }

    public void setInt(String name, int value)
            throws JMSException {
        data.setInt(name, value);
    }

    public void setLong(String name, long value)
            throws JMSException {
        data.setLong(name, value);
    }

    public void setFloat(String name, float value)
            throws JMSException {
        data.setFloat(name, value);
    }

    public void setDouble(String name, double value)
            throws JMSException {
        data.setDouble(name, value);
    }

    public void setString(String name, String value)
            throws JMSException {
        data.setString(name, value);
    }

    public void setBytes(String name, byte[] value)
            throws JMSException {
        try {
            data.setBytes(name, value);
        }
        catch (Exception e) {
            throw new MessageFormatException(e.getMessage());
        }
    }

    public void setBytes(String name, byte[] value, int offset, int length)
            throws JMSException {
        try {
            data.setBytes(name, value, offset, length);
        }
        catch (Exception e) {
            throw new MessageFormatException(e.getMessage());
        }
    }

    public void setObject(String name, Object value)
            throws JMSException {
        // Sets an object value with the specified name into the Map.
        data.setObject(name, value);
    }

    public boolean itemExists(String name) throws JMSException {
        return data.itemExists(name);
    }
}
