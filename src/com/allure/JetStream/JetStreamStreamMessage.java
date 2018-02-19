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
import javax.jms.*;

public class JetStreamStreamMessage extends JetStreamBytesMessage implements StreamMessage {

    private static final long serialVersionUID = 1L;

    public JetStreamStreamMessage() {
    }

    public JetStreamStreamMessage(JetStreamNetMessage msg) {
        this.msgId = msg.messageId;
        setDestinationName(msg.destName);
        setMessageType(msg.type);
        this.deliveryMode = msg.deliveryMode;
        this.expiration = msg.expiration;
        this.priority = msg.priority;
        this.remoteClient = msg.remoteClient;
        this.props = msg.props;
    }

    ///////////////////////////////////////////////////////////////////////////
    // StreamMessage
    public void writeString(String value) throws JMSException {
        // Writes a string to the bytes message stream using UTF-8 encoding in a machine-independent manner. 
        writeUTF(value);
    }

    public boolean readBoolean() throws JMSException, MessageNotReadableException, MessageEOFException {
        // Reads a boolean from the bytes message stream. 
        try {
            position++;
            return super.readBoolean();
        }
        catch (Exception e) {
            position--;
            e.printStackTrace();
            throw new MessageEOFException(e.getMessage());
        }
    }

    public byte readByte() throws JMSException, MessageNotReadableException, MessageEOFException {
        // Reads a signed 8-bit value from the bytes message stream. 
        try {
            position++;
            return super.readByte();
        }
        catch (Exception e) {
            position--;
            e.printStackTrace();
            throw new MessageEOFException(e.getMessage());
        }
    }

    public int readBytes(byte[] value) throws JMSException, MessageNotReadableException {
        // Reads a byte array from the bytes message stream. 
        try {
            int bytes = super.readBytes(value);
            position += bytes;
            return bytes;
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new MessageEOFException(e.getMessage());
        }
    }

    public int readBytes(byte[] value, int length) throws JMSException, MessageNotReadableException {
        // Reads a portion of the bytes message stream. 
        try {
            //ObjectInputStream ois = getObjectInputStream();
            int bytes = getDataInputStream().read(value, position, length);
            position += bytes;
            return bytes;
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new MessageEOFException(e.getMessage());
        }
    }

    public char readChar() throws JMSException, MessageNotReadableException, MessageEOFException {
        // Reads a Unicode character value from the bytes message stream. 
        try {
            position++;
            return super.readChar();
        }
        catch (Exception e) {
            position--;
            e.printStackTrace();
            throw new MessageEOFException(e.getMessage());
        }
    }

    public double readDouble() throws JMSException, MessageNotReadableException, MessageEOFException {
        // Reads a double from the bytes message stream. 
        try {
            position += 8;
            return super.readDouble();
        }
        catch (Exception e) {
            position -= 8;
            e.printStackTrace();
            throw new MessageEOFException(e.getMessage());
        }
    }

    public float readFloat() throws JMSException, MessageNotReadableException, MessageEOFException {
        // Reads a float from the bytes message stream. 
        try {
            position += 4;
            return super.readFloat();
        }
        catch (Exception e) {
            position -= 4;
            e.printStackTrace();
            throw new MessageEOFException(e.getMessage());
        }
    }

    public long readLong() throws JMSException, MessageNotReadableException, MessageEOFException {
        // Reads a signed 64-bit integer from the bytes message stream. 
        try {
            position += 8;
            return super.readLong();
        }
        catch (Exception e) {
            position -= 8;
            e.printStackTrace();
            throw new MessageEOFException(e.getMessage());
        }
    }

    public short readShort() throws JMSException, MessageNotReadableException, MessageEOFException {
        // Reads a signed 16-bit number from the bytes message stream. 
        try {
            position += 2;
            return super.readShort();
        }
        catch (Exception e) {
            position -= 2;
            e.printStackTrace();
            throw new MessageEOFException(e.getMessage());
        }
    }

    public int readUnsignedByte() throws JMSException, MessageNotReadableException, MessageEOFException {
        // Reads an unsigned 8-bit number from the bytes message stream. 
        try {
            position++;
            return super.readUnsignedByte();
        }
        catch (Exception e) {
            position--;
            e.printStackTrace();
            throw new MessageEOFException(e.getMessage());
        }
    }

    public int readUnsignedShort() throws JMSException, MessageNotReadableException, MessageEOFException {
        // Reads an unsigned 16-bit number from the bytes message stream. 
        try {
            position += 2;
            return super.readUnsignedShort();
        }
        catch (Exception e) {
            position -= 2;
            e.printStackTrace();
            throw new MessageEOFException(e.getMessage());
        }
    }

    public String readUTF() throws JMSException, MessageNotReadableException, MessageEOFException {
        // Reads a string that has been encoded using a modified UTF-8 format from the bytes message stream. 
        try {
            String s = super.readUTF();
            position += s.length();
            return s;
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new MessageEOFException(e.getMessage());
        }
    }

    public String readString() throws JMSException, MessageNotReadableException, MessageEOFException, MessageFormatException {
        return this.readUTF();
    }

    public Object readObject() throws JMSException, MessageNotReadableException, MessageEOFException, MessageFormatException {
        try {
            //ObjectInputStream ois = getObjectInputStream();
            ObjectInputStream ois = new ObjectInputStream(bis);
            return ois.readObject();
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new MessageEOFException(e.getMessage());
        }
    }

    public int readInt() throws JMSException, MessageNotReadableException, MessageEOFException {
        // Reads a signed 32-bit integer from the bytes message stream. 
        try {
            position += 4;
            return super.readInt();
        }
        catch (Exception e) {
            position -= 4;
            e.printStackTrace();
            throw new MessageEOFException(e.getMessage());
        }
    }
}
