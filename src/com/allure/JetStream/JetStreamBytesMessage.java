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

import com.allure.JetStream.network.*;

import java.io.*;
import javax.jms.*;

public class JetStreamBytesMessage extends JetStreamMessage implements BytesMessage {

    private static final long serialVersionUID = 1L;
    int position = 0;
    boolean readOnly = false;
    ByteArrayInputStream bis = null;
    ByteArrayOutputStream bos = null;
    DataInputStream dis = null;
    DataOutputStream dos = null;

    public JetStreamBytesMessage() {
    }

    public JetStreamBytesMessage(byte[] bytes) {
        // De-serialize the object
        //
        try {
            if (bytes != null) {

                bos = new ByteArrayOutputStream();
                dos = new DataOutputStream(bos);
                dos.write(bytes);
                bis = new ByteArrayInputStream(bytes);
                dis = new DataInputStream(bis);
                position = 0;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public JetStreamBytesMessage(JetStreamNetMessage msg) {
        this(msg.bytes);

        setDestinationName(msg.destName);
        setMessageType(msg.type);
        this.deliveryMode = msg.deliveryMode;
        this.expiration = msg.expiration;
        this.priority = msg.priority;
        this.remoteClient = msg.remoteClient;
        this.props = msg.props;
    }

    protected DataOutputStream getDataOutputStream() {
        if (dos != null) {
            return dos;
        }

        try {
            bos = new ByteArrayOutputStream();
            dos = new DataOutputStream(bos);
            return dos;
        }
        catch (Exception e) {
        }

        return null;
    }

    protected DataInputStream getDataInputStream() {
        if (dis != null) {
            return dis;
        }

        try {
            bis = new ByteArrayInputStream(bos.toByteArray());
            dis = new DataInputStream(bis);
            position = 0;
            return dis;
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public byte[] getData() {
        try {
            dos.flush();
            return bos.toByteArray();
        }
        catch (Exception e) {
        }

        return null;
    }

    ///////////////////////////////////////////////////////////////////////////
    // BytesMessage
    public void clearBody() throws JMSException {
        readOnly = false;
        position = 0;

        try {
            bos.close();
        }
        catch (Exception e) {
        }
        try {
            dos.close();
        }
        catch (Exception e) {
        }
        try {
            dis.close();
        }
        catch (Exception e) {
        }

        bos = null;
        dos = null;
        dis = null;
    }

    public long getBodyLength() throws JMSException, MessageNotReadableException {
        try {
            // Gets the number of bytes of the message body when the message is in read-only mode. 
            if (bos != null) {
                return bos.size();
            }
        }
        catch (Exception e) {
        }

        return 0;
    }

    public boolean readBoolean() throws JMSException, MessageNotReadableException, MessageEOFException {
        // Reads a boolean from the bytes message stream. 
        try {
            //ObjectInputStream ois = getObjectInputStream();
            return getDataInputStream().readBoolean();
        }
        catch (Exception e) {
            throw new MessageEOFException("Unexpected error reading stream");
        }
    }

    public byte readByte() throws JMSException, MessageNotReadableException, MessageEOFException {
        // Reads a signed 8-bit value from the bytes message stream. 
        try {
            //ObjectInputStream ois = getObjectInputStream();
            return getDataInputStream().readByte();
        }
        catch (Exception e) {
            throw new MessageEOFException("Unexpected error reading stream");
        }
    }

    public void writeBytes(byte[] value) throws JMSException {
        // Writes a byte array to the bytes message stream. 
        try {
            //ObjectOutputStream oos = getObjectOutputStream();
            getDataOutputStream().write(value);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int readBytes(byte[] value) throws JMSException, MessageNotReadableException {
        // Reads a byte array from the bytes message stream. 
        try {
            return getDataInputStream().read(value, 0, value.length);
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new MessageEOFException(e.getMessage());
        }
    }

    public int readBytes(byte[] value, int length) throws JMSException, MessageNotReadableException {
        // Reads a portion of the bytes message stream. 
        try {
            int bytes = getDataInputStream().read(value, position, length);
            position += bytes;
            return bytes;
        }
        catch (Exception e) {
            throw new MessageEOFException("Unexpected error reading stream");
        }
    }

    public char readChar() throws JMSException, MessageNotReadableException, MessageEOFException {
        // Reads a Unicode character value from the bytes message stream. 
        try {
            return getDataInputStream().readChar();
        }
        catch (Exception e) {
            throw new MessageEOFException("Unexpected error reading stream");
        }
    }

    public double readDouble() throws JMSException, MessageNotReadableException, MessageEOFException {
        // Reads a double from the bytes message stream. 
        try {
            return getDataInputStream().readDouble();
        }
        catch (Exception e) {
            throw new MessageEOFException("Unexpected error reading stream");
        }
    }

    public float readFloat() throws JMSException, MessageNotReadableException, MessageEOFException {
        // Reads a float from the bytes message stream. 
        try {
            return getDataInputStream().readFloat();
        }
        catch (Exception e) {
            throw new MessageEOFException("Unexpected error reading stream");
        }
    }

    public long readLong() throws JMSException, MessageNotReadableException, MessageEOFException {
        // Reads a signed 64-bit integer from the bytes message stream. 
        try {
            return getDataInputStream().readLong();
        }
        catch (Exception e) {
            throw new MessageEOFException("Unexpected error reading stream");
        }
    }

    public short readShort() throws JMSException, MessageNotReadableException, MessageEOFException {
        // Reads a signed 16-bit number from the bytes message stream. 
        try {
            return getDataInputStream().readShort();
        }
        catch (Exception e) {
            throw new MessageEOFException("Unexpected error reading stream");
        }
    }

    public int readUnsignedByte() throws JMSException, MessageNotReadableException, MessageEOFException {
        // Reads an unsigned 8-bit number from the bytes message stream. 
        try {
            return getDataInputStream().readUnsignedByte();
        }
        catch (Exception e) {
            throw new MessageEOFException("Unexpected error reading stream");
        }
    }

    public int readUnsignedShort() throws JMSException, MessageNotReadableException, MessageEOFException {
        // Reads an unsigned 16-bit number from the bytes message stream. 
        try {
            return getDataInputStream().readUnsignedShort();
        }
        catch (Exception e) {
            throw new MessageEOFException("Unexpected error reading stream");
        }
    }

    public String readUTF() throws JMSException, MessageNotReadableException, MessageEOFException {
        // Reads a string that has been encoded using a modified UTF-8 format from the bytes message stream. 
        try {
            return getDataInputStream().readUTF();
            //return readString(getDataInputStream());
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new MessageEOFException("Unexpected error reading stream");
        }
    }

    public void reset() throws JMSException, MessageFormatException {
        try {
            // Puts the message body in read-only mode and repositions the stream of bytes to the beginning. 
            readOnly = true;
            position = 0;
            if (bos != null) {
                bos.reset();
                bos.close();
            }

            if (bis != null) {
                bis.reset();
                bis.close();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new MessageFormatException(e.getMessage());
        }

        bis = null;
        bos = null;
        dos = null;
        dis = null;
    }

    public void writeBoolean(boolean value) throws JMSException {
        // Writes a boolean to the bytes message stream as a 1-byte value. 
        try {
            getDataOutputStream().writeBoolean(value);
        }
        catch (Exception e) {
        }
    }

    public void writeByte(byte value) throws JMSException {
        // Writes a byte to the bytes message stream as a 1-byte value. 
        try {
            getDataOutputStream().writeByte(value);
        }
        catch (Exception e) {
        }
    }

    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        // Writes a portion of a byte array to the bytes message stream. 
        try {
            getDataOutputStream().write(value, offset, length);
        }
        catch (Exception e) {
        }
    }

    public void writeChar(char value) throws JMSException {
        // Writes a char to the bytes message stream as a 2-byte value, high byte first. 
        try {
            getDataOutputStream().writeChar(value);
        }
        catch (Exception e) {
        }
    }

    public void writeDouble(double value) throws JMSException {
        // Converts the double argument to a long using the doubleToLongBits method in class Double, and then writes that long value to the bytes message stream as an 8-byte quantity, high byte first. 
        try {
            getDataOutputStream().writeDouble(value);
        }
        catch (Exception e) {
        }
    }

    public void writeFloat(float value) throws JMSException {
        // Converts the float argument to an int using the floatToIntBits method in class Float, and then writes that int value to the bytes message stream as a 4-byte quantity, high byte first. 
        try {
            getDataOutputStream().writeFloat(value);
        }
        catch (Exception e) {
        }
    }

    public void writeInt(int value) throws JMSException {
        // Writes an int to the bytes message stream as four bytes, high byte first. 
        try {
            getDataOutputStream().writeInt(value);
        }
        catch (Exception e) {
        }
    }

    public int readInt() throws JMSException, MessageNotReadableException, MessageEOFException {
        // Reads a signed 32-bit integer from the bytes message stream. 
        try {
            return getDataInputStream().readInt();
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new MessageEOFException("Unexpected error reading stream");
        }
    }

    public void writeLong(long value) throws JMSException {
        // Writes a long to the bytes message stream as eight bytes, high byte first. 
        try {
            getDataOutputStream().writeLong(value);
        }
        catch (Exception e) {
        }
    }

    public void writeObject(Object value) throws JMSException, MessageFormatException, MessageNotWriteableException {
        // Writes an object to the bytes message stream. 
        try {
            // TODO: need to serialize the object to a byte array and write the bytes
            //getDataOutputStream().writeObject(value);
        }
        catch (Exception e) {
            throw new MessageFormatException(e.getMessage());
        }
    }

    public void writeShort(short value) throws JMSException {
        // Writes a short to the bytes message stream as two bytes, high byte first. 
        try {
            getDataOutputStream().writeShort(value);
        }
        catch (Exception e) {
        }
    }

    public void writeUTF(String value) throws JMSException {
        // Writes a string to the bytes message stream using UTF-8 encoding in a machine-independent manner. 
        try {
            getDataOutputStream().writeUTF(value);
            //writeString(getDataOutputStream(), value);
        }
        catch (Exception e) {
        }
    }
}
