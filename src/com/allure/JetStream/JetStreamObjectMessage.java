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

public class JetStreamObjectMessage extends JetStreamMessage implements ObjectMessage {

    private static final long serialVersionUID = 1L;
    public Serializable obj = null;
    public byte[] objBytes = null;

    public JetStreamObjectMessage() {
        obj = null;
    }

    public JetStreamObjectMessage(JetStreamNetMessage msg) {
        setDestinationName(msg.destName);
        setMessageType(msg.type);
        this.objBytes = msg.bytes;
        this.deliveryMode = msg.deliveryMode;
        this.expiration = msg.expiration;
        this.priority = msg.priority;
        this.remoteClient = msg.remoteClient;
        this.props = msg.props;
        try {
            getObject();
        }
        catch (Exception e) {
        }
    }

    public JetStreamObjectMessage(JetStreamSession session) {
        this.session = session;
    }

    public JetStreamObjectMessage(byte[] bytes) {
        objBytes = bytes;
    }

    public JetStreamObjectMessage(Serializable obj) {
        this.obj = obj;
    }

    public JetStreamObjectMessage(JetStreamSession session, byte[] bytes) {
        this.session = session;
        objBytes = bytes;
    }

    public JetStreamObjectMessage(JetStreamSession session, Serializable obj) {
        this.session = session;
        this.obj = obj;
    }

    public byte[] getBytes() {
        return objBytes;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Message
    @Override
    public void clearBody() throws JMSException {
        //Clears out the message body. 
        obj = null;
    }

    ///////////////////////////////////////////////////////////////////////////
    // ObjectMessage
    public Serializable getObject() throws JMSException {
        // If we have an actual object than return that
        // 
        if (obj != null) {
            return obj;
        }

        // De-serialize the object
        //
        try {
            if (objBytes != null) {
                ByteArrayInputStream bais = new ByteArrayInputStream(objBytes);
                ObjectInputStream inStream = new ObjectInputStream(bais);
                Serializable serObj = (Serializable) inStream.readObject();
                inStream.close();
                return serObj;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public void setObject(Serializable obj) throws JMSException, MessageNotWriteableException {
        if (state == SENT_STATE) {
            throw new MessageNotWriteableException("setObject: Cannot change body after message is sent");
        }

        this.obj = obj;
    }
}
