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
import javax.jms.*;

public class JetStreamTextMessage extends JetStreamMessage implements TextMessage {

    private static final long serialVersionUID = 1L;
    protected String text = "";
    JetStreamNetMessage netMsg = null;

    public JetStreamTextMessage() {
    }

    public JetStreamTextMessage(JetStreamNetMessage msg) {
        super(msg.messageId);
        setDestinationName(msg.destName);
        setMessageType(msg.type);
        //this.text = new String(msg.text);
        this.deliveryMode = msg.deliveryMode;
        this.expiration = msg.expiration;
        this.priority = msg.priority;
        this.remoteClient = msg.remoteClient;
        this.props = msg.props;
        this.netMsg = msg;
        this.remoteClient = msg.remoteClient;
    }

    public JetStreamTextMessage(String text) {
        this.text = new String(text);
    }

    ///////////////////////////////////////////////////////////////////////////
    // TextMessage
    public void setText(String text) throws JMSException, MessageNotWriteableException {
        if (state == SENT_STATE) {
            throw new MessageNotWriteableException("setText: Cannot change body after message is sent");
        }

        this.text = new String(text);
    }

    public String getText() throws JMSException {
        // Gets the string containing this message's data. 
        // The default value is null.
        //
        if (netMsg != null) {
            return netMsg.text;
        }
        return text;
    }
}
