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

import javax.jms.*;

public class JetStreamQueueConnectionFactory extends JetStreamConnectionFactory implements QueueConnectionFactory {

    public JetStreamQueueConnectionFactory() {
    }

    ///////////////////////////////////////////////////////////////////////////
    // QueueConnectionFactory
    public QueueConnection createQueueConnection() {
        // Creates a queue connection with the default user identity. 
        //
        return new JetStreamQueueConnection();
    }

    public QueueConnection createQueueConnection(String userName, String password) {
        // Creates a queue connection with the specified user identity. 
        //
        return new JetStreamQueueConnection();
    }
}
