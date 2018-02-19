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
package com.allure.JetStream.jndi;

import com.allure.JetStream.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JNDIServer extends Thread {

    protected Logger logger = Logger.getLogger("JNDIServer");
    protected boolean listen = false;
    protected ServerSocket ss = null;

    public JNDIServer(int port) throws Exception {
        this.logger.setParent(JetStream.getInstance().getLogger());
        ss = new ServerSocket(port);
        listen = true;
        this.setName("JetStreamJNDIServer");
        start();
    }

    public void shutdown() {
        listen = false;
    }

    public void run() {
        while (listen) {
            try {
                ss.setSoTimeout(1000);
                Socket client = null;
                try {
                    client = ss.accept();
                }
                catch (SocketTimeoutException ste) {
                    if (listen) {
                        continue;
                    }
                    return;
                }

                if (client == null) {
                    continue;
                }

                JNDIClientConnection jndiClient = new JNDIClientConnection(client);

                jndiClient.start();
            }
            catch (InterruptedException ie) {
                listen = false;
            }
            catch (Exception e) {
                logger.log(Level.SEVERE, "Exception", e);
            }
        }
    }

    public boolean isServer() {
        return listen;
    }
}
