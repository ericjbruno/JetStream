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

import java.io.*;
import java.net.*;
import javax.naming.*;

import com.allure.JetStream.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JetStreamJNDI {

    private final static JetStreamJNDI _INSTANCE = new JetStreamJNDI();

    public static JetStreamJNDI getInstance() {
        return _INSTANCE;
    }
    public static final int JNDI_PORT = 1099;
    //private JetStreamLog logger = new JetStreamLog("JetStreamJNDI");
    private Logger logger = Logger.getLogger("JetStreamJNDI");
    private boolean fListening = false;
    private JNDIServer jndiServer = null;

    private JetStreamJNDI() {
        JetStream js = JetStream.getInstance();
        logger.setParent(js.getLogger());
        fListening = startJNDIServer();
    }

    public boolean startJNDIServer() {
        try {
            jndiServer = new JNDIServer(JNDI_PORT);
            if (jndiServer.isServer()) {
                if (JetStream.fineLevelLogging) {
                    logger.info("JNDI server started");
                }

                return true;
            }
        }
        catch (Exception e) {
        }

        return false;
    }

    public void stopJNDIServer() {
        if (isServer()) {
            jndiServer.shutdown();
        }
    }

    public boolean isServer() {
        // If we're already listening on the JNDI port, return true
        // otherwise return the result of trying to listen. If we still
        // cannot, then the remote JNDI server is still running
        //
        if (fListening) {
            return true;
        }

        return startJNDIServer();
    }

    private JNDIMessage sendRequest(JNDIMessage request) {
        Socket server = null;

        try {
            // Connect to the server
            //
            server = new Socket("localhost", JNDI_PORT);
            BufferedOutputStream out = new BufferedOutputStream(server.getOutputStream());
            BufferedInputStream in = new BufferedInputStream(server.getInputStream());

            // Send the message
            //
            ObjectOutputStream oos = new ObjectOutputStream(out);
            oos.writeObject(request);
            oos.flush();

            // Wait for the response
            //
            ObjectInputStream ois = new ObjectInputStream(in);
            JNDIMessage response = (JNDIMessage) ois.readObject();
            if (JetStream.fineLevelLogging) {
                logger.info(response.text);
            }

            return response;
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
        }
        finally {
            if (server != null) {
                try {
                    server.close();
                }
                catch (Exception e) {
                }
            }
        }

        return null;
    }

    public Object sendLookupRequest(String name) {
        // Send the message
        //
        JNDIMessage request = new JNDIMessage();
        request.type = JNDIMessage.LOOKUP_MSG;
        request.text = name;
        JNDIMessage response = sendRequest(request);

        // Was object found?
        if (response.object == null) {
            return null;
        }

        // Is the object a JMS Destination?
        try {
            if (response.object instanceof JetStreamDestinationJNDIData) {
                JetStreamDestinationJNDIData destData =
                        (JetStreamDestinationJNDIData) response.object;

                switch (destData.type) {
                    case JetStreamDestinationJNDIData.TOPIC:
                        return JetStream.getInstance().createTopic(destData.name);

                    case JetStreamDestinationJNDIData.QUEUE:
                        return JetStream.getInstance().createQueue(destData.name);

                    default:
                        return null;
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // Return the object
        return response.object;
    }

    public void sendBindRequest(String name, Object obj) throws NamingException {
        // Send the message
        //
        JNDIMessage request = new JNDIMessage();
        request.type = JNDIMessage.BIND_MSG;
        request.text = name;
        request.object = (Serializable) obj;
        JNDIMessage response = sendRequest(request);
        if (response == null || response.text.equalsIgnoreCase("error")) {
            throw new NamingException("General bind error ocurred");
        }
        else if (response.text.equals("duplicate")) {
            throw new NameAlreadyBoundException("Use rebind to override");
        }
    }

    public void sendRebindRequest(String name, Object obj) throws NamingException {
        // Send the message
        //
        JNDIMessage request = new JNDIMessage();
        request.type = JNDIMessage.REBIND_MSG;
        request.text = name;
        request.object = (Serializable) obj;
        JNDIMessage response = sendRequest(request);
        if (response == null || response.text.equals("error")) {
            throw new NamingException("General rebind error ocurred");
        }
    }
}
