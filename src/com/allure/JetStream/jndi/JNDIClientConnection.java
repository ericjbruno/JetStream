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

import com.allure.JetStream.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JNDIClientConnection extends Thread {
    //private JetStreamLog logger = new JetStreamLog("JNDIClientConnection");

    private Logger logger = Logger.getLogger("JNDIClientConnection");
    BufferedInputStream in = null;
    BufferedOutputStream out = null;

    public JNDIClientConnection() {
        logger.setParent(JetStream.getInstance().getLogger());
    }

    public JNDIClientConnection(Socket client) throws Exception {
        this();
        in = new BufferedInputStream(client.getInputStream());
        out = new BufferedOutputStream(client.getOutputStream());
        setName("JNDIClientConnection_" + client);
    }

    public void run() {
        while (true) {
            try {
                ObjectInputStream ois = new ObjectInputStream(in);
                JNDIMessage msg = (JNDIMessage) ois.readObject();
                //ois.reset();

                switch (msg.type) {
                    case JNDIMessage.LOOKUP_MSG:
                        if (JetStream.fineLevelLogging) {
                            logger.info("JNDI lookup request");
                        }
                        onLookupRequest(msg.text);
                        break;

                    case JNDIMessage.BIND_MSG:
                        if (JetStream.fineLevelLogging) {
                            logger.info("JNDI bind request");
                        }
                        onBindRequest(msg.text, msg.object);
                        break;

                    case JNDIMessage.REBIND_MSG:
                        if (JetStream.fineLevelLogging) {
                            logger.info("JNDI rebind request");
                        }
                        break;

                    case JNDIMessage.UNBIND_MSG:
                        if (JetStream.fineLevelLogging) {
                            logger.info("JNDI unbind request");
                        }
                        break;

                    default:
                        if (JetStream.fineLevelLogging) {
                            logger.info("Unsupported JNDI message");
                        }
                        break;
                }
            }
            catch (Exception e) {
                //e.printStackTrace();
                break;
            }
        }

        if (JetStream.fineLevelLogging) {
            logger.info("JNDI client closed connection. Thread exiting");
        }
    }

    private void sendResponse(JNDIMessage response) {
        ObjectOutputStream oos = null;

        try {
            oos = new ObjectOutputStream(out);
            oos.writeObject(response);
            oos.flush();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            try {
                if (oos != null) {
                    oos.close();
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void onLookupRequest(String name) {
        Serializable obj = null;

        try {
            // Lookup the object internally
            //
            JetStreamContext ctx = new JetStreamContext(null);
            Object temp = ctx.lookup(name);

            // If the object is a JMS Destination, create a special
            // object that will return information about it, otherwise
            // return the object itself
            //
            if (temp instanceof JetStreamTopic) {
                JetStreamDestinationJNDIData destData =
                        new JetStreamDestinationJNDIData();
                destData.type = JetStreamDestinationJNDIData.TOPIC;
                destData.name = ((JetStreamTopic) temp).getName();
                obj = destData;
            }
            else if (temp instanceof JetStreamQueue) {
                JetStreamDestinationJNDIData destData =
                        new JetStreamDestinationJNDIData();
                destData.type = JetStreamDestinationJNDIData.QUEUE;
                destData.name = ((JetStreamQueue) temp).getName();
                obj = destData;
            }
            else {
                obj = (Serializable) temp;
            }
        }
        catch (ClassCastException e) {
            if (JetStream.fineLevelLogging) {
                logger.info(name + " not serializable");
            }
        }
        catch (Exception e) {
            if (JetStream.fineLevelLogging) {
                logger.info(name + " not found");
            }
        }

        // Send a JNDIMessage back with the object (or null if not found)
        //
        JNDIMessage response = new JNDIMessage();
        response.type = JNDIMessage.LOOKUP_MSG;
        response.object = obj;
        if (obj == null) {
            response.text = "Object not found";
        }
        else {
            response.text = "Object found";
        }

        sendResponse(response);
    }

    public void onBindRequest(String name, Object obj) {
        JNDIMessage response = new JNDIMessage();

        try {
            // Lookup the object internally
            //
            JetStreamContext ctx = new JetStreamContext(null);
            Object temp = ctx.lookup(name);
            if (temp == null) {
                // Bind the object
                ctx.bind(name, obj);

                // Send a successful response
                response.type = JNDIMessage.BIND_MSG;
                response.text = "success";
            }
            else {
                // Tell the client this name is bound
                response.type = JNDIMessage.BIND_MSG;
                response.text = "duplicate";
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception:", e);
            e.printStackTrace();

            // An error occured
            response.type = JNDIMessage.BIND_MSG;
            response.text = "error";
        }

        sendResponse(response);
        return;
    }
}
