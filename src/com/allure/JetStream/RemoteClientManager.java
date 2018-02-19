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

import java.net.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * This class sends and listens for UDP packets. When the server is started this
 * class sends out a UDP multi-cast packet (twice) letting other servers on the
 * network know of its presence. When that packet is received, the remote server
 * connects to this server over port 5150 as though it were any other client.
 *
 */
public class RemoteClientManager {

    private static final Logger logger = Logger.getLogger("RemoteClientManager");
    // Constants
    protected static final int MAX_DATAGRAM_SIZE = 64;
    protected static final String UDPAddress = "224.2.2.2"; //"all-systems.mcast.net"; // LAN multicast
    protected static final int UDPTimeToLive = 16;
    protected static final int UDPPort = 5151;
    protected static final String NEW_SERVER = "NEW_SERVER";
    protected static final String SERVER_RESP = "SERVER_RESPONSE";
    protected static final String NEW_Q_LISTENER = "NEW_Q_LISTENER";
    protected static final byte NEW_SERVER_MSG_TYPE = 1;
    protected static final byte SERVER_RESP_MSG_TYPE = 2;
    protected static final byte NEW_Q_LISTENER_MSG_TYPE = 3;
    protected static final byte STATE_NEW_SERVER = 1;
    protected static final byte STATE_SERVER_STARTED = 2;
    protected byte serverState;
    // List of connected remote clients
    protected Vector remoteClientAddrs;
    //  This server's address
    //protected String localIPAddr = null;
    protected Vector localIPAddrs = new Vector();
    protected Vector localIPNames = new Vector();
    // UDP Connection info
    protected MulticastSocket multicastSocket = null;
    protected InetAddress inetAddr = null;
    MulticastSender sender = null;
    MulticastReceiver receiver = null;

    ///////////////////////////////////////////////////////////////////////////
    class MulticastSender extends Thread {

        protected String msgType = null;
        protected String msgData = null;

        public MulticastSender(String msgType, String msgData) {
            this.msgType = msgType;
            this.msgData = msgData;
            this.setName("MulticastSender");
            start();
        }

        @Override
        public void run() {
            try {
                if (JetStream.fineLevelLogging) {
                    StringBuffer sb = new StringBuffer();
                    sb.append("MulticastSender(");
                    sb.append(inetAddr);
                    sb.append(") - sending UDP datagram for message type ");
                    sb.append(msgType);
                    sb.append(" with message data=");
                    sb.append(msgData);
                    logger.info(sb.toString());
                }

                String msg = msgType + "|" + msgData;
                byte[] msgBytes = msg.getBytes();

                DatagramPacket dp = new DatagramPacket(msgBytes,
                        msgBytes.length,
                        inetAddr,
                        UDPPort);

                MulticastSocket msocket = new MulticastSocket();
                msocket.send(dp);
            }
            catch (Exception e) {
                logger.log(Level.SEVERE, "Exception", e);
            }
        }
    }
    private boolean fKeepRunning = true;

    ///////////////////////////////////////////////////////////////////////////
    class MulticastReceiver extends Thread {

        public MulticastReceiver() {
            if (JetStream.fineLevelLogging) {
                logger.info("MulticastReceiver created");
            }

            this.setName("MulticastReceiver");
            start();
        }

        @Override
        public void run() {
            try {
                byte[] buffer = new byte[MAX_DATAGRAM_SIZE];

                MulticastSocket msocket = new MulticastSocket(UDPPort);
                msocket.setSoTimeout(1000);
                msocket.joinGroup(inetAddr);

                while (fKeepRunning) {
                    DatagramPacket datagram = new DatagramPacket(buffer, buffer.length);

                    if (JetStream.fineLevelLogging) {
                        logger.info("MulticastReceiver - "
                                + "Calling receive() - will block");
                    }

                    try {
                        msocket.receive(datagram);
                    }
                    catch (SocketTimeoutException se) {
                        if (fKeepRunning) {
                            continue;
                        }

                        return;
                    }

                    if (JetStream.fineLevelLogging) {
                        logger.info("MulticastReceiver - "
                                + "Method receive() returned");
                    }

                    // Make sure this packet didn't come from us
                    //
                    InetAddress sender = datagram.getAddress();
                    if (!localIPAddrs.contains(getIPAddress(sender))) {
                        // Extract the messsage type and data from the message
                        //
                        String msg = new String(datagram.getData(),
                                datagram.getOffset(),
                                datagram.getLength());
                        int delim = msg.indexOf("|");
                        String msgType = msg.substring(0, delim);
                        String msgData = msg.substring(delim + 1);

                        if (JetStream.fineLevelLogging) {
                            logger.info(
                                    "MulticastReceiver - Received UDP datagram, message type "
                                    + msgType);
                        }

                        // Call the proper message handler based on message type
                        //
                        byte type = getMessageType(msgType);
                        switch (type) {
                            case NEW_SERVER_MSG_TYPE:
                                handleNewServerMsg(sender, msgData);
                                break;

                            case SERVER_RESP_MSG_TYPE:
                                handleServerResponseMsg(sender, msgData);
                                break;

                            case NEW_Q_LISTENER_MSG_TYPE:
                                handleNewQueueListenerMsg(sender, msgData);
                                break;

                            default:
                                logger.severe(
                                        "RemoteClientManager - unknown datagram type: "
                                        + msgType);
                        }
                    }
                }
            }
//            catch ( InterruptedException ie ) {
//
//            }
            catch (Exception e) {
                logger.log(Level.SEVERE, "Exception", e);
            }
            finally {
                System.out.println("RCM Terminating");
            }
        }

        public byte getMessageType(String type) {
            if (type.equals(NEW_SERVER)) {
                return NEW_SERVER_MSG_TYPE;
            }
            if (type.equals(SERVER_RESP)) {
                return SERVER_RESP_MSG_TYPE;
            }
            if (type.equals(NEW_Q_LISTENER)) {
                return NEW_Q_LISTENER_MSG_TYPE;
            }
            return 0;
        }
    }

    //////////////////////////////////////////////////////////////////////////
    // RemoteClientManager Methods
    ///////////////////////////////////////////////////////////////////////////
    public RemoteClientManager() {
        logger.setParent(JetStream.getInstance().getLogger());

        if (JetStream.fineLevelLogging) {
            logger.info("RemoteClientManager created");
        }

        checkPermissions();

        try {
            serverState = STATE_NEW_SERVER;

            // Save our IP address to filter UDP packets
            //
            getAllLocalAddresses();

            String l = "Local addresses = ";
            for (int a = 0; a < localIPAddrs.size(); a++) {
                if (a > 0) {
                    l += ", ";
                }
                l += localIPAddrs.elementAt(a);
            }

            //if ( JetStream.logMessages )
            logger.info(l);

            // Create the UDP address to communicate over
            //
            inetAddr = InetAddress.getByName(UDPAddress);

            //if ( JetStream.logMessages )
            logger.info("UDPAddress = " + getIPAddress(inetAddr));

            // Create a listener that runs forever listening for UDP datagrams
            //
            receiver = new MulticastReceiver();

            // Set the "new server" state and send the multicast that
            // contains our IP address
            //
            serverState = STATE_NEW_SERVER;
            String localIPAddr = (String) localIPAddrs.elementAt(0);
            String localIPName = (String) localIPNames.elementAt(0);
            JetStream.getInstance().setHostInfo(localIPName, localIPAddr);
            sender = new MulticastSender(NEW_SERVER, localIPAddr);
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
        }
    }

    public void shutdown() {
        fKeepRunning = false;
        this.receiver.interrupt();
        //this.receiver.stop();
    }

    public void getAllLocalAddresses() {
        try {
            Enumeration e = NetworkInterface.getNetworkInterfaces();

            while (e.hasMoreElements()) {
                NetworkInterface ni = (NetworkInterface) e.nextElement();

                Enumeration e2 = ni.getInetAddresses();

                while (e2.hasMoreElements()) {
                    InetAddress ip = (InetAddress) e2.nextElement();

                    if (ip.isLinkLocalAddress()) {
                        continue; // ignore localhost 127.0.0.1
                    }

                    byte[] address = ip.getAddress();
                    if (address.length == 4) {
                        String localName = this.getIPAddress(ip);//ip.getHostName();
                        String localAddr = this.getIPAddress(ip);
                        localIPAddrs.add(localAddr);
                        localIPNames.add(localName);
                    }
                }
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
        }
    }

    protected void checkPermissions() {
        try {
            SecurityManager sm = System.getSecurityManager();
            if (sm != null) {
                sm.checkConnect("localhost", -1);
            }
        }
        catch (SecurityException se) {
            logger.log(Level.SEVERE, "Exception", se);
            return;
        }

        if (JetStream.fineLevelLogging) {
            logger.info("Network permissions ok");
        }
    }

    protected void handleNewServerMsg(InetAddress sender, String IPAddress) {
        // Send a new-server response message
        //if ( JetStream.logMessages )
        logger.info("RemoteClientManager - NEW_SERVER message from " + IPAddress);

        // Connect to the new server
        JetStream.getInstance().connectRemoteClient(sender);
    }

    protected void handleServerResponseMsg(InetAddress sender, String msgData) {
        if (serverState == STATE_NEW_SERVER) {
            // Handle the message
        }
    }

    protected void handleNewQueueListenerMsg(InetAddress sender, String msgData) {
        // Since we have a new queue client, trigger the queue
        // to begin sending to clients
        //
        if (JetStream.fineLevelLogging) {
            logger.info("handleNewQueueListenerMsg recevied for queue: " + msgData);
        }

        try {
            JetStreamQueue q =
                    (JetStreamQueue) JetStream.getInstance().createQueue(msgData);

            JetStream.processQueue(q);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendNewQueueListnerMsg(String queueName) {
        if (JetStream.fineLevelLogging) {
            logger.info("Sending a NEW_Q_LISTENER UDP message");
        }
        sender = new MulticastSender(NEW_Q_LISTENER, queueName);
    }

    public String getIPAddress(InetAddress ia) {
        String s = ia.toString();
        int n = s.indexOf('/');
        return s.substring(n + 1);
    }
}
