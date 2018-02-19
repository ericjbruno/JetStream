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
package com.allure.JetStream.network;

import com.allure.JetStream.*;
import java.net.*;
import java.io.*;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.*;

public class JetStreamSocketClient {

    public JetStream jetStream = JetStream.getInstance();
    public int port = 0;
    public int localPort = 0;
    public long lastSend = 0;
    public boolean remoteClient = false;
    private Logger logger = Logger.getLogger("JetStreamSocketClient");
    private JetStreamSender sender = null;
    private JetStreamListener listener = null;
    public String hostName = "";
    public String hostAddr = "";
    public InetAddress ipaddr = null;
    public long serverStartTime = 0;
    public long lastHeartbeat = 0;
    private long current = 0;
    private long elapsed = 0;
    private static final int OBJECT = 1;
    private static final int DYN_SIZE = 2;
    private static final int STATIC_SIZE = 3;
    private static final int SIZE_DELIMETED = 4;
    private static final int FIELD_DELIMITED = 5;
    private int streamType = SIZE_DELIMETED;
    static int BYTE_MASK = 0xff;
    static int SHORT_MASK = 0xffff;
    static int BYTE_SHIFT = 8; //bits
    Vector<JetStreamNetMessage> _msgs =
            new Vector<JetStreamNetMessage>();

    public JetStreamListener getListener() {
        return listener;
    }
    // This flag indicates that we should report link status changes. One case
    // where we don't want to is with the dedicated HA link because it's a
    // redundant link to a remote server
    //
    private boolean stealthConnection = false;

    public void setStealthConnection(boolean stealthConnection) {
        this.stealthConnection = stealthConnection;
        if (stealthConnection && JetStream.fineLevelLogging) {
            logger.info("Stealth connection: " + this);
        }
    }

    public boolean isStealthConnection() {
        return stealthConnection;
    }

    ///////////////////////////////////////////////////////////////////////////
    public class JetStreamListener extends Thread {

        public boolean listening = true;
        private Socket conn = null;

        private JetStreamListener() {
        }

        public JetStreamListener(Socket conn) {
            if (JetStream.fineLevelLogging) {
                logger.info("JetStreamListener created");
            }
            this.conn = conn;
            this.setName("JetStreamListener:" + hostAddr);
            this.start();
        }

        public Socket getConnection() {
            return this.conn;
        }

        @Override
        public void run() {

            InputStream instream = null;

            try {
                Inflater decompresser = new Inflater();

                DataInputStream dis =
                    new DataInputStream(
                        new BufferedInputStream(conn.getInputStream()));

                while (listening) {
                    if (JetStream.fineLevelLogging) {
                        logger.info("JetStreamSocketClient: Waiting for a network message");
                    }

                    // Get the length of the next message
                    int msgSize = dis.readInt();
                    if (msgSize == -1) {
                        throw new SocketException("Client Closed Socket");
                    }
                    int compressedSize = dis.readInt();
                    if (compressedSize == -1) {
                        throw new SocketException("Client Closed Socket");
                    }

                    if (JetStream.fineLevelLogging) {
                        logger.info("Reading " + compressedSize + " compressed bytes. Original length=" + msgSize);
                    }

                    // make sure to read the next message's bytes
                    // completely before deserializing 
                    if (msgSize > 0) {
                        //byte[] msgBytes = new byte[msgSize];
                        byte[] msgBytes = null;
                        byte[] compressed = new byte[compressedSize];

                        dis.readFully(compressed);

                        if (compressedSize != msgSize) {
                            // Decompress the bytes
                            decompresser.reset();
                            decompresser.setInput(compressed);
                            msgBytes = new byte[msgSize];
                            decompresser.inflate(msgBytes);
                        }
                        else {
                            msgBytes = compressed;
                        }

                        JetStreamNetMessage netMsg =
                                deserializeNetObject(msgBytes);

                        if (netMsg != null) {
                            processNetMessage(netMsg, conn);
                        }
                    }
                }
            }
            catch (SocketException se) {
                logger.info("Client SocketException: " + se.toString());
                listening = false;
                close();
            }
            catch (StreamCorruptedException sce) {
                logger.severe("StreamCorruptedException: " + sce.toString());
                try {
                    if (instream != null) {
                        instream.skip(instream.available());
                    }
                }
                catch (Exception e1) {
                }
            }
            catch (EOFException e) {
                // Client disconnected
                logger.info("Client disconnected");
                listening = false;
                close();
            }
            catch (Exception e) {
                logger.log(Level.SEVERE, "Exception", e);
                //listening = false;
                //close();
            }

            onClientLinkLost();
        }
    }
    int last = -1;

    private void processNetMessage(JetStreamNetMessage msg,
            Socket conn) {
        current = System.currentTimeMillis();
        elapsed = current - lastSend;
//                System.out.println("RECEIVED MSG");

        String destName = msg.destName;
        switch (msg.type) {
            case JetStreamNetMessage.SERVER_HEARTBEAT_MESSAGE:
                //System.out.print("K");
                if (JetStream.fineLevelLogging) {
                    logger.info("JetStreamSocketClient: SERVER_HEARTBEAT_MESSAGE received, time: " + elapsed);
                }

                long startTime = new Long(destName).longValue(); // hack!
                jetStream.netMsgHandler.onHeartbeat(hostName, hostAddr, startTime);
                break;

            case JetStreamNetMessage.QUEUE_RESPONSE_MESSAGE:
                //System.out.print("R");
                if (JetStream.fineLevelLogging) {
                    logger.info("JetStreamSocketClient: QUEUE_RESPONSE_MESSAGE received, queue: " + destName + ", rcvd=" + msg.messageReceived);
                }

                // Each message serves as a heartbeat
                if (remoteClient) {
                    jetStream.netMsgHandler.onHeartbeat(hostName, hostAddr, -1);
                }

                jetStream.netMsgHandler.onQueueResponseMessage(
                        destName, msg.netMsgId, msg.messageReceived,
                        conn.getPort(), conn.getLocalPort());
                break;

            case JetStreamNetMessage.NEW_DURABLE_SUB_MESSAGE:
                if (JetStream.fineLevelLogging) {
                    logger.info("JetStreamSocketClient: NEW_DURABLE_SUB_MESSAGE received");
                }

                // Each message serves as a heartbeat
                if (remoteClient) {
                    jetStream.netMsgHandler.onHeartbeat(hostName, hostAddr, -1);
                }

                jetStream.netMsgHandler.onNewDurableSubMsg(this, destName, msg.text);
                break;

            case JetStreamNetMessage.DURABLE_SUB_REMOVE_MESSAGE:
                if (JetStream.fineLevelLogging) {
                    logger.info("JetStreamSocketClient: DURABLE_SUB_REMOVE_MESSAGE received");
                }

                // Each message serves as a heartbeat
                if (remoteClient) {
                    jetStream.netMsgHandler.onHeartbeat(hostName, hostAddr, -1);
                }

                jetStream.netMsgHandler.onRemoveDurableSubMsg(this, destName, msg.text);
                break;

            case JetStreamNetMessage.DURABLE_SUB_UNSUBSCRIBE_MESSAGE:
                if (JetStream.fineLevelLogging) {
                    logger.info("JetStreamSocketClient: DURABLE_SUB_UNSUBSCRIBE_MESSAGE received");
                }

                // Each message serves as a heartbeat
                if (remoteClient) {
                    jetStream.netMsgHandler.onHeartbeat(hostName, hostAddr, -1);
                }

                jetStream.netMsgHandler.onUnsubscribeMsg(this, destName, msg.text);
                break;

            case JetStreamNetMessage.NEW_QUEUE_LISTENER_MESSAGE:
                //System.out.print("L");
                if (JetStream.fineLevelLogging) {
                    logger.info("JetStreamSocketClient: NEW_QUEUE_LISTENER_MESSAGE received");
                }

                // Each message serves as a heartbeat
                if (remoteClient) {
                    jetStream.netMsgHandler.onHeartbeat(hostName, hostAddr, -1);
                }

                jetStream.netMsgHandler.onNewQueueListenerMsg(destName);
                break;

            case JetStreamNetMessage.HIGH_AVAIL_STORE_MESSAGE:
                //System.out.print("H");
                if (JetStream.fineLevelLogging) {
                    logger.info("JetStreamSocketClient: HIGH_AVAIL_STORE_MESSAGE received");
                }

                // Each message serves as a heartbeat
                if (remoteClient) {
                    jetStream.netMsgHandler.onHeartbeat(hostName, hostAddr, -1);
                }

                fire_onNewHighAvailStoreMessage(
                        destName, msg,
                        msg.netMsgId,
                        hostName, hostAddr);
                break;

            case JetStreamNetMessage.HIGH_AVAIL_REMOVE_MESSAGE:
                //System.out.print("D");
                if (JetStream.fineLevelLogging) {
                    logger.info("JetStreamSocketClient: HIGH_AVAIL_REMOVE_MESSAGE received");
                }

                // Each message serves as a heartbeat
                if (remoteClient) {
                    jetStream.netMsgHandler.onHeartbeat(hostName, hostAddr, -1);
                }

                jetStream.ha.onHighAvailRemovalMessage(
                        destName, msg.text, hostName, hostAddr);
                break;

            case JetStreamNetMessage.HIGH_AVAIL_RECOVERY_MESSAGE:
                //if ( JetStream.logMessages )
                logger.info("JetStreamSocketClient: HIGH_AVAIL_RECOVERY_MESSAGE received");

                String failedHostAddr = destName;
                jetStream.ha.onRemoteServerRecovery(failedHostAddr, hostAddr);
                break;

            case JetStreamNetMessage.RECOVERED_PERSISTED_MESSAGES:
                //if ( JetStream.logMessages )
                logger.info("JetStreamSocketClient: RECOVERED_PERSISTED_MESSAGES received");

                jetStream.onMessagesRecovered(hostAddr);
                break;

            case JetStreamNetMessage.PUB_SUB_MESSAGE:
            case JetStreamNetMessage.QUEUE_MESSAGE:
                //System.out.print("Q");
                if (JetStream.fineLevelLogging) {
                    if (msg.type == JetStreamNetMessage.PUB_SUB_MESSAGE) {
                        logger.info("PUB_SUB_MESSAGE received for topic " + destName);
                    }
                    else {
                        logger.info("QUEUE_MESSAGE received for queue " + destName);
                    }
                }

                msg.remoteClient = remoteClient;

                // Each message serves as a heartbeat
                if (remoteClient) {
                    jetStream.netMsgHandler.onHeartbeat(hostName, hostAddr, -1);
                }

                if (msg.messagePayloadType == JetStreamNetMessage.STRING_OBJ) {
                    JetStreamTextMessage jsTextMsg =
                            new JetStreamTextMessage(msg);

                    /*
                     JUST FOR TESTING
                     Integer msgVal = new Integer(msg.text);
                     int newVal = msgVal.intValue();
                     if ( last > -1 ) {
                     if ( newVal != (last + 1) )
                     logger.info("Low level seq skip. CURR=" + newVal + ", PREV=" + last);
                     }
                     last = newVal;
                     */

                    /*
                     //if ( JetStream.getInstance().isServer() )
                     {
                     try {
                     logger.info(jsTextMsg.getText());
                     }catch(Exception e){}
                     }
                     */
                    jetStream.netMsgHandler.onTextMessage(
                            destName, jsTextMsg,
                            msg.netMsgId,
                            conn.getPort(),
                            conn.getLocalPort());
                }
                else if ((msg.messagePayloadType == JetStreamNetMessage.CUSTOM_OBJ)) {
                    JetStreamObjectMessage jsObjMsg =
                            new JetStreamObjectMessage(msg);

                    jetStream.netMsgHandler.onObjectMessage(
                            destName, jsObjMsg,
                            msg.netMsgId,
                            conn.getPort(),
                            conn.getLocalPort());
                }
                else if ((msg.messagePayloadType == JetStreamNetMessage.MAP_OBJ)) {
                    JetStreamMapMessage jsMapMsg =
                            new JetStreamMapMessage(msg);

                    jetStream.netMsgHandler.onMapMessage(
                            destName, jsMapMsg,
                            msg.netMsgId,
                            conn.getPort(),
                            conn.getLocalPort());
                }
                else {
                    JetStreamBytesMessage jsBytesMsg =
                            new JetStreamBytesMessage(msg);

                    jetStream.netMsgHandler.onBytesMessage(
                            destName, jsBytesMsg,
                            msg.netMsgId,
                            conn.getPort(),
                            conn.getLocalPort());
                }
                break;

            default:
                logger.severe("Unknown message type: " + msg.type);
        }
    }
    static int sends = 0;

    ///////////////////////////////////////////////////////////////////////////
    class JetStreamSender {

        public boolean readyToSend = true;
        private final Socket conn;
        private OutputStream os = null;

        private JetStreamSender() {
            // Should never be called
            this.conn = null;
        }

        public JetStreamSender(Socket conn) {
            this.conn = conn;
            try {
                this.conn.setTcpNoDelay(true);
                this.os = new BufferedOutputStream(conn.getOutputStream());
            }
            catch (Exception e) {
                logger.log(Level.SEVERE, "Exception", e);
            }

            if (JetStream.compressSpeed) {
                this.compresser = new Deflater(Deflater.BEST_SPEED);
            }
            else {
                this.compresser = new Deflater(Deflater.BEST_COMPRESSION);
            }
        }

        public void sendMsg(Object obj) throws Exception {
            // Serialize the object and send
            byte[] bytes = serializeNetObject(obj);
            sendMsg(bytes);
        }
        Deflater compresser = null;//new Deflater(Deflater.HUFFMAN_ONLY);

        public void sendMsg(byte[] bytes) throws Exception {
            if (JetStream.fineLevelLogging) {
                logger.info("JetStreamSender.sendMsg() - sending message to client");
            }

            synchronized (conn) {
                try {
                    //System.out.println("sendMsg(begin) - " + System.currentTimeMillis());

                    // Write the uncompressed message size first as four bytes (of the int)
                    byte b1 = (byte) ((bytes.length >> (JetStreamSocketClient.BYTE_SHIFT * 3)) & JetStreamSocketClient.BYTE_MASK);
                    byte b2 = (byte) ((bytes.length >> (JetStreamSocketClient.BYTE_SHIFT * 2)) & JetStreamSocketClient.BYTE_MASK);
                    byte b3 = (byte) ((bytes.length >> JetStreamSocketClient.BYTE_SHIFT) & JetStreamSocketClient.BYTE_MASK);
                    byte b4 = (byte) (bytes.length & JetStreamSocketClient.BYTE_MASK);
                    os.write(b1);
                    os.write(b2);
                    os.write(b3);
                    os.write(b4);

                    int length = bytes.length;

                    // Only compress large messages sent to remote servers
                    if (JetStream.compress && remoteClient
                            && (length > JetStream.COMPRESS_THRESHOLD)) {
                        // Compress the bytes
                        compresser.reset();
                        compresser.setInput(bytes);
                        compresser.finish();
                        length = compresser.deflate(bytes);
                    }

                    // Write the COMPRESSED message size first as four bytes (of the int)
                    b1 = (byte) ((length >> (JetStreamSocketClient.BYTE_SHIFT * 3)) & JetStreamSocketClient.BYTE_MASK);
                    b2 = (byte) ((length >> (JetStreamSocketClient.BYTE_SHIFT * 2)) & JetStreamSocketClient.BYTE_MASK);
                    b3 = (byte) ((length >> JetStreamSocketClient.BYTE_SHIFT) & JetStreamSocketClient.BYTE_MASK);
                    b4 = (byte) (length & JetStreamSocketClient.BYTE_MASK);
                    os.write(b1);
                    os.write(b2);
                    os.write(b3);
                    os.write(b4);

                    if (JetStream.fineLevelLogging) {
                        logger.info("Writing " + length + " compressed bytes. Original length=" + bytes.length);
                    }

                    // Write the message bytes now
                    os.write(bytes, 0, length);
                    jsflush();

                    //System.out.println("sendMsg(end) - " + System.currentTimeMillis());
                }
                catch (SocketException se) {
                    if (JetStream.fineLevelLogging) {
                        logger.info("Client SocketException: " + se.getMessage());
                    }
                    close();
                    onClientLinkLost();
                }
                catch (Exception e) {
                    logger.log(Level.SEVERE, "Exception", e);
                }
            }
        }

        private void jsflush() throws Exception {
            os.flush();
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // JetStreamSocketClient Methods

    public byte[] serializeNetObject(Object obj) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        JetStreamNetMessage netMsg = (JetStreamNetMessage)obj;
        dos.writeUTF(netMsg.destName);
        dos.writeByte(netMsg.type);
        dos.writeByte(netMsg.messagePayloadType);
        dos.writeLong(netMsg.netMsgId);
        dos.writeUTF(netMsg.messageId);
        dos.writeBoolean(netMsg.messageReceived);
        dos.writeInt(netMsg.clientPort);
        dos.writeInt(netMsg.clientLocalPort);
        dos.writeBoolean(netMsg.remoteClient);
        dos.writeByte(netMsg.deliveryMode);
        dos.writeByte(netMsg.priority);
        dos.writeLong(netMsg.expiration);
        if (netMsg.props != null) {
            ByteArrayOutputStream bstream = new ByteArrayOutputStream();
            ObjectOutputStream ostream = new ObjectOutputStream(bstream);
            ostream.writeObject(netMsg.props);
            byte[] bytes = bstream.toByteArray();
            ostream.close();

            dos.writeLong(bytes.length);
            dos.write(bytes);
        }
        else {
            dos.writeLong(0);
        }
        if (netMsg.messagePayloadType == JetStreamNetMessage.STRING_OBJ) {
            //dos.writeUTF(netMsg.text);
            writeString(dos, netMsg.text);
        }
        else {
            if (netMsg.bytes != null) {
                dos.writeInt(netMsg.bytes.length);
                dos.write(netMsg.bytes);
            }
            else {
                dos.writeInt(0);
            }
        }
        dos.flush();
        byte[] bytes = bos.toByteArray();
        bos.reset();
        return bytes;
    }

    public JetStreamNetMessage deserializeNetObject(byte[] bytes) throws Exception {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);

        try {
            if (JetStream.fineLevelLogging) {
                logger.info("DESERIALIZE--BYTES AVAILABLE: " + bis.available());
            }

            String destName = dis.readUTF();
            byte type = dis.readByte();

            // For performance reasons, if there are no listeners and
            // we are not the server, just abort deserialization
            if (!jetStream.isServer()) {
                if (type == JetStreamNetMessage.PUB_SUB_MESSAGE) {
                    JetStreamTopic jsTopic =
                            jetStream._topics.get(destName);
                    if (jsTopic == null || jsTopic.numConsumers() == 0) {
                        return null;
                    }
                }
            }

            JetStreamNetMessage netMsg = new JetStreamNetMessage();
            netMsg.destName = destName;
            netMsg.type = type;
            netMsg.messagePayloadType = dis.readByte();
            netMsg.netMsgId = dis.readLong();
            netMsg.messageId = dis.readUTF();
            netMsg.messageReceived = dis.readBoolean();
            netMsg.clientPort = dis.readInt();
            netMsg.clientLocalPort = dis.readInt();
            netMsg.remoteClient = dis.readBoolean();
            netMsg.deliveryMode = dis.readByte();
            netMsg.priority = dis.readByte();
            netMsg.expiration = dis.readLong();
            long propsLen = dis.readLong();
            if (propsLen > 0) {
                byte[] propBytes = new byte[(int) propsLen];
                dis.read(propBytes);
                ByteArrayInputStream bstream = new ByteArrayInputStream(propBytes);
                ObjectInputStream ostream = new ObjectInputStream(bstream);
                netMsg.props = (CollectionContainer) ostream.readObject();
                ostream.close();
            }
            if (netMsg.messagePayloadType == JetStreamNetMessage.STRING_OBJ) //netMsg.text = dis.readUTF();
            {
                netMsg.text = readString(dis);
            }
            else {
                int sz = dis.readInt();
                if (sz > 0) {
                    netMsg.bytes = new byte[sz];
                    int read = dis.read(netMsg.bytes);
                }
                else {
                    netMsg.bytes = null;
                }
            }

            return netMsg;
        }
        catch (EOFException e) {
            logger.log(Level.SEVERE, "Exception", e);
        }

        return null;
    }

    private void writeString(DataOutputStream dos, String str) throws IOException {
        int length = str.length();

        dos.writeInt(length);
        if (length > 0) {
            dos.writeChars(str);
        }
    }

    private String readString(DataInputStream dis) throws IOException {
        int res = dis.readInt();

        // No data to retrieve.  Used to signal terminating connection.
        if (res == 0) {
            return "";
        }

        StringBuffer buf = new StringBuffer(res);
        for (int i = 0; i < res; i++) {
            buf.append(dis.readChar());
        }

        String result = buf.toString();
        return result;
    }

    public JetStreamSocketClient() throws Exception {
        logger.setParent(jetStream.getLogger());
        initClient(null, false);
    }

    public JetStreamSocketClient(Socket conn) throws Exception {
        logger.setParent(jetStream.getLogger());
        initClient(conn, false);
    }

    public JetStreamSocketClient(String ipAddress, boolean stealthConnection) throws Exception {
        logger.setParent(jetStream.getLogger());
        this.hostAddr = ipAddress;

        if (JetStream.fineLevelLogging) {
            StringBuffer sb = new StringBuffer();
            sb.append("Connecting to server");
            sb.append("\n Addr: ");
            sb.append(hostAddr);
            sb.append("\n on port ");
            sb.append(JetStream.SERVER_SOCKET);
            logger.info(sb.toString());
        }

        Socket conn = new Socket(ipAddress, JetStream.SERVER_SOCKET);
        conn.setTcpNoDelay(true);
        initClient(conn, stealthConnection);
    }

    public JetStreamSocketClient(InetAddress ipAddress) throws Exception {
        logger.setParent(jetStream.getLogger());
        this.ipaddr = ipAddress;
        this.hostAddr = ipAddress.getHostAddress();
        this.hostName = hostAddr;//ipAddress.getHostName();

        if (JetStream.fineLevelLogging) {
            StringBuffer sb = new StringBuffer();
            sb.append("Connecting to server");
            sb.append("\n Name: ");
            sb.append(hostName);
            sb.append("\n Addr: ");
            sb.append(hostAddr);
            sb.append("\n on port ");
            sb.append(JetStream.SERVER_SOCKET);
            logger.info(sb.toString());
        }

        Socket conn = new Socket(this.ipaddr, JetStream.SERVER_SOCKET);
        conn.setTcpNoDelay(true);
        initClient(conn, false);
    }

    protected void initClient(Socket conn, boolean stealthConnection)
            throws Exception {
        setStealthConnection(stealthConnection);

        // If no client connection was supplied, try to connect to
        // the server on port 6969
        //
        if (conn == null) {
            if (JetStream.fineLevelLogging) {
                StringBuffer sb = new StringBuffer();
                sb.append("Connecting to server ");
                sb.append(JetStream.SERVER_HOSTNAME);
                sb.append(" on port ");
                sb.append(JetStream.SERVER_SOCKET);
                logger.info(sb.toString());
            }

            try {
                conn = new Socket(JetStream.SERVER_HOSTNAME,
                        JetStream.SERVER_SOCKET);
                conn.setTcpNoDelay(true);
            }
            catch (Exception e) {
                logger.log(Level.SEVERE, "Exception opening socket", e);
                throw e;
            }
        }

        port = conn.getPort();
        localPort = conn.getLocalPort();
        ipaddr = conn.getInetAddress();
        hostAddr = ipaddr.getHostAddress();
        hostName = ipaddr.getHostAddress();//ipaddr.getHostName();

        if (JetStream.fineLevelLogging) {
            StringBuffer sb = new StringBuffer();
            sb.append("Client connection on port=");
            sb.append(port);
            sb.append(", localPort=");
            sb.append(localPort);
            sb.append(", clientObj: ");
            sb.append(this.toString());
            logger.info(sb.toString());
        }

        // Start a listener thread that checks for new
        // net messages. When a new message arrives
        // fire a JavaBean event
        //
        listener = new JetStreamListener(conn);

        // Start Sender thread that checks the outbound
        // message queue for message transmission
        //
        sender = new JetStreamSender(conn);

        StringBuffer sb = new StringBuffer();
        if (hostAddr.equals("127.0.0.1")) {
            remoteClient = false;
            sb.append("Local client connected");
        }
        else {
            remoteClient = true;
            sb.append("Remote client connected with address ");
            sb.append(hostAddr);

            // Store this connection in a server list
            jetStream.addServer(this);
        }

        sb.append(", port=");
        sb.append(port);
        sb.append(", localPort=");
        sb.append(localPort);
        logger.info(sb.toString());

        if (JetStream.fineLevelLogging) {
            logger.info("JetStreamSocketClient: ready");
        }
    }

    public boolean sendNetMsg(Object obj) {
        //logger.trace();
        if (JetStream.fineLevelLogging) {
            logger.info("JetStreamSocketClient:sendNetMsg() with object: " + obj);
        }

        if (sender.readyToSend) {
            try {
                sender.sendMsg(obj);
                return true;
            }
            catch (Exception e) {
                logger.log(Level.SEVERE, "Exception", e);
            }
        }
        else {
            logger.info("JetStreamSocketClient.sendNetMsg() - sender not in right state");
        }
        return false;
    }

    public boolean sendNetMsg(byte[] bytes) {
        //logger.trace();
        if (JetStream.fineLevelLogging) {
            logger.info("JetStreamSocketClient:sendNetMsg() with bytes");
        }

        if (sender.readyToSend) {
            try {
                sender.sendMsg(bytes);
                return true;
            }
            catch (Exception e) {
                logger.log(Level.SEVERE, "Exception", e);
            }
        }
        else {
            logger.info("JetStreamSocketClient.sendNetMsg() - sender not in right state");
        }
        return false;

    }

    public void close() {
        try {
            if (listener != null) {
                listener.interrupt();
            }
        }
        catch (Exception e) {
        }

        sender.readyToSend = false;
        listener.listening = false;
        closed = true;
    }
    private boolean closed = false;

    public boolean isClosed() {
        return closed;
    }

    private void onClientLinkLost() {
        //logger.trace();
        try {
            closed = true;
            if (JetStream.fineLevelLogging) {
                StringBuffer sb = new StringBuffer();
                if (this.remoteClient) {
                    sb.append("Cleaning up for remote client. Details:");
                }
                else {
                    sb.append("Cleaning up for local client. Details:");
                }
                sb.append("\n  Name: ");
                sb.append(this.hostName);
                sb.append("\n  Addr: ");
                sb.append(this.hostAddr);
                sb.append("\n  on port ");
                sb.append(this.port);
                logger.info(sb.toString());
            }

            // Stop the listener loop, and indicate no more sending
            //
            listener.listening = false;
            sender.readyToSend = false;

            // Ensure connections are closed
            //
            try {
                listener.conn.close();
            }
            catch (Exception e) {
            }
            try {
                sender.conn.close();
            }
            catch (Exception e) {
            }

            // Clean up
            if (!isStealthConnection()) {
                jetStream.removeNetClient(this);
            }

            // Interrupt the listening thread
            //
            try {
                if (listener != null) {
                    listener.interrupt();
                }
            }
            catch (Exception e) {
            }
        }
        catch (Exception e) {
        }
    }

    private void fire_onNewHighAvailStoreMessage(
            String destName, JetStreamNetMessage netMsg,
            long netMsgId, String hostName, String hostAddr) {
        //logger.trace();
        JetStreamMessage msg;
        if (netMsg.messagePayloadType == JetStreamNetMessage.STRING_OBJ) {
            msg = new JetStreamTextMessage(netMsg);
        }
        else if (netMsg.messagePayloadType == JetStreamNetMessage.CUSTOM_OBJ) {
            msg = new JetStreamObjectMessage(netMsg);
        }
        else {
            msg = new JetStreamMapMessage(netMsg);
        }

        jetStream.ha.onHighAvailStorageMessage(destName, msg, netMsgId, hostName, hostAddr);
    }
}
