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

import com.allure.JetStream.admin.JetStreamAdminServer;
import com.allure.JetStream.bridges.MQTTBridge;
import com.allure.JetStream.jndi.*;
import com.allure.JetStream.network.*;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.*;
import util.*;

public class JetStream {
    private int majorVer = 1;
    private int minorVer = 5;
    private int subVer = 0;

    private static final JetStream INSTANCE = new JetStream();
    private static Logger logger = Logger.getLogger("JetStream");

    public Logger getLogger() {
        if (logger == null) {
            Logger.getLogger("JetStream");
        }
        return logger;
    }
    public static int adminPort = 0;
    private JetStreamJNDI jndi = null;
    public static final int SERVER_SOCKET = 5150;
    public static final String SERVER_HOSTNAME = "localhost";
    public static long startTime = System.currentTimeMillis();
    // Known message Destinations
    public final HashMap<String, JetStreamTopic> _topics =
            new HashMap<String, JetStreamTopic>();
    public final HashMap<String, JetStreamQueue> _queues =
            new HashMap<String, JetStreamQueue>();
    // Network address information for this server
    public InetAddress addr = null;
    public String localIPName = "";
    public String localIPAddr = "";
    // Network message processing
    public JetStreamNetMessageHandler netMsgHandler = null;//
    public JetStreamNetMessageSender netMsgSender = null;//
    // Store all client connections, local and remote
    protected Vector<JetStreamSocketClient> _netClients =
            new Vector<JetStreamSocketClient>();

    // Track remote server connections by IP address
    public class RemoteServer {

        public JetStreamSocketClient link = null;
        public JetStreamSocketClient haLink = null;
        public boolean online = false;
        public boolean recovered = false; // If this server
        public HashMap<String, Persistence> journals = null;
    }
    protected HashMap<String, RemoteServer> serversByAddress =
            new HashMap<String, RemoteServer>();
    // High Availability message storage implementation
    public JetStreamHA ha = null;
    private JetStreamServer server = null;
    private static boolean serverAllowed = true;
    protected RemoteClientManager rcm = null;
    protected static boolean isolatedMode = false;
    // Message persistence and recovery related variables
    protected JetStreamMessageRecovery messageRecovery = null;
    protected boolean recoverMessages = true;
    public static boolean dbPersist = false;
    public static String ROOT_FOLDER = null;
    public static String QUEUES_FOLDER = null;
    public static String TOPICS_FOLDER = null;
    public static String HA_FOLDER = null;
    public static boolean highAvailOn = true;
    // Turn heartbeat processing on/off
    private boolean enableHeartbeats = false;
    private static long messageSequence = 0;
    private static final Object seqNumLock = new Object();
    private static long netMsgSequence = 0;
    private static final Object netMsgSeqLock = new Object();
    // Logger related flags
    public static int LOG_FILE_SIZE = 100000000; // 100 MB
    public static int LOG_FILE_ROLLOVER_COUNT = 30;
    public static boolean LOG_FILE_APPEND = true;
    public static String LOG_FILE_PATH_NAME = "jetstream%u.%g.log";
    public static boolean fineLevelLogging = false;
    public static boolean traceOn = false;
    public static boolean fTimeStamp = false;
    public static boolean writeToFile = true;
    public static boolean writeToStdOut = false;
    public static int QUEUE_CACHE_SIZE = 1000000;
    public static boolean compress = true; // turn compression on or off
    public static boolean compressSpeed = true; // compression speed vs. size
    public static int COMPRESS_THRESHOLD = 1024; // message size in bytes
    // Used to calculate queue message send rate
    private static final int QMSGS_PER_SECOND = 10000;
    private int queueMsgCount = 0;
    private long qSendStartTime = 0;
    private long qSendEndTime = 0;
    
    ///////////////////////////////////////////////////////////////////////////
    // QUEUE SCHEDULING
    private static final LinkedList<JetStreamQueue> toBeSent =
            new LinkedList<JetStreamQueue>();
//    private static final ConcurrentLinkedQueue<JetStreamQueue> toBeSent = 
//            new ConcurrentLinkedQueue<JetStreamQueue>();
    // Queue schedulers are server threads that send messages to clients
    private Vector<JetStreamQueueScheduler> queueSchedulers =
            new Vector<JetStreamQueueScheduler>();
    private static int MAX_QUEUE_SENDERS = 8; // command-line param overridden

    protected static void processQueue(JetStreamQueue queue) {
        synchronized (toBeSent) {
            // Queue the request and notify a waiting thread in the pool
            toBeSent.add(queue);
            toBeSent.notify();
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // PUBSUB SCHEDULING
    protected class PubSubData {
        public PubSubData(byte[] data, JetStreamSocketClient link) {
            this.data = data;
            this.link = link;
        }
        byte[] data;
        JetStreamSocketClient link;
    }
    private static final LinkedList<PubSubData> pubSubMessages =
            new LinkedList<PubSubData>();
    // Topic schedulers are server threads that send messages to clients
    private Vector<JetStreamTopicScheduler> topicSchedulers =
            new Vector<JetStreamTopicScheduler>();
    private static int MAX_PUBSUB_SENDERS = 8;
    private Object throttleLock = new Object();
    private byte throttleLockSet = 0;
    private static int PUBSUB_THROTTLE_COUNT = 1000000;

    protected static void processTopic() {
        synchronized (pubSubMessages) {
            pubSubMessages.notify();
        }
    }
    public Timer remoteSendTimer = null;

    class SendTimeout extends TimerTask {

        @Override
        public void run() {
            // Iterate through the queues and have them check for
            // expired send requests
            //
            synchronized (_queues) {
                for (JetStreamQueue queue : _queues.values()) {
                    queue.checkForExpiredSends();
                }
            }
        }
    }

    class JetStreamTopicScheduler extends Thread {

        public JetStreamTopicScheduler() {
            this.setName("JetStreamTopicScheduler");
            start();
        }

        @Override
        public void run() {
            try {
                while (true) {
                    PubSubData pubsubData = null;
                    synchronized (pubSubMessages) {
                        while (pubSubMessages.isEmpty()) {
                            pubSubMessages.wait();
                        }

                        pubsubData = pubSubMessages.removeFirst();
                    }

                    // Since the client link reference is put in a list
                    // that may last some time, check that it's still connected
                    if (!pubsubData.link.isClosed()) {
                        pubsubData.link.sendNetMsg(pubsubData.data);
                    }

                    // Let the senders send
                    if (throttleLockSet > 0) {
                        synchronized (throttleLock) {
                            if (pubSubMessages.size()
                                    < (PUBSUB_THROTTLE_COUNT - 500)) {
                                throttleLockSet = 0;
                                throttleLock.notifyAll();
                            }
                        }
                    }
                }
            }
            catch (InterruptedException e) {
            }
        }
    }
    public static int QSnum = 1;

    class JetStreamQueueScheduler extends Thread {

        public boolean fKeepRunning = true;

        public JetStreamQueueScheduler() {
            this.setName("JetStreamQueueScheduler-" + (QSnum++));
            start();
        }
        public final static String SHUTDOWN_MSG =
                "JetStreamQueueScheduler shutting down";

        @Override
        public void run() {
            // Loop, waiting on an object that is signaled
            // when a message is enqueued to be sent
            //
            while (fKeepRunning) {
                try {
                    JetStreamQueue jsQueue = null;

                    synchronized (toBeSent) {
                        while (toBeSent.isEmpty()) {
                            toBeSent.wait();
                        }

                        jsQueue = toBeSent.removeFirst();
                        //jsQueue = toBeSent.remove();
                    }

                    jsQueue.deliverMessage();
                }
                catch (InterruptedException ie) {
                    // Allow the thread to shut down
                    fKeepRunning = false;
                }
                catch (Exception e) {
                    // Log the Exception but keep running
                    logger.log(Level.SEVERE, "Exception:", e);
                }
            }

            // Orderly shutdown
            if (JetStream.fineLevelLogging) {
                logger.info(SHUTDOWN_MSG);
            }
        }
    }

    public static boolean isIsolatedMode() {
        return isolatedMode;
    }

    protected void createJournalFolders() {
        //
        // Create folders to store persitent messages. If they cannot
        // be created, we must log an error and terminate
        //
        boolean created = false;
        try {
            if (ROOT_FOLDER == null) {
                readConfig();
            }

            //if (JetStream.fineLevelLogging)
            logger.info("Creating root message persistence folders in " + ROOT_FOLDER);
            //System.out.println("Creating root message persistence folders in " + ROOT_FOLDER);

            File path = null;
            path = new File(ROOT_FOLDER);
            created = path.mkdirs();
            if (!created && !path.exists()) {
                created = false;
            }
            else {
                path = new File(HA_FOLDER);
                created = path.mkdirs();
                if (!created && !path.exists()) {
                    return;
                }

                path = new File(QUEUES_FOLDER);
                created = path.mkdirs();
                if (!created && !path.exists()) {
                    return;
                }

                path = new File(TOPICS_FOLDER);
                created = path.mkdirs();
                if (!created && !path.exists()) {
                    return;
                }

                created = true;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (!created) {
                logger.severe("Unable to create message persistence folder!");
                //System.out.println("Unable to create message persistence folder!");
            }
        }
    }
    ///////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////
    JetStreamAdminServer webserver = null;

    class JetStreamServer extends Thread {

        public boolean listening = false;
        private String host;
        private int port;
        private ServerSocket clientConnect = null;
        public JetStreamHeartbeat heartbeat = null;

        class JetStreamHeartbeat extends Thread {

            static final int HEARTBEAT_INTERVAL = 1000; // one second
            static final int CHECK_PARTNERS_COUNT = 2;
            int checkPartnersCount = CHECK_PARTNERS_COUNT;

            public JetStreamHeartbeat() {
                this.setName("JetStreamHeartbeat");
            }

            @Override
            public void run() {
                // Wait one interval before starting heartbeats
                //try { sleep(HEARTBEAT_INTERVAL); } catch ( Exception e ) { }

                while (listening) {
                    if (serversByAddress != null
                            && !serversByAddress.isEmpty()) {
                        // Iterate through servers and send hearbeat to each
                        //
                        HashMap<String, RemoteServer> serversToCheck;
                        synchronized (serversByAddress) {
                            serversToCheck =
                                    (HashMap<String, RemoteServer>) serversByAddress.clone();
                        }

                        for (RemoteServer svr : serversToCheck.values()) {
                            if (svr.online) {
                                netMsgSender.sendHeartbeat(svr.link);
                            }
                        }

                        // If the counter has reached zero, check that we have
                        // received heartbeats from all other servers
                        //
                        checkPartnersCount--;
                        if (checkPartnersCount <= 0) {
                            checkPartnersCount = CHECK_PARTNERS_COUNT;
                            checkPartners(serversToCheck);
                        }
                    }

                    // Wait before sending next heartbeat
                    try {
                        sleep(HEARTBEAT_INTERVAL);
                    }
                    catch (InterruptedException ie) {
                        return;
                    }
                    catch (Exception e) {
                    }
                }
            }

            private void checkPartners(HashMap<String, RemoteServer> serversToCheck) {
                try {
                    if (serversByAddress.isEmpty()) {
                        return;
                    }

                    // Check how long it has been since a heartbeat has been received
                    // for each partner. If the period is greater than the heartbeat
                    // send interval multiplied by the value of CHECK_PARTNERS_COUNT
                    // then consider it a failure
                    //
                    long failureTime = HEARTBEAT_INTERVAL * CHECK_PARTNERS_COUNT;
                    long currentTime = System.currentTimeMillis();

                    // Iterate through the servers and check each
                    //
                    for (RemoteServer server : serversToCheck.values()) {
                        if (!server.online) {
                            continue;
                        }

                        // Received a heartbeat within the timeout period?
                        long serverTime = server.link.lastHeartbeat;
                        if (serverTime == 0) {
                            // Perhaps the server just started right before
                            // it was time to check partners? Give the server
                            // another chance to send heartbeats before the
                            // next check by setting the heartbeat time to 1
                            server.link.lastHeartbeat = 1;
                            continue;
                        }

                        long diff = currentTime - serverTime;
                        if (diff < 0) {
                            if (JetStream.fineLevelLogging) {
                                logger.info("Negative heartbeat diff: " + diff);
                            }
                            continue; // Sometimes the heartbeat is in the future
                        }

                        if (diff > failureTime) {
                            if (JetStream.fineLevelLogging) {
                                logger.info("Heartbeat timeout: cleaning up");
                            }
                            handleFailedClient(server.link);
                        }

                    }
                }
                catch (Exception e) {
                    logger.log(Level.SEVERE, "Exception:", e);
                }
            }
        }

        private JetStreamServer() {
        }

        public JetStreamServer(String host, int port) {
            this.host = host;
            this.port = port;
            this.setName("JetStreamServer");
            readConfig();
        }

        public void shutdown() {
            listening = false;
        }

        // BRIDGES
        org.eclipse.moquette.server.Server mqttServer = null;
        MQTTBridge mqttBridge = null;
                
        @Override
        public void run() {
            try {
                jndi = JetStreamJNDI.getInstance();
                ha = new JetStreamHA();

                if (host == null) {
                    System.out.println("Host is null");
                }

                // Attempt to become the server unless told not to
                if (serverAllowed) {
                    clientConnect = new ServerSocket(port);
                    listening = true;
                    addr = clientConnect.getInetAddress();
                    String hostName = addr.getHostAddress();//addr.getHostName();
                    String hostAddr = addr.getHostAddress();

                    //
                    // MQTT Support
                    //
                    mqttServer = org.eclipse.moquette.server.Server.createAndStart();
                    mqttBridge = new MQTTBridge();
                    
                    //
                    // COAP Support
                    //
                    
                    //
                    // XMPP Support
                    //
                    
                }
                else {
                    logger.info("Command-line param set to NOT become server");
                }
            }
            catch (Exception e) {
                if (fineLevelLogging) {
                    logger.info("JetStreamServer: Another listener already on this port");
                }
                listening = false;
            }

            onListening(listening);

            if (listening == false) {
                return;
            }

            if (isServer() == true) {
                if (!isolatedMode) {
                    rcm = new RemoteClientManager();
                }
                else {
                    System.out.println("WARNING: JETSTREAMQ IN ISOLATED MODE");
                }

                // Create folders to store persitent messages. If they cannot
                // be created, we must log an error and terminate
                //
                createJournalFolders();

                if (enableHeartbeats) {
                    // Start sending hearbeats
                    //
                    this.heartbeat = new JetStreamHeartbeat();
                    this.heartbeat.setPriority(Thread.MAX_PRIORITY);
                    this.heartbeat.start();
                }

                // Create admin server
                //
                try {
                    logger.info("HTTP Admin Port=" + adminPort);
                    webserver = new JetStreamAdminServer(adminPort);
                    webserver.start();
                }
                catch (Exception e) {
                    logger.log(Level.SEVERE, "Exception", e);
                }

                // Recover undelivered messages
                //
                messageRecovery = new JetStreamMessageRecovery(ROOT_FOLDER);
            }

            logger.info("JetStreamServer: Listening for clients");

            while (listening) {
                try {
                    clientConnect.setSoTimeout(1000);
                    Socket clientReq = null;
                    try {
                        clientReq = clientConnect.accept();
                    }
                    catch (SocketTimeoutException ste) {
                        if (listening) {
                            continue;
                        }
                        return;
                    }

                    if (clientReq == null) {
                        continue;
                    }

                    clientReq.setTcpNoDelay(true);
                    InetAddress clientAddr = clientReq.getInetAddress();
                    String hostName = clientAddr.getHostAddress();//clientAddr.getHostName();
                    String hostAddr = clientAddr.getHostAddress();

                    if (JetStream.fineLevelLogging) {
                        StringBuffer sb = new StringBuffer();
                        sb.append("JetStreamServer: new client connected");
                        sb.append("\n Name=");
                        sb.append(hostName);
                        sb.append("\n Addr=");
                        sb.append(hostAddr);
                        logger.info(sb.toString());
                    }

                    JetStreamSocketClient client = new JetStreamSocketClient(clientReq);

                    // Only add this to the
                    if (!client.isStealthConnection()) {
                        addNetClient(client);
                    }
                }
                catch (Exception e) {
                    listening = false;
                }
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    @SuppressWarnings("static-access")
    private JetStream() {
        readConfig();

        try {
            if (logger == null) {
                logger = Logger.getLogger("JetStream");
            }

            if (writeToFile) {
                FileHandler fh = new FileHandler(LOG_FILE_PATH_NAME,
                        LOG_FILE_SIZE,
                        LOG_FILE_ROLLOVER_COUNT,
                        LOG_FILE_APPEND);
                logger.addHandler(fh);
                if (fineLevelLogging) {
                    logger.setLevel(Level.ALL);
                }
                else {
                    logger.setLevel(Level.INFO);
                }

                SimpleFormatter formatter = new SimpleFormatter();
                fh.setFormatter(formatter);
            }

            if (!writeToStdOut) {
                // Iterate through list of handlers and remove command line
                Logger l = logger;
                while (l != null) {
                    Handler[] handlers = l.getHandlers();
                    for (Handler hand : handlers) {
                        if (hand instanceof ConsoleHandler) {
                            l.removeHandler(hand);
                        }
                    }

                    l = l.getParent();
                }
            }

        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
            System.out.println("Could not configure logger and/or log file");
        }

        netMsgHandler = new JetStreamNetMessageHandler(this);
        netMsgSender = new JetStreamNetMessageSender(this);


        OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
        RuntimeMXBean rtBean = ManagementFactory.getRuntimeMXBean();
        List<String> args = rtBean.getInputArguments();
        Runtime rt = Runtime.getRuntime();

        StringBuffer sb = new StringBuffer();
        sb.append(" ");
        sb.append("\n***************************************************");
        sb.append("\nJetStreamQ v").append(getVersion());
        sb.append("\nCopyright (c) 2010-2015 Allure Technology, Inc. All Rights Reserved.");
        sb.append("\nwww.alluretechnology.com");
        sb.append("\nJetStreamQ is free software: you can redistribute it and/or modify");
        sb.append("\nit under the terms of the GNU General Public License as published by");
        sb.append("\nthe Free Software Foundation, version 2 of the License.");
        sb.append("\n***************************************************");
        sb.append("\nStarted ").append((new Date()).toString());
        sb.append("\nJournal home folder: ").append(this.ROOT_FOLDER);
        sb.append("\nUser home folder: ").append(System.getProperty("user.home"));
        sb.append("\n***************************************************");
        sb.append("\nOS: ").append(System.getProperty("os.name")).append(", ").append(System.getProperty("os.arch")).append(", ").append(System.getProperty("os.version"));
        sb.append("\nAvailable CPUs/cores: ").append(osBean.getAvailableProcessors());
        sb.append("\nJava runtime version: ").append(System.getProperty("java.version"));
        sb.append("-").append(System.getProperty("java.vm.version"));
        sb.append("\nClasspath: ").append(rtBean.getClassPath());
        sb.append("\nArguments: ");
        for (String arg : args) {
            sb.append(arg).append(", ");
        }
        sb.append("\n--------------------------------------------------");
        sb.append("\nHeap mem: ").append(memBean.getHeapMemoryUsage());
        sb.append("\nOther mem: ").append(memBean.getNonHeapMemoryUsage());
        sb.append("\n--------------------------------------------------");
        sb.append("\nTotal mem: ").append(rt.totalMemory());
        sb.append("\nFree mem: ").append(rt.freeMemory());
        sb.append("\nMax mem: ").append(rt.maxMemory());
        sb.append("\n***************************************************");
        logger.info(sb.toString());

        startServer();
    }

    public String getVersion() {
        return majorVer + "." + minorVer + "." + subVer;
    }

    public boolean isServer() {
        return server.listening;
    }

    protected void setLog(boolean log) {
        JetStream.fineLevelLogging = log;
    }

    protected boolean getMessageRecoveryFlag() {
        return recoverMessages;
    }

    protected void setMessageRecoveryFlag(boolean flag) {
        this.recoverMessages = flag;
    }

    protected static void setMessageFolders() {
        // Next, set it to the default location
        if (ROOT_FOLDER == null || ROOT_FOLDER.length() == 0) {
            ROOT_FOLDER = System.getProperty("user.home") + File.separator + "jsjournal";
        }

        QUEUES_FOLDER = ROOT_FOLDER + File.separator + "queues";
        TOPICS_FOLDER = ROOT_FOLDER + File.separator + "topics";
        HA_FOLDER = ROOT_FOLDER + File.separator + "ha";
    }
    static String uuid = null;

    protected String getNewMessageID() {
        synchronized (seqNumLock) {
            if (uuid == null) {
                byte[] uuidRoot = ("" + localIPAddr + startTime).getBytes();
                uuid = UUID.nameUUIDFromBytes(uuidRoot).toString();
                logger.info("This server's UUID: " + uuid);
            }

            StringBuffer sbMsgId = new StringBuffer(
                    formatMessageId(++messageSequence));
            sbMsgId.append(".");
            sbMsgId.append(uuid);
            //sbMsgId.append(UUID.randomUUID().toString());
            return sbMsgId.toString();
        }
    }

    protected boolean dispatchPubSubMessage(byte[] bytes, JetStreamSocketClient client) {
        PubSubData psd = new PubSubData(bytes, client);
        synchronized (pubSubMessages) {
            pubSubMessages.addLast(psd);
            if (pubSubMessages.size() > PUBSUB_THROTTLE_COUNT) {
                throttleLockSet = 1;
                return true;
            }
        }
        return false;
    }

    protected long getNetMsgSeqNumber() {
        //synchronized (netMsgSeqLock) 
        {
            return ++netMsgSequence;
        }
    }

    private String formatMessageId(long msgId) {
        if (msgId < 10) {
            return "000000000" + msgId;
        }
        if (msgId < 100) {
            return "00000000" + msgId;
        }
        if (msgId < 1000) {
            return "0000000" + msgId;
        }
        if (msgId < 10000) {
            return "000000" + msgId;
        }
        if (msgId < 100000) {
            return "00000" + msgId;
        }
        if (msgId < 1000000) {
            return "0000" + msgId;
        }
        if (msgId < 10000000) {
            return "000" + msgId;
        }
        if (msgId < 100000000) {
            return "00" + msgId;
        }
        if (msgId < 1000000000) {
            return "0" + msgId;
        }
        return "" + msgId;
    }

    private void startServer() {
        try {
            // Start the link listener
            server = new JetStreamServer(SERVER_HOSTNAME, SERVER_SOCKET);
            server.setPriority(Thread.MAX_PRIORITY);
            server.start();
            Thread.yield();
        }
        catch (Exception e) {
        }
    }

    protected void shutdown() {
        if (isServer()) {
            server.shutdown();

            for (JetStreamQueueScheduler queueSched : queueSchedulers) {
                try {
                    queueSched.interrupt();
                }
                catch (Exception e) {
                }
            }

            for (JetStreamTopicScheduler topicSched : topicSchedulers) {
                try {
                    topicSched.interrupt();
                }
                catch (Exception e) {
                }
            }

            webserver.shutdown();

            this.jndi.getInstance().stopJNDIServer();

            this.remoteSendTimer.cancel();

            this.rcm.shutdown();
        }
    }

    protected boolean connectRemoteClient(InetAddress IPAddress) {
        try {
            JetStreamSocketClient socketClient =
                    new JetStreamSocketClient(IPAddress);
            addNetClient(socketClient);

            // Create a dedicated HA connection
            RemoteServer remote =
                    getServersByAddress().get(IPAddress.getHostAddress());
            if (remote.haLink == null) {
                logger.info("Creating a dedicated HA connection");
                remote.haLink = new JetStreamSocketClient(remote.link.hostAddr, true);
                remote.haLink.setStealthConnection(true);
            }

            return true;
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
        }

        return false;
    }

    protected synchronized void onListening(boolean listening) {
        readConfig();

        if (JetStream.fineLevelLogging) {
            logger.fine("onListening called");
        }

        // If the listener could not bind to the port, that means that
        // another JetStream client already has and is currently the server
        // In this case, just start an JetStreamSocketClient and connect.
        //
        if (!listening) {
            // Try to connect a few times before giving up
            //
            for (int tries = 0; tries < 4; tries++) {
                try {
                    JetStreamSocketClient socketClient = new JetStreamSocketClient();
                    if (socketClient != null) {
                        addNetClient(socketClient);
                        break;
                    }
                }
                catch (Exception e) {
                    logger.log(Level.SEVERE, "Exception conncting to server:", e);
                }
            }
        }

        if (server.listening) {
            //
            // This instance is the server
            //

            // Create the queue scheduler. This class ensures that
            // all queues get messages delivered in a reasonably
            // distributed manner. No queue should starve other
            // queues due to a high-rate of message sending.
            //
            for (int i = 0; i < MAX_QUEUE_SENDERS; i++) {
                queueSchedulers.add(new JetStreamQueueScheduler());
            }

            for (int i = 0; i < MAX_PUBSUB_SENDERS; i++) {
                topicSchedulers.add(new JetStreamTopicScheduler());
            }

            // Start a queue message send timeout timer that will check
            // a list of outstanding queue message send requests and tick
            // them down until either a valid response is recived, or
            // they expire
            if (JetStream.fineLevelLogging) {
                logger.info("starting remoteSendTimer");
            }

            remoteSendTimer = new Timer("QueueSenderTimeoutTimer");
            remoteSendTimer.schedule(new SendTimeout(), 500, 1000);

            logger.info("Server listening for clients");
        }
    }

    protected void enQueueMessage(String queueName, JetStreamMessage msg,
            long netMsgId, int port, int localPort) {
        //logger.fine("enter");

        try {
            // The following call to createQueue will return the queue if
            // it exists already, or create it if not
            JetStreamQueue jsQueue = (JetStreamQueue) createQueue(queueName);
            jsQueue.midFlight = true;
            jsQueue.send(msg, netMsgId, port, localPort);
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
        }
    }

    protected void addNetClient(JetStreamSocketClient client) {
        //logger.trace();

        if (JetStream.fineLevelLogging) {
            logger.info("Adding client: " + client.hostName);
        }

        synchronized (_netClients) {
            _netClients.addElement(client);
        }

        // Add new client to all queues
        //
        synchronized (_queues) {
            for (JetStreamQueue queue : _queues.values()) {
                // Add the new client to front of list if local, otherwise
                // add to the tail of the list
                if (client.remoteClient) {
                    if (JetStream.fineLevelLogging) {
                        logger.info(("Remote client - adding to tail of queue listeners"));
                    }
                    queue.clientList.add(client);
                }
                else {
                    if (JetStream.fineLevelLogging) {
                        logger.info(("Local client - adding to head of queue listeners"));
                    }
                    queue.clientList.add(0, client);
                }

                // force the queue to be reoptimized, which ensures that
                // for each queue, the last net client to receive a queue
                // message will be the first one tried on subsequent
                // queue message sends
                queue.setOptimized(false);

                if (JetStream.fineLevelLogging) {
                    logger.info("Added client to list for queue " + queue.name);
                }

                // Start delivery of any waiting messages
                JetStream.processQueue(queue);
            }
        }
    }

    public void onServerFailure(JetStreamSocketClient failed) {
        StringBuffer sb = new StringBuffer();
        sb.append("Remote server failed:");
        sb.append("\n Name: ");
        sb.append(failed.hostName);
        sb.append("\n Addr: ");
        sb.append(failed.hostAddr);
        sb.append("\n Time since last heartbeat: ");
        sb.append(System.currentTimeMillis() - failed.lastHeartbeat);
        logger.info(sb.toString());

        RemoteServer remote = null;

        try {
            // Mark the server as offline
            synchronized (serversByAddress) {
                remote = serversByAddress.get(failed.hostAddr);
                remote.online = false;
                remote.link = null;
                remote.haLink = null;
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
        }

        // The server with the smallest start time of the remaining servers
        // will take over for the failed server
        //
        long youngest = -1;
        Collection<RemoteServer> coll = serversByAddress.values();
        for (RemoteServer svr : coll) {
            if (svr.link == null) {
                continue;
            }

            // Was the other server's start time less than the failed server?
            if (youngest == -1 || svr.link.serverStartTime < youngest) {
                youngest = svr.link.serverStartTime;
            }
        }

        // Now compare ours with the youngest of the others. If youngest
        // is -1, it means there are no other servers running now
        if (youngest == -1 || startTime < youngest) {
            if (!remote.recovered) {
                // We need to recover for the failed server
                remote.recovered = true;
                ha.recoverFailedServer(failed);
            }
        }
    }

    public void removeNetClient(JetStreamSocketClient client) {
        if (JetStream.fineLevelLogging) {
            logger.info("Removing client: " + client.hostName);
        }

        synchronized (_netClients) {
            _netClients.remove(client);
        }

        if (isServer()) {
            synchronized (serversByAddress) {
                // Check to see if a remote server failed
                if (serversByAddress.containsKey(client.hostAddr)) {
                    // Handle a server failure
                    onServerFailure(client);
                }
            }
        }
        else if (!server.listening) {
            // This link connection may have been to the server
            // so try to grab the server port in an attempt to
            // take over for the server
            //
            startServer();
        }

        removeClientFromQueues(client);
        removeClientFromTopics(client);
    }

    public void removeClientFromQueues(JetStreamSocketClient client) {
        // Notify all queues of a lost client
        //
        if (_queues.size() > 0) {
            Collection qColl;
            Iterator qIter;
            synchronized (_queues) {
                qColl = _queues.values();
                qIter = qColl.iterator();
            }

            while (qIter.hasNext()) {
                JetStreamQueue q = (JetStreamQueue) qIter.next();
                q.optimized = false;
                synchronized (q.clientList) {
                    int count = q.clientList.size();
                    for (int i = 0; i < count; i++) {
                        JetStreamSocketClient nextClient = null;

                        try {
                            nextClient =
                                    (JetStreamSocketClient) q.clientList.elementAt(i);
                        }
                        catch (Exception e) {
                            logger.log(Level.SEVERE, "Exception:", e);
                        }

                        if (nextClient == null) {
                            continue;
                        }

                        if (nextClient.port == client.port
                                && nextClient.localPort == client.localPort) {
                            q.clientList.remove(i);

                            // force the queue to be reoptimized, which ensures that
                            // for each queue, the last net client to receive a queue
                            // message will be the first one tried on subsequent
                            // queue message sends
                            q.setOptimized(false);

                            if (JetStream.fineLevelLogging) {
                                logger.info("Removed client for queue " + q.name);
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    private void removeClientFromTopics(JetStreamSocketClient client) {
        // Notify all topics of a lost client. This will activate
        // the durable subscriptions and saved any missed messages
        //
        System.out.println("****** removeClientFromTopics()");
        if (_topics.size() > 0) {
            synchronized (_topics) {
                for (JetStreamTopic topic : _topics.values()) {
                    topic.removeDurableSubscriber(client);
                }
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // JetStream Public Methods
    ///////////////////////////////////////////////////////////////////////////
    public static JetStream getInstance() {
        return INSTANCE;
    }

    // This is a directed message sent to inform this server that another
    // server recovered our messages while we were down. Therefore, we
    // should abandon all persisted queued messages
    // AND POSSIBLY ALL HA MESSAGES?
    public void onMessagesRecovered(String recoveryAddr) {
        this.recoverMessages = false;
    }

    public HashMap<String, JetStreamQueue> getQueues() {
        return (HashMap<String, JetStreamQueue>) _queues.clone();
    }

    protected Vector<JetStreamSocketClient> getNetClients() {
        return this._netClients;
    }

    protected Vector<JetStreamQueueScheduler> getQueueSchedulers() {
        return queueSchedulers;
    }

    public HashMap<String, RemoteServer> getServersByAddress() {
        return serversByAddress;
    }

    public int getOutstandingQueueSends() {
        int outstandingSends = 0;
        for (JetStreamQueue queue : _queues.values()) {
            outstandingSends += queue.getOutstandingQueueSends();
        }
        return outstandingSends;
    }

    public int getQueueSchedulerRequestCount() {
        return toBeSent.size();
    }

    public int getNetworkClientCount() {
        return _netClients.size();
    }

    public int getRemoteServerCount() {
        return serversByAddress.size();
    }

    protected void setHostInfo(String name, String addr) {
        this.localIPName = name;
        this.localIPAddr = addr;
    }

    protected void optimizeQueueReceivers(JetStreamQueue q,
            int clientPort, int clientLocalPort) {
        if (JetStream.fineLevelLogging) {
            logger.info("Optimizing receivers for queue " + q.getName());
        }

        // Optimize the queue's client list so that this client is
        // at the head of the list since it received the message
        //
        synchronized (q.clientList) {
            JetStreamSocketClient client =
                    (JetStreamSocketClient) q.clientList.elementAt(0);

            if (client.port != clientPort
                    || client.localPort != clientLocalPort) {
                // Search for this client in the list
                //
                int cnt = q.clientList.size();
                for (int i = 0; i < cnt; i++) {
                    client = q.clientList.elementAt(i);
                    if (client.port == clientPort
                            && client.localPort == clientLocalPort) {
                        // Remove the client from its current location
                        // and add it to the beginning of the queue
                        //
                        q.clientList.remove(i);
                        q.clientList.add(0, client);
                        if (JetStream.fineLevelLogging) {
                            logger.info("Optimized client list for q=" + q.name);
                        }
                        break;
                    }
                }
            }
        }
    }

    protected void handleFailedClient(JetStreamSocketClient client) {
        if (JetStream.fineLevelLogging) {
            logger.info("Handling failed client: " + client.hostName);
        }

        try {
            client.close();
        }
        catch (Exception e) {
        }

        removeNetClient(client);

        // If the client connection was actually the server
        // then try to take over for the server
        //
        if (!server.listening) {
            // Start the link listener
            //
            startServer();
        }
    }

    protected void throttleSender() {
        synchronized (throttleLock) {
            try {
                //if ( JetStream.fineLevelLogging )
                logger.info("Throttling");
                throttleLock.wait();
            }
            catch (Exception e) {
            }
        }
    }

    public void addServer(JetStreamSocketClient remote) {
        //logger.trace();

        if (JetStream.fineLevelLogging) {
            logger.info("Adding server: " + remote.hostName);
        }

        // Store remote server by both address and name
        RemoteServer server = null;
        synchronized (serversByAddress) {
            //serversByHostname.put(remote.hostName, remote);
            server = serversByAddress.get(remote.hostAddr);
            if (server == null) {
                // This is a new server we've never seen before
                if (JetStream.fineLevelLogging) {
                    logger.info("NEW SERVER CONNECTED: " + remote.hostAddr);
                }
                server = new RemoteServer();
                server.journals = new HashMap<String, Persistence>();
            }
            else {
                if (server.online && server.link != null && server.haLink == null) {
                    // This is a second connection to the remote server
                    // dedicated to handling HA messages
                    server.haLink = remote;
                    server.haLink.setStealthConnection(true);
                    if (JetStream.fineLevelLogging) {
                        logger.info("Dedicated HA connection complete");
                    }

                    // Check to see if it's in list of net clients and remove
                    synchronized (_netClients) {
                        if (_netClients.contains(server.haLink)) {
                            if (JetStream.fineLevelLogging) {
                                logger.info("Removing HALINK from list and queues");
                            }
                            _netClients.remove(server.haLink);
                        }
                    }
                    removeClientFromQueues(server.haLink);
                    return;
                }

                // Check to see if we need to send a "recovered" message
                // to the server if we recovered its messages when it failed
                //
                if (JetStream.fineLevelLogging) {
                    logger.info("FAILED SERVER RECONNECTED: " + remote.hostAddr);
                }

                if (server.recovered) {
                    // Send recovered message
                    if (JetStream.fineLevelLogging) {
                        logger.info("SENDING A RECOVERED MESSAGE TO " + remote.hostAddr);
                    }

                    // Send message that we recovered this new server's messages
                    JetStreamNetMessage netMessage = new JetStreamNetMessage(
                            this.localIPAddr,
                            JetStreamNetMessage.RECOVERED_PERSISTED_MESSAGES,
                            0);
                    remote.sendNetMsg(netMessage);
                }
            }

            server.link = remote;
            server.online = true;
            server.recovered = false;
            serversByAddress.put(remote.hostAddr, server);
        }

        // Create HA storage area for this server
        try {
            boolean created = true;
            //String haFolder = "/JetStream/HA/S." + remote.hostAddr;
            StringBuffer haFolder = new StringBuffer(HA_FOLDER);
            haFolder.append(File.separator);
            haFolder.append("S.");
            haFolder.append(remote.hostAddr);
            File haPath = new File(haFolder.toString());
            if (!haPath.exists()) {
                created = haPath.mkdir();
            }

            haFolder.append(File.separator);
            haFolder.append("queues");

            if (created) {
                haPath = new File(haFolder.toString());
                if (!haPath.exists()) {
                    created = haPath.mkdir();
                }
            }

            if (!created) {
                logger.severe("Failed to create HA folder: " + haFolder);
            }
            else if (JetStream.fineLevelLogging) {
                logger.info("Created HA folder: " + haFolder);
            }

            // Create a journal for all known queues
            synchronized (_queues) {
                if (_queues.size() > 0) {
                    Collection<JetStreamQueue> coll = _queues.values();
                    Iterator<JetStreamQueue> iter = coll.iterator();
                    while (iter.hasNext()) {
                        JetStreamQueue q = iter.next();

                        // Create a journal for this queue and store it
                        // for the current server
                        Persistence journal =
                                new NIOPersistence(haFolder.toString(),
                                q.getName(),
                                true); // reuse existing file
                        server.journals.put(q.getName(), journal);
                    }
                }
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
        }
    }

    public Object lookup(String name) {
        try {
            if (name.equals("ConnectionFactory")) {
                return new JetStreamConnectionFactory();
            }

            if (name.equals("TopicConnectionFactory")) {
                return new JetStreamTopicConnectionFactory();
            }

            if (name.equals("QueueConnectionFactory")) {
                return new JetStreamQueueConnectionFactory();
            }

            //
            // Anything else must be a topic or queue
            //

            synchronized (_topics) {
                if (_topics.containsKey(name) == true) {
                    if (JetStream.fineLevelLogging) {
                        logger.info("JetStream: Topic object exists for topic " + name);
                    }
                    //System.out.println("JetStream: Topic object exists for topic " + name);
                    return _topics.get(name);
                }
                /*
                 else {
                 createTopic(name);
                 }
                 */
            }

            //System.out.println("About to synchronize on _queues");
            synchronized (_queues) {
                //System.out.println("Inside synchronize");
                if (_queues.containsKey(name) == true) {
                    //if ( JetStream.fineLevelLogging )
                    logger.info("Queue object exists for queue " + name);
                    //System.out.println("Queue object exists for queue " + name);
                    return _queues.get(name);
                }
                /*
                 else {
                 createQueue(name);
                 }
                 */
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception:", e);
        }

        return this.createTopic(name);
        //return null;
    }
    
    class JetStreamSuperSubscriber {
        JetStreamTopicSubscriber jsSub;
        JetStreamConnection jsConn;
    }
    
    Vector<JetStreamSuperSubscriber> superSubscribers = 
            new Vector<JetStreamSuperSubscriber>();

    public void storeMultiSubscriber(   JetStreamTopicSubscriber jsSub,
                                        JetStreamConnection jsConn ) {
        synchronized ( superSubscribers ) {
            JetStreamSuperSubscriber jsSuperSub = new JetStreamSuperSubscriber();
            jsSuperSub.jsConn = jsConn;
            jsSuperSub.jsSub = jsSub;
            superSubscribers.add(jsSuperSub);
        }
    }
    
    public Object createTopic(String topicName) {
        //logger.trace();

        // If it already exists, return it
        //
        JetStreamTopic topic = _topics.get(topicName);
        if (topic != null) {
            return topic;
        }

        synchronized (_topics) {
            // Topic is new, create it now
            //
            topic = new JetStreamTopic(topicName);
            _topics.put(topicName, topic);
        }

        if (JetStream.fineLevelLogging) {
            logger.info("New Topic object created for topic " + topicName);
        }

        // Check to see if we have super-subscribers to add now
        synchronized ( superSubscribers ) {
            for ( JetStreamSuperSubscriber jsSuperSub: superSubscribers) {
                try {
                    topic.addConsumer(  jsSuperSub.jsSub, 
                                        jsSuperSub.jsConn );
                }
                catch ( Exception e ) {
                    e.printStackTrace();
                }
            }
        }

        return topic;
    }

    public Object createQueue(String queueName) throws Exception {
        // If it already exists, return it
        //
        JetStreamQueue queue = _queues.get(queueName);
        if (queue != null) {
            return queue;
        }

        // Queue is new, create it now
        //
        queue = new JetStreamQueue(queueName);

        synchronized (_queues) {
            _queues.put(queueName, queue);
        }

        // Give the queue its initial list of clients
        //
        Vector clientList = null;
        synchronized (_netClients) {
            clientList = (Vector) _netClients.clone();
        }

        queue.setClientList(clientList);

        if (isServer()) {
            // Add HA folder for this queue for all remote clients
            Set<String> keys;
            synchronized (serversByAddress) {
                keys = serversByAddress.keySet();
            }

            String[] serverAddrs = new String[keys.size()];
            keys.toArray(serverAddrs);
            for (int i = 0; i < serverAddrs.length; i++) {
                boolean created = false;
                StringBuffer haFolder = new StringBuffer(HA_FOLDER);
                haFolder.append(File.separator);
                haFolder.append("S.");
                haFolder.append(serverAddrs[i]);
                File haPath = new File(haFolder.toString());
                if (!haPath.exists()) {
                    created = haPath.mkdir();
                }

                haFolder.append(File.separator);
                haFolder.append("queues");

                if (created) {
                    haPath = new File(haFolder.toString());
                    if (!haPath.exists()) {
                        created = haPath.mkdir();
                    }
                }

                RemoteServer remote = serversByAddress.get(serverAddrs[i]);
                Persistence journal =
                        new NIOPersistence(haFolder.toString(),
                        queueName,
                        true); // reuse existing file
                remote.journals.put(queueName, journal);
            }
        }

        if (JetStream.fineLevelLogging) {
            logger.info("New Queue object created for queue " + queueName);
        }

        return queue;
    }
    // Interval timing for debugging
    private long intervalTimerStart = 0;

    public void startIntervalTimer(boolean print) {
        if (print) {
            logger.info("START INTERVAL TIMER");
        }
        intervalTimerStart = System.currentTimeMillis();
    }

    public long endIntervalTimer(boolean print) {
        long interval = System.currentTimeMillis() - intervalTimerStart;
        if (print) {
            logger.info("END INTERVAL TIMER: " + interval);
        }
        return interval;
    }

    public static void readConfig() {
        // Get logger related command-line directives
        //
        String s = System.getProperty("JetStream.logall", "false");
        if (s.equals("true")) {
            fineLevelLogging = true;
        }
        s = System.getProperty("JetStream.trace", "false");
        if (s.equals("true")) {
            traceOn = true;
        }
        s = System.getProperty("JetStream.logtofile", "true");
        writeToFile = true;
        if (s.equals("false")) {
            writeToFile = false;
        }
        s = System.getProperty("JetStream.logtostdout", "false");
        if (s.equals("true")) {
            writeToStdOut = true;
        }
        s = System.getProperty("JetStream.logappend", "true");
        if (s.equals("false")) {
            LOG_FILE_APPEND = false;
        }
        s = System.getProperty("JetStream.logfilesize", "100000000");
        LOG_FILE_SIZE = new Integer(s).intValue();
        s = System.getProperty("JetStream.logfilecount", "30");
        LOG_FILE_ROLLOVER_COUNT = new Integer(s).intValue();
        s = System.getProperty("JetStream.logfilepath", "");
        if (s != null && s.length() > 0) {
            if (s.endsWith(File.separator)) {
                LOG_FILE_PATH_NAME = s + "jetstream%u.%g.log";
            }
            else {
                LOG_FILE_PATH_NAME = s + File.separator + "jetstream%u.%g.log";
            }
        }
        else {
            LOG_FILE_PATH_NAME = "jetstream%u.%g.log";
        }

        // to be the server or not to be
        s = System.getProperty("JetStream.serverallowed", "true");
        if (s.equals("false")) {
            serverAllowed = false;
        }
        else {
            serverAllowed = true;
        }

        s = System.getProperty("JetStream.qcache", "1000");
        QUEUE_CACHE_SIZE = new Integer(s).intValue();

        // Internal queue sender threads
        s = System.getProperty("JetStream.queuesenders", "8");
        MAX_QUEUE_SENDERS = new Integer(s).intValue();
        // Internal topic sender threads
        s = System.getProperty("JetStream.topicsenders", "8");
        MAX_PUBSUB_SENDERS = new Integer(s).intValue();
        // Pub/sub throttle
        s = System.getProperty("JetStream.psthrottle", "1000000");
        PUBSUB_THROTTLE_COUNT = new Integer(s).intValue();

        s = System.getProperty("JetStream.ha", "true");
        if (s.equals("false")) {
            highAvailOn = false;
        }

        //
        // Message Compression
        //
        s = System.getProperty("JetStream.compress", "true");
        if (s.equals("false")) {
            compress = false;
        }
        s = System.getProperty("JetStream.compressSpeed", "true");
        if (s.equals("false")) {
            compressSpeed = false;
        }
        s = System.getProperty("JetStream.compressthreshold", "1024");
        COMPRESS_THRESHOLD = new Integer(s).intValue();

        // Message persistence
        s = System.getProperty("JetStream.persistdb", "true");
        if (s.equals("true")) {
            dbPersist = true;
        }
        s = System.getProperty("JetStream.persistfolder", "");
        if (s != null && s.length() > 0) {
            if (s.endsWith(File.separator)) {
                ROOT_FOLDER = s + "jsjournal";
            }
            else {
                ROOT_FOLDER = s + File.separator + "jsjournal";
            }
        }

        // Admin port
        s = System.getProperty("JetStream.adminport", "8081");
        adminPort = new Integer(s).intValue();
        if (s.equals("true")) {
            dbPersist = true;
        }

        // Determine if JetStream should communicate with other servers
        s = System.getProperty("JetStream.isolated", "false");
        if (s.equals("true")) {
            isolatedMode = true;
        }

        // Now try to load from an optional properties file, which will overwrite
        // the command-line params
        readConfigFile();

        setMessageFolders();
    }

    public static void readConfigFile() {
        Properties props = new Properties();

        InputStream is = null;

        // First try loading from the current directory
        try {
            File f = new File("jetstreamq.properties");
            is = new FileInputStream(f);
        }
        catch (Exception e) {
        }

        try {
            if (is == null) {
                // Try loading from classpath
                is = JetStream.getInstance().getClass().getResourceAsStream("jetstreamq.properties");
            }

            // Try loading properties from the file (if found)
            props.load(is);
        }
        catch (Exception e) {
            //System.out.println("jetstreamq.properties file does not exist");
            return;
        }

        // Get logger related command-line directives
        //
        String s = props.getProperty("logall", "false");
        if (s.equals("true")) {
            fineLevelLogging = true;
        }
        s = props.getProperty("trace", "false");
        if (s.equals("true")) {
            traceOn = true;
        }
        s = props.getProperty("logtofile", "true");
        writeToFile = true;
        if (s.equals("false")) {
            writeToFile = false;
        }
        s = props.getProperty("logtostdout", "false");
        if (s.equals("true")) {
            writeToStdOut = true;
        }
        s = props.getProperty("logappend", "true");
        if (s.equals("false")) {
            LOG_FILE_APPEND = false;
        }
        s = props.getProperty("logfilesize", "100000000");
        LOG_FILE_SIZE = new Integer(s).intValue();
        s = props.getProperty("logfilecount", "30");
        LOG_FILE_ROLLOVER_COUNT = new Integer(s).intValue();
        s = props.getProperty("logfilepath", "");
        if (s != null && s.length() > 0) {
            if (s.endsWith(File.separator)) {
                LOG_FILE_PATH_NAME = s + "jetstream%u.%g.log";
            }
            else {
                LOG_FILE_PATH_NAME = s + File.separator + "jetstream%u.%g.log";
            }
        }
        else {
            LOG_FILE_PATH_NAME = "jetstream%u.%g.log";
        }

        // to be the server or not to be
        s = props.getProperty("serverallowed", "true");
        if (s.equals("false")) {
            serverAllowed = false;
        }
        else {
            serverAllowed = true;
        }

        s = props.getProperty("qcache", "1000");
        QUEUE_CACHE_SIZE = new Integer(s).intValue();

        // Internal queue sender threads
        s = props.getProperty("queuesenders", "8");
        MAX_QUEUE_SENDERS = new Integer(s).intValue();
        // Internal topic sender threads
        s = props.getProperty("topicsenders", "8");
        MAX_PUBSUB_SENDERS = new Integer(s).intValue();
        // Pub/sub throttle
        s = props.getProperty("psthrottle", "1000000");
        PUBSUB_THROTTLE_COUNT = new Integer(s).intValue();

        s = props.getProperty("ha", "true");
        if (s.equals("false")) {
            highAvailOn = false;
        }

        //
        // Message Compression
        //
        s = props.getProperty("compress", "true");
        if (s.equals("false")) {
            compress = false;
        }
        s = props.getProperty("compressSpeed", "true");
        if (s.equals("false")) {
            compressSpeed = false;
        }
        s = props.getProperty("compressthreshold", "1024");
        COMPRESS_THRESHOLD = new Integer(s).intValue();

        // Message persistence
        s = props.getProperty("persistdb", "true");
        if (s.equals("true")) {
            dbPersist = true;
        }
        s = props.getProperty("persistfolder", "");
        if (s != null && s.length() > 0) {
            if (s.endsWith(File.separator)) {
                ROOT_FOLDER = s + "jsjournal";
            }
            else {
                ROOT_FOLDER = s + File.separator + "jsjournal";
            }
        }

        // Admin port
        s = props.getProperty("adminport", "8081");
        adminPort = new Integer(s).intValue();
        if (s.equals("true")) {
            dbPersist = true;
        }

        // Determine if JetStream should communicate with other servers
        s = props.getProperty("isolated", "false");
        if (s.equals("true")) {
            isolatedMode = true;
        }
    }

    public static void main(String[] args) {
        readConfig();
    }
}
