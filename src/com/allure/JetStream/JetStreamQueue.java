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

import com.allure.JetStream.network.JetStreamSocketClient;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import javax.jms.*;
import util.NIOPersistence;

public class JetStreamQueue extends JetStreamDestination implements javax.jms.Queue {

    protected int lastReceiver = 0;
    protected Vector<JetStreamSocketClient> clientList =
            new Vector<JetStreamSocketClient>();
    protected boolean suspended = false;
    protected boolean optimized = false;
    protected boolean midFlight = false;
    protected boolean skipOrigSender = false;
    protected int entries = 0;
    protected int delivered = 0;
    protected boolean HAEnabled = true;
    // Outstanding queue message send list
    private final ConcurrentHashMap<Long, QueueRequest> outstandingSends =
            new ConcurrentHashMap<Long, QueueRequest>();

    // This object serves as a lightweight placeholder for the in-memory queue
    // If the queue goes over a certain length, only the message ID is stored
    // in memory. The message itself will be read from disk upon delivery
    class QueuedMessage {

        String id = null;
        JetStreamMessage msg = null;
        boolean sentAsHA = false;

        public boolean wasSentHA() {
            return sentAsHA;
        }
        
        public void setSentHA(boolean sent) {
            this.sentAsHA = sent;
        }
        

        public QueuedMessage(JetStreamMessage msg) {
            this.id = msg.getMsgId(); //null;
            this.msg = msg;
        }

        public QueuedMessage(String msgId) {
            this.id = msgId;
            this.msg = null;
        }
    }
    
    
    // The actual message queue and queue index
//    private final ConcurrentLinkedQueue<QueuedMessage> messageQ =
//            new ConcurrentLinkedQueue<QueuedMessage>();
    private final ConcurrentLinkedQueue<QueuedMessage> messageQ =
            new ConcurrentLinkedQueue<QueuedMessage>();

    // The message journal is where persisted messages are stored
    protected NIOPersistence journal = null;
    public JetStream jetstream;

    ////////////////////////////////////////////////////////////////////////////
    protected JetStreamQueue() {
        this.destType = JetStreamDestination.QUEUE;
    }

    public JetStreamQueue(String queueName) throws Exception {
        this();

        if (queueName == null || queueName.length() == 0) {
            logger.log(Level.SEVERE, "Error: Client sent Null Queue Name");
            System.out.println("Error: Client sent Null Queue Name");
            throw new Exception("JetStream: Client passed NULL queue name");
        }

        jetstream = JetStream.getInstance();
        name = queueName;

        if (jetstream.isServer() && queueName != null) {
            // Create a place to persist messages until delivered
            createMessageJournal();
        }
    }

    protected void createMessageJournal() {
        //logger.fine("entering");

        if (journal == null) {
            JetStream.getInstance().createJournalFolders();

            // Create the journal, providing base dir and base journal name
            String journalName = name;
            journalName = journalName.replaceAll("/", "_");
            //System.out.println("!!! NEW QUEUE, name="+name+", journalName="+journalName);
            journal = new NIOPersistence(JetStream.QUEUES_FOLDER, journalName);
        }
    }

    public boolean isSuspended() {
        return suspended;
    }

    public int getClientListSize() {
        return this.clientList.size();
    }

    public int getClientListCapacity() {
        return this.clientList.capacity();
    }

    public int getConsumerSize() {
        return this.consumers.size();
    }

    public int getConsumerCapactity() {
        return this.consumers.capacity();
    }

    protected void setSuspended(boolean suspended) {
        //logger.fine("entering");
        this.suspended = suspended;

        if (!jetstream.isServer()) {
            return;
        }

        if (JetStream.fineLevelLogging) {
            if (suspended) {
                logger.info("QUEUE " + name + " SUSPENDED");
            }
            else {
                logger.info("QUEUE " + name + " READY");
            }
        }
    }

    public boolean isOptimized() {
        return optimized;
    }

    protected void setOptimized(boolean optimized) {
        this.optimized = optimized;
    }

    protected void enableHA() {
        this.HAEnabled = true;
    }

    protected void disableHA() {
        this.HAEnabled = false;
    }

    protected boolean isHAEnabled() {
        if (!JetStream.highAvailOn) {
            return false;
        }
        if (!this.HAEnabled) {
            return false;
        }

        return true;
    }

    protected void setClientList(Vector clients) {
        clientList = clients;
    }

    protected void addSender(QueueSender sender, Connection conn) throws JMSException {
        addProducer(sender, conn);
        logger.info("sender added for queue " + name);
    }

    protected void addReceiver(QueueReceiver rec, Connection conn) throws JMSException {
        addConsumer(rec, conn);
        logger.info("receiver added for queue " + name);
    }

    protected Enumeration getMessages() throws JMSException {
        //logger.fine("entering");
        synchronized (messageQ) {
            // TODO: Need to change this to read in the actual messages
            Vector<QueuedMessage> msgs = new Vector<QueuedMessage>(messageQ);
            return msgs.elements();
        }
    }

    public boolean isEmpty() {
        return messageQ.isEmpty();
    }

    public int getMessageCount() {
        //logger.fine("entering");
        return entries;
    }

    public int getDeliveredCount() {
        //logger.fine("entering");
        return delivered;
    }

    public int getMessagesInMemory() {
        return messageQ.size();
    }

    public int getOutstandingQueueSends() {
        return outstandingSends.size();
    }

    public long getJournalRecordCount() {
        if (journal == null) {
            return -1;
        }

        synchronized (journal) {
            return journal.getRecordCount();
        }
    }

    public long getJournalEmptyCount() {
        if (journal == null) {
            return -1;
        }

        synchronized (journal) {
            return journal.getEmptyCount();
        }
    }

    public long getJournalFileSize() {
        if (journal == null) {
            return -1;
        }

        synchronized (journal) {
            return journal.getFilesize();
        }
    }

    protected void send(Message message) throws JMSException {
        //logger.fine("entering");
        send(message, 0, 0, 0);
    }

    protected void send(Message message, long origNetMsgId,
            int clientPort, int clientLocalPort) {
        //logger.fine("entering");
        if (JetStream.fineLevelLogging) {
            logger.info("in Send() for queue: " + name);
        }

        if (message == null) {
            logger.severe("message object is null");
            return;
        }

        try {
            // Create the message to be queued and sent
            //
            JetStreamMessage jsMessage = (JetStreamMessage) message;
            jsMessage.setDestinationName(name);
            jsMessage.setMessageType(JetStreamMessage.QUEUE_MESSAGE);
            jsMessage.setJMSDestination(this);
            jsMessage.setSent(false);
            jsMessage.clientPort = clientPort;
            jsMessage.clientLocalPort = clientLocalPort;
            jsMessage.deliveryMode = (byte) message.getJMSDeliveryMode();

            // Place the message on an internal FIFO queue
            QueuedMessage qMsg = null;
            synchronized (this) {
                if (message.getJMSDeliveryMode() == DeliveryMode.PERSISTENT) {
                    
                    //qMsg = enqueueMessage(jsMessage);
                    
                    if (jetstream.isServer()) {
                        boolean persisted = journal.persistMessage(jsMessage);
                        if (!persisted) {
                            logger.severe("Could not persist message " + message.getJMSMessageID());
                            throw new Exception("Could not persist message");
                        }

                        if (isHAEnabled()) {
                            // Send an HA Persist message to other servers
                            boolean sent = jetstream.ha.sendHAPersistMsg(
                                    this, jsMessage, clientPort, clientLocalPort);

                            qMsg.setSentHA(sent);
                        }
                    }
                }
                else {
                    qMsg = enqueueMessage(jsMessage);                    
                }
            }

            // Only awaken the thread if we are not the server. If we are the
            // server, the queue scheduler will notify the sender thread
            // at the proper time
            //
            if (!jetstream.isServer()) {
                deliverMessageDirect(jsMessage);
            }
            else {
                JetStream.processQueue(this);
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
        }
    }

    protected void internalSend(Message message,
            String origMsgId,
            int clientPort,
            int clientLocalPort,
            boolean haRecovery) {
        //logger.fine("entering");
        if (JetStream.fineLevelLogging) {
            logger.info("in internalSend() for queue: " + name);
        }

        if (message == null) {
            logger.severe("message object is null");
            return;
        }

        try {
            // Create the message to be queued and sent
            //
            JetStreamMessage jsMessage = (JetStreamMessage) message;
            jsMessage.setDestinationName(name);
            jsMessage.setMessageType(JetStreamMessage.QUEUE_MESSAGE);
            jsMessage.setJMSDestination(this);
            jsMessage.setSent(false);
            jsMessage.clientPort = clientPort;
            jsMessage.clientLocalPort = clientLocalPort;
            jsMessage.deliveryMode = (byte) message.getJMSDeliveryMode();

            synchronized (this) {
                // Place the message on an internal FIFO queue
                //
                enqueueMessage(jsMessage);

                if (message.getJMSDeliveryMode() == DeliveryMode.PERSISTENT) {
                    if (jetstream.isServer()) {
                        boolean persisted = journal.persistMessage(jsMessage);
                        if (!persisted) {
                            logger.severe("Could not persist message " + message.getJMSMessageID());
                            throw new Exception("Could not persist message");
                        }
                    }
                }
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
        }
    }

    synchronized protected void deliverMessageDirect(JetStreamMessage msg) {
        //logger.fine("entering");
        if (JetStream.fineLevelLogging) {
            logger.info("deliverMessageDirect called for queue " + name);
        }
        //System.out.println("deliverMessageDirect");

        // Make sure there is a message to send
        if (messageQ.isEmpty()) {
            if (JetStream.fineLevelLogging) {
                logger.info("Queue " + name + " is empty");
            }
            //System.out.println("Queue " + name + " is empty");
            return;
        }

        if (isSuspended()) {
            if (JetStream.fineLevelLogging) {
                logger.info("Queue " + name + " is suspended");
            }
            //System.out.println("Queue " + name + " is suspended");
            return;
        }

        // Attempt to deliver the given msg. If null, deliver head msg
        //
        msg = null; // make sure we're always delivering
                    // the head message. This becomes and issue when
                    // there's a failure of the server instance with
                    // messages in flight

        if (msg == null) {
            msg = getNextQueuedMessage();
            if (msg == null) {
                logger.info("Message is null");
                return;
            }
        }

        // flight at a time for a queue, we mark the queue as suspended
        // until a response is received for this outstanding message
        //
        setSuspended(true);

        // Attempt to deliver the message locally. If no local
        // receivers, attempt delivery to a remote receiver
        //
        if (localNotify(msg)) {
            //System.out.println("DELIVERED LOCALLY");
            onMessageSent(true, msg.msgId);
//            // Check if there are more messages to deliver
//            if ( ! isEmpty() ) {
//                System.out.println("THERE ARE MORE MESSAGES QUEUED!!!!!");
//                while ( ! isEmpty() ) {
//                    if (localNotify(msg)) {
//                        System.out.println("DELIVERED LOCALLY");
//                        onMessageSent(true, msg.msgId);
//                    }
//                }
//            }
        }
        else {
            //System.out.println("deliverMessageDirect - IN SYNCHRONIZE");
            // We check the queue size before and after sending the
            // message to the server. If it's the same, this indicates
            // a comms failure, and we need to retry sending the message
            //
            int attempts = 0;
            int queueSizeBefore = messageQ.size();
            while (messageQ.size() == queueSizeBefore) {
                // Try up to 5 times, and abort if we become the server
                if (++attempts > 5 || JetStream.getInstance().isServer()) {
                    break;
                }

                if (msg.getSent()) {
                    try {
                        logger.info("Message marked as sent: " + ((TextMessage) msg).getText());
                    }
                    catch (Exception e) {
                        logger.info("Message marked as sent");
                    }
                    //logger.info("MESSAGE WAS SENT - NOT RETRYING");
                    //break;
                }

                if (attempts > 1) {
                    logger.info("Retrying send to server - attempt " + attempts);
                }

                //
                // Here's where the message gets sent to the server
                //
                if (remoteNotify(msg, false) == true) {
                    // Wait until the queue gets signaled, which occurs
                    // when the JS Server acks the message 
                    try {
                        this.wait(5000);
                    }
                    catch (Exception e) {
                        logger.log(Level.INFO, "Exception", e);
                    }
                }
                else {
                    logger.severe("Comms failure sending message - need to retry");
                }
            }
        }

        // Mark queue as active again since we can only get here in two
        // conditions: 1) localNotify handled the message, or 2) the call
        // to wait() returned when signaled when a queue response was
        // received. In either case, the queue is free to send another msg
        //
        setSuspended(false);

        //removeExpiredMessages();
    }

    protected void deliverMessage() {
        //logger.fine("entering");
        if (JetStream.fineLevelLogging) {
            logger.info("JetStreamQueue.deliverMessage called");
        }

        // Attempt to deliver the head message
        //
        JetStreamMessage jsMessage = null;

        synchronized (this) {
            if (isSuspended()) {
                if (JetStream.fineLevelLogging) {
                    logger.info("Queue " + name + " is suspended");
                }
                return;
            }

            // Make sure there is a message to send
            if (messageQ.isEmpty()) {
                if (JetStream.fineLevelLogging) {
                    logger.info("Queue " + name + " is empty");
                }
                return;
            }

            jsMessage = getNextQueuedMessage();
            if (jsMessage == null) {
                if (JetStream.fineLevelLogging) {
                    logger.info("Queue " + name + " is empty");
                }
                return;
            }

            // flight at a time for a queue, we mark the queue as suspended
            // until a response is received for this outstanding message
            //
            setSuspended(true);
        }

        if (JetStream.fineLevelLogging) {
            logger.info("deliverMessage called for queue " + name);
        }

        // Attempt to deliver the message locally. If no local
        // receivers, attempt delivery to a remote receiver
        //
        boolean sent = localNotify(jsMessage);
        if (sent) {
            if (JetStream.fineLevelLogging) {
                logger.info("Sent message to local client");
            }

            onMessageSent(true, jsMessage.msgId);
            setSuspended(false);

            // Ensure the queue gets serviced again if there are more messages
            if (jetstream.isServer()) {
                if (!messageQ.isEmpty()) {
                    JetStream.processQueue(this);
                }
            }

            // Check if a deferred queue response needs to be sent
            jetstream.netMsgSender.checkToSendDeferredQResponse(jsMessage, true);

            return;
        }

        sent = remoteNotify(jsMessage, this.skipOrigSender);
        if (!sent) {
            if (JetStream.fineLevelLogging) {
                logger.info("There are no remote listeners");
            }
            setSuspended(false);
        }

        //removeExpiredMessages();
    }

    protected boolean localNotify(Message message) {
        //logger.fine("entering");
        if (JetStream.fineLevelLogging) {
            logger.info("message received for queue " + name);
        }

        // Notify a local client
        //
        if (noLocalReceivers()) {
            if (JetStream.fineLevelLogging) {
                logger.info("No local receivers for queue " + name);
            }
            return false;
        }

        // Pick the receiver to notify
        //
        int count = consumers.size();
        if (lastReceiver >= count) {
            lastReceiver = 0;
        }

        ConsumerNode consumer = (ConsumerNode) consumers.elementAt(lastReceiver);
        lastReceiver++;

        try {
            JetStreamQueueReceiver rec = (JetStreamQueueReceiver) consumer._obj;
            return rec.onMessage(message);
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
            return false;
        }
    }

    protected boolean remoteNotify(JetStreamMessage jsMessage,
            boolean skipSourceCheck) {
        //logger.fine("entering");
        if (JetStream.fineLevelLogging) {
            StringBuffer sb = new StringBuffer("remoteNotify, msgId=");
            sb.append(jsMessage.getMsgId());
            sb.append(", queue=");
            sb.append(name);
            logger.info(sb.toString());
        }

        // Skip the original sender unless flag indicates otherwise
        int clientPort = -1;
        int clientLocalPort = -1;
        if (!skipSourceCheck) {
            clientPort = jsMessage.clientPort;
            clientLocalPort = jsMessage.clientLocalPort;
        }

        boolean sent = jetstream.netMsgSender.sendNetQueueMsg(
                jsMessage, this,
                jsMessage.msgId,
                clientPort,
                clientLocalPort);
        
        jsMessage.setSent(sent);

        return sent;
    }

    private boolean noLocalReceivers() {
        //logger.fine("entering");
        int count = consumers.size();
        if (count == 0) {
            return true;
        }

        for (int i = 0; i < count; i++) {
            ConsumerNode consumer = (ConsumerNode) consumers.elementAt(i);
            JetStreamConnection conn = (JetStreamConnection) consumer._conn;

            if (conn == null || conn.isStarted()) {
                return false;
            }
        }

        // No receivers are in receive mode
        //
        return true;
    }

    private QueuedMessage enqueueMessage(JetStreamMessage msg) {
        //logger.fine("entering");
        if (JetStream.fineLevelLogging) {
            logger.info("enqueueMessage called with id " + msg.getMsgId());
        }

        QueuedMessage qMsg = null;

        // If the queue is over a certain size, only store the message id
        // to preserve memory, otherwise store the whole thing
//        if (getMessageCount() > JetStream.QUEUE_CACHE_SIZE
//                && msg.getJMSDeliveryMode() == DeliveryMode.PERSISTENT
//                && jetstream.isServer()) {
//        if ( jetstream.isServer() 
//             && ( msg.getJMSDeliveryMode() == DeliveryMode.PERSISTENT 
//                  || getMessageCount() > JetStream.QUEUE_CACHE_SIZE ) ) {
        if (getMessageCount() > JetStream.QUEUE_CACHE_SIZE
                && msg.getJMSDeliveryMode() == DeliveryMode.PERSISTENT
                && jetstream.isServer()) {
            qMsg = new QueuedMessage(msg.getMsgId());
        }
        else {
            qMsg = new QueuedMessage(msg);
        }

        qMsg.setSentHA( msg.sentHAStore );
        messageQ.add(qMsg);
        entries++;

        return qMsg;
    }

    protected JetStreamMessage dequeueMessage() {
        if (JetStream.fineLevelLogging) {
            logger.info("dequeueMessage called");
        }

        if (messageQ.isEmpty()) {
            logger.severe("message queue is EMPTY");
            return null;
        }

        try {
            QueuedMessage qMsg = messageQ.remove();
            entries--;
            delivered++;
            JetStreamMessage msg = qMsg.msg;
            if (msg == null) {
                msg = (JetStreamMessage) journal.getRecord(qMsg.id);
                msg.setDestinationName(name);
                msg.setMessageType(JetStreamMessage.QUEUE_MESSAGE);
                msg.sentHAStore = qMsg.wasSentHA();
            }

            if (msg instanceof TextMessage) {
                TextMessage txtMsg = (TextMessage) msg;
            }
            return msg;
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
        }

        return null;
    }

    protected JetStreamMessage getNextQueuedMessage() {
        //logger.fine("entering");
        if ( messageQ == null) {
            System.out.println("NULL");
        }
        if (messageQ.isEmpty()) {
            return null;
        }

        QueuedMessage qMsg = messageQ.peek();
        JetStreamMessage msg = qMsg.msg;
        if (msg == null) {
            try {
                msg = (JetStreamMessage) journal.getRecord(qMsg.id);
                msg.setDestinationName(name);
                msg.setMessageType(JetStreamMessage.QUEUE_MESSAGE);
                msg.sentHAStore = qMsg.wasSentHA();
            }
            catch ( Exception e ) {
                e.printStackTrace();
            }
        }

        return msg;
    }

    private void removeExpiredMessages() {
        // TODO: Need to remove from persisted storage also !!

        // Calculate GMT
        //
        GregorianCalendar currentGMT = new GregorianCalendar();
        currentGMT.set(currentGMT.get(Calendar.YEAR) - 1900,
                currentGMT.get(Calendar.MONTH),
                currentGMT.get(Calendar.DAY_OF_MONTH),
                currentGMT.get(Calendar.HOUR_OF_DAY),
                currentGMT.get(Calendar.MINUTE),
                currentGMT.get(Calendar.SECOND));

        long gmt = currentGMT.getTime().getTime();

        // Check queued messages for expiration
        //
        /*
        synchronized (messageQ) {
            for (int i = 0; i < getMessageCount(); i++) {
                 QueuedMessage qMsg = messageQ.get(i);
                 JetStreamMessage jsMessage = qMsg._msg;
                 if ( jsMessage == null ) {
                    jsMessage = (JetStreamMessage)journal.getRecord(qMsg._id);
                    jsMessage.sentHAStore = qMsg.sentHAStore;
                 }
                 if ( jsMessage != null ) {
                    // Make sure that if an exception occurs trying to get the
                    // message expiration, the message gets removed by initializing
                    // the expiration to -1
                    //
                    long expiration = -1;
                    try { expiration = jsMessage.getJMSExpiration(); } catch ( Exception e ) { }

                    if ( gmt >= expiration ) {
                        logger.info("REMOVING EXPIRED MESSAGE FOR QUEUE" + name);

                        // Don't let exceptions stop us from removing from the queue
                        //
                        try { jsMessage.clearBody(); } catch ( Exception e ) { }
                        try { jsMessage.clearProperties(); } catch ( Exception e ) { }
                        messageQ.remove(i);
                        //HAQ.remove(jsMessage);
                    }
                 }
            }
        }
        */
    }

    // This method is called when a queue message has been sent to all
    // available listeners
    //
    protected synchronized JetStreamMessage onMessageSent(boolean delivered, String messageId) {
        //logger.fine("entering");
        if (JetStream.fineLevelLogging) {
            StringBuffer sb = new StringBuffer();
            sb.append("onMessageSent() - Ended a send for queue ");
            sb.append(name);
            sb.append(", msgId=");
            sb.append(messageId);
            logger.info(sb.toString());
        }

        JetStreamMessage msg = null;

        try {
            // Get the head message from the queue
            msg = getNextQueuedMessage();
            //msg.setSent(true);

            // If the message was delivered, or the message wasn't and it was
            // from a remote client, dequeue and delete the message
            if (msg == null) {
                logger.info("Message object is NULL - this instance probably recovered for the server");
                new Exception("STACK TRACE").printStackTrace();
                return null;
            }

            if (delivered || msg.remoteClient) {
                if (msg.msgId.equals(messageId)) {
                    dequeueMessage();
                }
                else {
                    logger.severe("Message ID " + msg.msgId + " doesn't match expected ID " + messageId);
                    StringBuffer sb = new StringBuffer();
                    sb.append("Information dump...");
                    sb.append("\n  Called by: ");
                    Throwable t = new Throwable();
                    StackTraceElement ste = t.getStackTrace()[1];
                    sb.append(ste.getMethodName());
                    sb.append(this.messageQ.size());
                    sb.append("\n  Messages queued: ");
                    sb.append(this.messageQ.size());
                    if (JetStream.fineLevelLogging) {
                        sb.append("\n  Queued message ids: ");
                        Iterator<QueuedMessage> iter = messageQ.iterator();
                        while (iter.hasNext()) {
                            sb.append("\nid=" + iter.next().id);
                        }
                        logger.log(Level.FINEST, sb.toString());
                    }
                }

                if (jetstream.isServer()
                        && msg.deliveryMode == DeliveryMode.PERSISTENT) {
                    journal.deleteMessage(msg);

                    // If the message was submitted for HA storage, and it
                    // arrived from a local client, send an HA Remove message
                    // to remote servers now
                    if (msg.sentHAStore) {
                        jetstream.ha.sendHARemoveMsg(
                                name, msg.msgId, msg.clientPort, msg.clientLocalPort);
                    }
                }
            }
            else {
                msg.setSent(false);
                if (JetStream.fineLevelLogging) {
                    logger.info("Message not delivered");
                }
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
        }

        return msg;
    }

    protected void addOutstandingQueueRequest(QueueRequest qReq, long id) {
        // Add this to the SendTimeout timer queue
        qReq.sendCountdownTick = 5;
        outstandingSends.put(id, qReq);
        if (JetStream.fineLevelLogging) {
            Throwable t = new Throwable();
            StackTraceElement ste = t.getStackTrace()[1];
            StringBuffer sb = new StringBuffer();
            sb.append("Added Outstanding qreq, netMsgId=");
            sb.append(id);
            sb.append(", from:");
            sb.append(ste.getMethodName());
            logger.info(sb.toString());
        }
    }

    protected QueueRequest removeOutstandingQueueRequest(long id) {
        QueueRequest qReq = outstandingSends.remove(id);
        if (JetStream.fineLevelLogging) {
            Throwable t = new Throwable();
            StackTraceElement ste = t.getStackTrace()[1];
            if (JetStream.fineLevelLogging) {
                StringBuffer sb = new StringBuffer();
                if (qReq == null) {
                    sb.append("Outstanding qreq NOT removed, netMsgId=");
                }
                else {
                    sb.append("Outstanding qreq removed, netMsgId=");
                }
                sb.append(id);
                sb.append(", from:");
                sb.append(ste.getMethodName());
                logger.info(sb.toString());
            }
        }

        return qReq;
    }

    protected void checkForExpiredSends() {
        // Iterate through the list of outstanding sends and
        // decrement each tick. For each one that ticks down
        // to zero, remove it from the list and fake a queue
        // response with a "false" received value
        //
        try {
            //synchronized (outstandingSends)
            {
                Collection<QueueRequest> values = outstandingSends.values();
                Iterator<QueueRequest> iter = values.iterator();
                int n = values.size();
                while (iter.hasNext()) {
                    QueueRequest qReq = iter.next();
                    if (--qReq.sendCountdownTick <= 0) {
                        StringBuffer sb = new StringBuffer();
                        sb.append("Expired queue send detected, queue=");
                        sb.append(qReq.queue.name);
                        sb.append(", netMsgId=");
                        sb.append(qReq.netMsg.netMsgId);
                        sb.append(", clientPort=");
                        sb.append(qReq.clientPort);
                        sb.append(", clientLocalPort=");
                        sb.append(qReq.clientLocalPort);
                        logger.info(sb.toString());

                        // Expired. Fake a response and trigger a queue resend
                        // in case there are other listeners for this queue
                        //
                        jetstream.netMsgHandler.onQueueResponseMessage(
                                qReq.queue.name,
                                qReq.netMsg.netMsgId, false,
                                (int) qReq.netMsg.clientPort,
                                (int) qReq.netMsg.clientLocalPort);

                        JetStream.processQueue(qReq.queue);
                    }
                }
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception:", e);
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Queue
    public String getQueueName() throws JMSException {
        return name;
    }

    @Override
    public String toString() {
        try {
            // Returns a string representation of this object 
            return "JetStream_" + getQueueName();
        }
        catch (Exception e) {
            return null;
        }
    }
}
