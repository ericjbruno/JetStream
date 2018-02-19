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
import com.allure.JetStream.network.JetStreamSocketClient;
import util.*;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.*;

/**
 * This module implements all of the code for high-availability in JetStream
 */
public class JetStreamHA {

    private Logger logger = Logger.getLogger("JetSteamHA");
    JetStream jetstream;

    class DestinationEntry {

        public String dirName = null;
        public String[] msgNames = null;
    }

    ///////////////////////////////////////////////////////////////////////////
    public JetStreamHA() {
        jetstream = JetStream.getInstance();
        logger.setParent(jetstream.getLogger());
    }

    public boolean sendHAPersistMsg(JetStreamQueue queue, JetStreamMessage msg,
            int clientPort, int clientLocalPort) {
        boolean sent = false;

        //logger.trace();
        if (!JetStream.highAvailOn) {
            return false;
        }

        try {
            if (msg == null) {
                if (JetStream.fineLevelLogging) {
                    logger.info("No messages on HA persist queue");
                }
                return false;
            }

            if (JetStream.fineLevelLogging) {
                Throwable t = new Throwable();
                StackTraceElement ste = t.getStackTrace()[1];
                StringBuffer sb = new StringBuffer();
                sb.append("sendHAPersistMsg for msgId=");
                sb.append(msg.msgId);
                sb.append(" called from:");
                sb.append(ste.getMethodName());
                logger.info(sb.toString());
            }

            // Don't propogate remote msgNames to remote clients
            if (msg.remoteClient) {
                if (JetStream.fineLevelLogging) {
                    logger.info("Queue msg from remote client, skipping");
                }
                return false;
            }

            // Get the list of connected network clients
            HashMap<String, JetStream.RemoteServer> servers =
                    jetstream.getServersByAddress();

            // Make sure there are clients connected
            if (servers.size() == 0) {
                if (JetStream.fineLevelLogging) {
                    logger.info("No clients, skipping");
                }
                return false;
            }

            JetStreamNetMessage netMessage = new JetStreamNetMessage(
                    queue.name,
                    JetStreamNetMessage.HIGH_AVAIL_STORE_MESSAGE,
                    msg);

            // Send the message to remote servers only
            for (JetStream.RemoteServer server : servers.values()) {
                if (server.online) {
                    if (JetStream.fineLevelLogging) {
                        StringBuffer sb = new StringBuffer();
                        sb.append("Sending HIGH_AVAIL_STORAGE_MESSAGE");
                        sb.append("\n  msgId=");
                        sb.append(msg.msgId);
                        sb.append("\n  port=");
                        sb.append(server.haLink.port);
                        sb.append("\n  localPort=");
                        sb.append(server.haLink.localPort);
                        logger.info(sb.toString());
                    }

                    if (server.haLink != null) {
                        server.haLink.sendNetMsg(netMessage);
                    }
                    else {
                        if (JetStream.fineLevelLogging) {
                            logger.info("No HALINK, using message link");
                        }
                        server.link.sendNetMsg(netMessage);
                    }

                    msg.sentHAStore = true;
                    sent = true;
                }
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
        }

        return sent;
    }

    public void sendHARemoveMsg(String queueName, String msgId,
            int clientPort, int clientLocalPort)
            throws Exception {
        //logger.trace();
        if (!JetStream.highAvailOn) {
            return;
        }

        // Get the list of connected network clients
        HashMap<String, JetStream.RemoteServer> servers =
                jetstream.getServersByAddress();

        // Make sure there are clients connected
        if (servers.size() == 0) {
            if (JetStream.fineLevelLogging) {
                logger.info("No clients, skipping");
            }
            return;
        }

        JetStreamNetMessage netMessage = new JetStreamNetMessage(
                queueName,
                JetStreamNetMessage.HIGH_AVAIL_REMOVE_MESSAGE,
                clientPort, msgId);

        // Send the message to remote servers only
        //
        for (JetStream.RemoteServer server : servers.values()) {
            if (server.online) {
                if (JetStream.fineLevelLogging) {
                    StringBuffer sb = new StringBuffer();
                    sb.append("Sending HIGH_AVAIL_REMOVE_MESSAGE");
                    sb.append("\n  msgId=");
                    sb.append(msgId);
                    sb.append("\n  port=");
                    sb.append(server.haLink.port);
                    sb.append("\n  localPort=");
                    sb.append(server.haLink.localPort);
                    logger.info(sb.toString());
                }

                if (server.haLink != null) {
                    server.haLink.sendNetMsg(netMessage);
                }
                else {
                    if (JetStream.fineLevelLogging) {
                        logger.info("No HALINK, using message link");
                    }
                    server.link.sendNetMsg(netMessage);
                }
            }
        }
    }

    public void sendHARecoveryMessage(String failedHostAddr) {
        //logger.trace();

        // Get the list of connected network clients
        Vector<JetStreamSocketClient> netClients =
                jetstream.getNetClients();

        synchronized (netClients) {
            // Make sure there are clients connected
            if (netClients.size() == 0) {
                if (JetStream.fineLevelLogging) {
                    logger.info("No clients, skipping");
                }
                return;
            }

            JetStreamNetMessage netMessage =
                    new JetStreamNetMessage(
                    failedHostAddr,
                    JetStreamNetMessage.HIGH_AVAIL_RECOVERY_MESSAGE,
                    0);

            // Send the message to remote servers only
            for (JetStreamSocketClient client : netClients) {
                if (client.remoteClient) {
                    if (JetStream.fineLevelLogging) {
                        StringBuffer sb = new StringBuffer();
                        sb.append("Sending HIGH_AVAIL_RECOVERY_MESSAGE");
                        sb.append("\n  Failed server: ");
                        sb.append(failedHostAddr);
                        sb.append("\n  Recovered by: ");
                        sb.append(jetstream.localIPAddr);
                        logger.info(sb.toString());
                    }

                    client.sendNetMsg(netMessage);
                }
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    public void onHighAvailStorageMessage(String destName, JetStreamMessage msg,
            long netMsgId,
            String hostName, String hostAddr) {
        //logger.trace();

        if (!jetstream.isServer()) {
            logger.severe("Received a server message - I'm not the server!");
            return;
        }

        if (JetStream.fineLevelLogging) {
            StringBuffer sb = new StringBuffer("HighAvailStorage for queue ");
            sb.append(destName);
            sb.append("\n msgId=");
            sb.append(msg.msgId);
            sb.append("\n host=");
            sb.append(hostAddr);
            logger.info(sb.toString());
        }

        // Make sure the queue (and its HA folder) exist already
        try {
            JetStreamQueue queue = jetstream.getQueues().get(destName);
            if (queue == null) {
                queue = (JetStreamQueue) jetstream.createQueue(destName);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // Locate the journal for this queue for the specified server
        JetStream.RemoteServer server =
                jetstream.getServersByAddress().get(hostAddr);
        //RAFPersistence journal = server.journals.get(destName);
        Persistence journal = server.journals.get(destName);
        journal.persistMessage(msg);
    }

    public void onHighAvailRemovalMessage(String destName, String msgId,
            String hostName, String hostAddr) {
        //logger.trace();

        if (!jetstream.isServer()) {
            logger.severe("Received a server message - I'm not the server!");
            return;
        }

        try {
            // Locate the journal for this queue for the specified server
            JetStream.RemoteServer server =
                    jetstream.getServersByAddress().get(hostAddr);
            //RAFPersistence journal = server.journals.get(destName);
            Persistence journal = server.journals.get(destName);

            // Remove the specified message from the HA journal
            boolean deleted = journal.deleteMessage(msgId);

            if (deleted && JetStream.fineLevelLogging) {
                StringBuffer sb = new StringBuffer();
                sb.append("Deleted HA message for queue ");
                sb.append(destName);
                sb.append("\n msgId: ");
                sb.append(msgId);
                sb.append("\n from: ");
                sb.append(hostAddr);
                logger.info(sb.toString());
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
        }
    }

    public void onRemoteServerRecovery(String failedAddr, String recoveryAddr) {
        // This method is called when one remote server recovers for
        // another, failed, remote server
        //
        //logger.trace();
        if (JetStream.fineLevelLogging) {
            StringBuffer sb = new StringBuffer("Server ");
            sb.append(recoveryAddr);
            sb.append(" recovered for failed server ");
            sb.append(failedAddr);
            logger.info(sb.toString());
        }

        // Move the records for each queue from the failed server's journals
        // to the recovered server's journals
        //

        // Get the failed server's journals
        JetStream.RemoteServer failed =
                jetstream.getServersByAddress().get(failedAddr);

        // Get the recovered server's journals
        JetStream.RemoteServer recovered =
                jetstream.getServersByAddress().get(recoveryAddr);

        // Iterate through the queue journals
        for (String qName : failed.journals.keySet()) {
            if (JetStream.fineLevelLogging) {
                logger.info("Transfering HA messages from queue " + qName + " for server " + failedAddr + " to server " + recoveryAddr);
            }

            Persistence source = failed.journals.get(qName);
            Persistence target = recovered.journals.get(qName);

            TreeMap<String, Message> messages = source.getMessages();

            for (Message msg : messages.values()) {
                target.persistMessage(msg);
            }

            source.resetJournal();
        }
    }

    public void recoverFailedServer(JetStreamSocketClient failedServer) {
        // This method is called when this server recovers for a failed
        // remote server. We need to copy msgNames from the failed
        // server's HA directory into our own and in internal queues
        //
        //logger.trace();
        //if ( JetStream.logMessages )
        {
            StringBuffer sb = new StringBuffer("HA recovery: taking over for server ");
            sb.append(failedServer.hostName);
            sb.append(" (address: ");
            sb.append(failedServer.hostAddr);
            sb.append(")");
            logger.info(sb.toString());
        }

        // Send a recovery message to all other servers (they will move
        // message files from one HA directory to another)
        //
        sendHARecoveryMessage(failedServer.hostAddr);

        // Get the failed server's journals
        JetStream.RemoteServer failed =
                jetstream.getServersByAddress().get(failedServer.hostAddr);

        // Iterate through the queue journals
        for (String qName : failed.journals.keySet()) {
            //if (JetStream.logMessages)
            logger.info("Recovering HA messages from queue " + qName + " for server " + failedServer.hostAddr);

            try {
                // Get queue reference
                JetStreamQueue queue =
                        (JetStreamQueue) jetstream.createQueue(qName);

                // Get the saved messages for this queue
                Persistence source = failed.journals.get(qName);
                TreeMap<String, Message> messages = source.getMessages();

                // Internally enqueue these messages
                int messagesRecovered = 0;
                for (Message msg : messages.values()) {
                    if (msg != null) {
                        JetStreamMessage jsMsg = (JetStreamMessage) msg;
                        jsMsg.sentHAStore = true; // This message was sent as an HA

                        // Enqueue the message
                        queue.internalSend(jsMsg,
                                jsMsg.getMsgId(),
                                jsMsg.clientPort,
                                jsMsg.clientLocalPort,
                                true);
                        messagesRecovered++;
                    }
                }
                logger.info("  " + messagesRecovered + " HA messages recovered for queue " + queue.name);

                // Remove all records (messages) from the failed server's journal
                source.resetJournal();

                // If there were messages recovered, trigger queue delivery attempt
                JetStream.processQueue(queue);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        logger.info("HA message recovery complete");
    }
}
