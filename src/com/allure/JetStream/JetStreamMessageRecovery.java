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

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import javax.jms.*;
import java.util.*;

import java.util.logging.Level;
import java.util.logging.Logger;
import util.*;

public class JetStreamMessageRecovery extends Thread {

    private Logger logger = Logger.getLogger("JetStreamMessageRecovery");
    private String messagePath = null;

    class DestinationEntry {

        public String journalVersion = null;
        public String queueName = null;
        public Long queueStartTime = null;
        public RandomAccessFile journal = null;
        public String filepath = null;
    }
    private DestinationEntry[] queueJournals = null;
    private DestinationEntry[] topicDirs = null;

    public JetStreamMessageRecovery(String messagePath) {
        logger.setParent(JetStream.getInstance().getLogger());

        this.messagePath = messagePath;

        // Look for messages to recover. It's important to do this immediately
        // even if we are to abandon message recovery so that the journal
        // files are straightened out
        //
        findMessageJournals();

        // Start the thread to begin recovering the messages
        //
        start();
    }

    @Override
    public void run() {

        // Delay message recovery to give other servers a chance to inform
        // us whether they recovered our messages as part of HA implementation
        //
        try {
            Thread.sleep(1000);
        }
        catch (Exception e) {
        }
        logger.info("Message recovery started");

        boolean recoverMessages = JetStream.getInstance().getMessageRecoveryFlag();
        if (recoverMessages == false) {
            // This flag set to false indicates that another server has
            // recovered our messages. Therefore abandon this recovery and
            // reset the flag. However, we still need to loop through the
            // affected queues to ensure that have jounrals, and delete older
            // journal files
            JetStream.getInstance().setMessageRecoveryFlag(true);
            logger.info("Message recovery aborted - another server recovered them");
        }


        Vector<JetStreamQueue> queuesRecovered =
                new Vector<JetStreamQueue>();
        try {
            //Thread.sleep(250);
            JetStream jetStream = JetStream.getInstance();

            ConnectionFactory connFact =
                    (ConnectionFactory) jetStream.lookup("QueueConnectionFactory");

            Connection conn = connFact.createConnection();
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            conn.start();

            // Start recovering the queued messages
            //
            if (queueJournals != null) {
                for (DestinationEntry dest : this.queueJournals) {
                    int messages = 0; // count of msgs recovered
                    if (dest == null
                            || dest.journal == null
                            || dest.queueName == null /*|| dest.queueName.length() == 0*/) {
                        continue;
                    }

                    JetStreamQueue q = null;
                    if (dest.queueName != null && dest.queueName.length() > 0) {
                        q = (JetStreamQueue) jetStream.createQueue(dest.queueName);
                        q.createMessageJournal();
                    }

                    if (q != null && recoverMessages) {
                        FileChannel channel = dest.journal.getChannel();
                        long filesize = dest.journal.length();
                        MappedByteBuffer mbb =
                                channel.map(FileChannel.MapMode.READ_ONLY,
                                channel.position(), filesize);
                        if (JetStream.fineLevelLogging) {
                            logger.info("Reading messages for queue " + q.name);
                        }
                        synchronized (q) {
                            try {
                                while (mbb.position() < filesize) {
                                    // Get the header fields
                                    boolean active = true;
                                    if (mbb.get() == Persistence.INACTIVE_RECORD) {
                                        active = false;
                                    }
                                    int clientPort = mbb.getInt();
                                    int clientLocalPort = mbb.getInt();
                                    byte type = mbb.get();
                                    if (type == 0) {
                                        break; // end of file
                                    }
                                    int size = mbb.getInt();

                                    if (!active) {
                                        // Inactive record, just skip over it
                                        mbb.position(mbb.position() + size);
                                    }
                                    else {
                                        // Get the data fields (msgID & data)
                                        byte[] bytes = new byte[mbb.getInt()];
                                        mbb.get(bytes);
                                        String msgId = new String(bytes);

                                        String text = null;
                                        JetStreamMessage msg = null;
                                        if (type == Persistence.TEXT_RECORD) {
                                            // String
                                            bytes = new byte[mbb.getInt()];
                                            mbb.get(bytes);
                                            text = new String(bytes);
                                        }
                                        else {
                                            // Bytes
                                            bytes = new byte[size - msgId.length() - 4];
                                            mbb.get(bytes);
                                        }

                                        // Reconstruct a message object from the data
                                        if (text != null) {
                                            msg = new JetStreamTextMessage(text);
                                        }
                                        else {
                                            msg = new JetStreamObjectMessage(bytes);
                                        }

                                        msg.msgId = msgId;
                                        msg.setFilename("");
                                        msg.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
                                        msg.clientPort = clientPort;
                                        msg.clientLocalPort = clientLocalPort;

                                        q.internalSend(msg,
                                                msg.getMsgId(),
                                                msg.clientPort,
                                                msg.clientLocalPort,
                                                false); // NOT an HA recovery
                                        messages++;
                                    }
                                }
                            }
                            catch (Exception e) {
                            }

                            //if ( JetStream.logMessages )
                            logger.info("  " + messages + " messages recovered for queue " + q.name);
                        }
                    }

                    // TODO: Test this code on Windows
                    // Clear, close and delete the recovered journal file
                    try {
                        try {
                            dest.journal.setLength(0);
                        }
                        catch (Exception e) {
                        }
                        dest.journal.close();
                        dest.journal = null;
                    }
                    catch (Exception e) {
                    }

                    try {
                        // The delete doesn't work on all platforms so rename
                        // it to mark as deleted in case
                        File f = new File(dest.filepath);
                        boolean deleted = f.delete();
                        if (deleted) {
                            if (JetStream.fineLevelLogging) {
                                logger.info("Deleted recovered journal " + dest.filepath);
                            }
                        }
                        else {
                            if (JetStream.fineLevelLogging) {
                                logger.info("Marking recovered journal " + dest.filepath);
                            }
                            File newName = new File(dest.filepath + "_d");
                            f.renameTo(newName);
                        }
                    }
                    catch (Exception e1) {
                    }

                    // Keep track of which queues had messages recovered
                    // so we can trigger them for delivery when done
                    if (messages > 0) {
                        queuesRecovered.add(q);
                    }
                }
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
        }

        logger.info("Message Recovery complete");

        if (recoverMessages) {
            // If there were messages recovered, trigger queue delivery attempt
            for (JetStreamQueue q : queuesRecovered) {
                JetStream.processQueue(q);
            }
        }
    }

    protected void deleteFiles(String subFolder, String[] files) {
        for (int i = 0; i < files.length; i++) {
            File dir = new File(messagePath + File.separator
                    + subFolder + File.separator
                    + files[i]);
            if (!dir.delete()) {
                logger.severe("JetStreamMessageRecovery.run() - could not delete dir "
                        + messagePath + File.separator + subFolder + File.separator + files[i]);
            }
        }
    }

    private void findMessageJournals() {
        try {
            StringBuffer basePath = new StringBuffer(messagePath);
            basePath.append(File.separator);
            basePath.append("queues");
            File baseDir = new File(basePath.toString());
            String[] journalNames = baseDir.list();
            if (journalNames == null) {
                return;
            }

            queueJournals = new DestinationEntry[journalNames.length];
            for (int i = 0; i < journalNames.length; i++) {
                queueJournals[i] = new DestinationEntry();

                // Create the journal's filepath
                StringBuffer filepath = new StringBuffer(basePath);
                filepath.append(File.separator);
                filepath.append(journalNames[i]);

                // Delete journals that are marked for delete ("_d") or
                // those for temporary destinations from previous run
                String sFilepath = filepath.toString();
                if (sFilepath.endsWith("_d") || sFilepath.contains("LJMS_TEMP")) {
                    // This is a file that was marked for deletion during
                    // a previous message recovery. Delete it now.
                    File del = new File(sFilepath);
                    boolean deleted = del.delete();
                    if (!deleted) {
                        logger.info("Could not delete journal " + filepath);
                    }
                    continue;
                }

                // Load the journal and read the header information
                RandomAccessFile journal =
                        new RandomAccessFile(filepath.toString(), "r");
                String journalVersion = null;
                String queueName = null;
                Long queueStartTime = new Long(-1);
                try {
                    String preamble = journal.readUTF();
                    if (preamble.startsWith("JETSTREAMJOURNALVERSION")) {
                        journalVersion = preamble;
                        queueName = journal.readUTF();
                    }
                    else {
                        journalVersion = NIOPersistence.JOURNAL_VERSION;
                        queueName = preamble;
                    }
                    queueStartTime = journal.readLong();
                }
                catch (Exception e) {
                }
                journal.close();

                // If this journal doesn't belong to this server run then
                // recover then renameit and messages within it
                //
                if (queueStartTime != JetStream.startTime) {
                    // Rename journal file since a new one will be created
                    File oldFile = new File(filepath.toString());
                    filepath.append("_recover");
                    oldFile.renameTo(new File(filepath.toString()));
                    if (JetStream.fineLevelLogging) {
                        logger.info("Renamed journal " + journalNames[i] + " to " + filepath);
                    }

                    try {
                        // Load the renamed journal file for message recovery
                        queueJournals[i].journal =
                                new RandomAccessFile(filepath.toString(), "rw");

                        // Only version 2 or later has the version number in the journal
                        String preamble = queueJournals[i].journal.readUTF();
                        if (preamble.startsWith("JETSTREAMJOURNALVERSION")) {
                            queueJournals[i].journalVersion = preamble;
                            queueJournals[i].queueName =
                                    queueJournals[i].journal.readUTF();
                        }
                        else {
                            logger.info("* Converting older message journal");
                            queueJournals[i].journalVersion = NIOPersistence.JOURNAL_VERSION;
                            queueJournals[i].queueName = preamble;
                        }

                        queueJournals[i].queueStartTime =
                                queueJournals[i].journal.readLong();
                        queueJournals[i].filepath = filepath.toString();
                    }
                    catch (Exception e) {
                        logger.log(Level.SEVERE, "Exception reading journal " + filepath, e);

                        // Since there was an error, close and delete the file
                        try {
                            queueJournals[i].journal.close();
                        }
                        catch (Exception e1) {
                        }
                        File del = new File(filepath.toString());
                        boolean deleted = del.delete();
                        if (deleted) {
                            logger.info("Corrupt journal " + filepath + " deleted");
                        }
                        else {
                            logger.info("Could not delete journal " + filepath);
                        }
                    }
                }
                else {
                    // Journal was created by this server instance. Leave it alone
                    if (JetStream.fineLevelLogging) {
                        logger.info("Skipping current journal file");
                    }
                    queueJournals[i] = null;
                }
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception:", e);
        }
    }
}
