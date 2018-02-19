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
package util;

import com.allure.JetStream.*;

import java.io.*;
import java.util.*;

import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.*;

public class RAFPersistence implements Persistence {

    private Logger logger = Logger.getLogger("RAFPersistence");
    //
    // The message journal is where persisted messages are stored
    // Intend to move the bulk of the internals out of this class
    // and hide implementation within its own class
    //
    protected RandomAccessFile journal = null;
    // Keep an index by last name of each emp in the journal
    public HashMap<String, Long> recordIndex =
            new HashMap<String, Long>();
    // Keep an index of LinkedList, where the index key is the size
    // of the empty record. The LinkedList contains pointers (offsets) to
    // the empty records
    public TreeMap<Integer, LinkedList<Long>> emptyIdx =
            new TreeMap<Integer, LinkedList<Long>>();

    class Header {
        //   Boolean    - Active record indicator
        //   Byte       - Message type (0=Empty, 1=Bytes, 2=String)
        //   Integer    - Size of record's payload (not header)

        boolean active; // 1 byte
        byte type;      // 1 byte - duh!
        int size;       // 4 bytes
        static final int HEADER_SIZE = 6;
    }

    class JournalLocationData {

        long offset;
        int newEmptyRecordSize;
    }
    private String journalFolder = "";
    private String journalName = "";
    // Performance tracking data
    private boolean trackPerformance = false;
    private int persistCount = 0;
    private long worstPersistTime = 0;
    private int deleteCount = 0;
    private long worstDeleteTime = 0;

    ///////////////////////////////////////////////////////////////////////////
    private RAFPersistence() {
        logger.setParent(JetStream.getInstance().getLogger());
    }

    public RAFPersistence(String folder, String name) {
        this(folder, name, false);
    }

    public RAFPersistence(String folder, String name, boolean reuseExisting) {
        this();
        this.journalFolder = folder;
        this.journalName = name;
        createMessageJournal(reuseExisting);
    }

    public synchronized long getRecordCount() {
        return this.recordIndex.size();
    }

    public synchronized long getEmptyCount() {
        return this.emptyIdx.size();
    }

    public String getName() {
        return this.journalName;
    }

    public String getFolder() {
        return this.journalFolder;
    }

    public long getFilesize() {
        synchronized (this) {
            try {
                return journal.length();
            }
            catch (Exception e) {
                return 0;
            }
        }
    }

    protected boolean createMessageJournal(boolean reuseExisting) {
        //logger.trace();

        try {
            // First create the directory
            File queuePath = new File(journalFolder);
            queuePath.mkdir();

            // Next create the message journal
            StringBuffer filename = new StringBuffer(journalFolder);
            filename.append(File.separator);
            filename.append(journalName); // queue or server
            filename.append("journal");

            // If the journal file already exists, rename it unless we're
            // supposed to reuse the existing file and its contents
            boolean fileExists = false;
            try {
                File file = new File(filename.toString());
                fileExists = file.exists();
                if (fileExists && !reuseExisting) {
                    File newFile = new File(filename.toString() + "_prev");
                    logger.info("Moving journal " + filename + " to " + newFile.getName());
                    file.renameTo(newFile);
                }
            }
            catch (Exception e) {
            }

            journal = new RandomAccessFile(filename.toString(), "rw");

            if (fileExists && reuseExisting) {
                indexJournal();

                if (JetStream.fineLevelLogging) {
                    StringBuffer sb = new StringBuffer();
                    sb.append("Initializized journal '");
                    sb.append(journalName);
                    sb.append("', existing filename=");
                    sb.append(filename);
                    logger.info(sb.toString());
                }
            }
            else {
                if (JetStream.fineLevelLogging) {
                    StringBuffer sb = new StringBuffer();
                    sb.append("Created journal '");
                    sb.append(journalName);
                    sb.append("', filename=");
                    sb.append(filename);
                    logger.info(sb.toString());
                }

                //
                // Write some journal header data
                //

                // write the journal name to the file
                journal.writeUTF(journalName);

                // identify the journal as belonging to this server's run
                // (to avoid it from being recovered by the recovery thread)
                journal.writeLong(JetStream.startTime);
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception:", e);
        }

        return false;
    }

    // Empties the journal file and resets indexes
    public void resetJournal() {
        try {
            // Clear the existing indexes first
            //
            recordIndex.clear();
            for (LinkedList<Long> val : emptyIdx.values()) {
                val.clear();
            }
            emptyIdx.clear();

            // Reset the file pointer and length
            journal.setLength(0);
            journal.seek(0);

            //
            // Write some journal header data
            //

            // write the journal name to the file
            journal.writeUTF(journalName);

            // identify the journal as belonging to this server's run
            // (to avoid it from being recovered by the recovery thread)
            journal.writeLong(JetStream.startTime);
        }
        catch (EOFException eof) {
        }
        catch (IOException io) {
        }
    }

    // Iterate through the contents of the journal and create
    // an index for the live records, and an index for the
    // empty records
    private void indexJournal() {
        if (JetStream.fineLevelLogging) {
            logger.info("Indexing existing journal");
        }

        // Clear the existing indexes first
        //
        recordIndex.clear();
        for (LinkedList<Long> val : emptyIdx.values()) {
            val.clear();
        }
        emptyIdx.clear();

        try {
            String name = journal.readUTF();
            Long createTime = journal.readLong();

            while (true) {
                //
                // Attempt to read a record
                //

                // Read the header fields
                byte[] bytes = null;
                String text = "";

                // Read the next record, recording the current offset
                long offset = journal.getFilePointer();
                byte active = journal.readByte();
                byte type = journal.readByte();
                int size = journal.readInt();

                String msgId = "";

                if (active == Persistence.INACTIVE_RECORD) {
                    // Inactive record, index it and then skip over it
                    storeEmptyRecord(offset, size);
                    journal.skipBytes(size);
                }
                else {
                    // This is an active record, get the msg id and data
                    //msgId = journal.readUTF();
                    bytes = new byte[journal.readInt()];
                    journal.read(bytes);
                    msgId = new String(bytes);

                    // Store the journal record offset in the index
                    recordIndex.put(msgId, offset);

                    // Read the data fields (msgID & data)
                    if (type == 2) {   // String
                        //text = journal.readUTF();
                        bytes = new byte[journal.readInt()];
                        journal.read(bytes);
                        text = new String(bytes);
                    }
                    else {              // Bytes
                        bytes = new byte[size - msgId.length()];
                        journal.read(bytes);
                    }
                }
            }
        }
        catch (EOFException eof) {
        }
        catch (IOException io) {
        }
    }

    public boolean persistMessage(Message msg) {
        //logger.trace();

        // Each message is written to a file with the following
        // record structure:
        //
        // HEADER:
        //   Boolean    - Active record indicator
        //   Byte       - Message type (0=Empty, 1=Bytes, 2=String)
        //   Integer    - Size of record's payload (not header)
        //
        // DATA:
        //   String     - Unique message ID
        //   Byte array - The message payload
        //
        try {
            if (trackPerformance) {
                JetStream.getInstance().startIntervalTimer(false);
            }

            JetStreamMessage jsMsg = (JetStreamMessage) msg;

            // Grab the payload and determine the record size (msgID + data)
            int recordSize = 0;
            String text = null;
            byte[] bytes = null;
            if (msg instanceof JetStreamObjectMessage) {
                bytes = ((JetStreamObjectMessage) msg).getBytes();
                recordSize += bytes.length;
            }
            else {
                JetStreamTextMessage textMsg = (JetStreamTextMessage) msg;
                text = textMsg.getText();
                recordSize += text.length() + 4;
            }

            // The msgId is part of the data (and the data size)
            recordSize += jsMsg.getMsgId().length() + 2;

            synchronized (this) {
                // Always persist messages at the end of the journal unless
                // there's an empty position large enough within the journal
                JournalLocationData location = getStorageLocation(recordSize);
                if (location.offset == -1) {
                    // We need to add to the end of the journal. Seek there
                    // now only if we're not already there
                    long currentPos = journal.getFilePointer();
                    long jounralLen = journal.length();
                    if (jounralLen != currentPos) {
                        journal.seek(jounralLen);
                    }

                    location.offset = jounralLen;
                }
                else {
                    // Seek to the returned insertion point
                    journal.seek(location.offset);
                }

                //
                // Store the record by key and save the offset
                //

                // Fist begin writing the header
                journal.writeByte(Persistence.ACTIVE_RECORD); // indicate an active record

                if (msg instanceof JetStreamObjectMessage) {
                    // more header information
                    journal.writeByte(Persistence.OBJ_RECORD); // save message type OBJECT
                    journal.writeInt(recordSize);

                    // Data portion
                    journal.writeInt(jsMsg.getMsgId().length());
                    journal.write(jsMsg.getMsgId().getBytes());
                    journal.write(bytes);
                }
                else {
                    // more header information
                    journal.writeByte(Persistence.TEXT_RECORD); // save message type TEXT
                    journal.writeInt(recordSize);

                    // Data portion
                    journal.writeInt(jsMsg.getMsgId().length());
                    journal.write(jsMsg.getMsgId().getBytes());
                    journal.writeInt(text.length());
                    journal.write(text.getBytes());
                }

                // Next, see if we need to append an empty record if we inserted
                // this new record at an empty location
                if (location.newEmptyRecordSize != -1) {
                    // Simply write a header
                    journal.writeByte(Persistence.INACTIVE_RECORD); //inactive record
                    journal.writeByte(Persistence.EMPTY_RECORD); // save message type EMPTY
                    journal.writeInt(location.newEmptyRecordSize);
                }

                // Store the journal record offset in the index
                recordIndex.put(jsMsg.getMsgId(), location.offset);

                if (JetStream.fineLevelLogging) {
                    StringBuffer sb = new StringBuffer("Saved msg to journal");
                    sb.append("\n record size=");
                    sb.append(recordSize);
                    sb.append("\n record offset=");
                    sb.append(location.offset);
                    sb.append("\n total journal size=");
                    sb.append(journal.length());
                    logger.info(sb.toString());
                }
            }

            if (trackPerformance) {
                trackWorstPersistTime(JetStream.getInstance().endIntervalTimer(false));
            }

            return true;
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception:", e);
        }

        return false;
    }

    private void trackWorstPersistTime(long interval) {
        if (interval > worstPersistTime) {
            worstPersistTime = interval;
        }
        if (++persistCount >= 1000) {
            persistCount = 0;
            logger.info("Worst persist time=" + worstPersistTime);
            worstPersistTime = 0;
        }
    }

    private JournalLocationData getStorageLocation(int recordLength) {
        JournalLocationData location = new JournalLocationData();
        location.offset = -1;   // where to write new record
        location.newEmptyRecordSize = -1; // Left over portion of empty space

        try {
            // Determine if there's an empty location to insert this new record
            // There are a few criteria. The empty location must either be
            // an exact fit for the new record with its header (replacing the
            // existing empty record's header, or if the empty record is larger,
            // it must be large enough for the new record's header and data,
            // and another header to mark the empty record so the file can
            // be traversed at a later time. In other words, the journal
            // consists of sequential records, back-to-back, with no gap in
            // between, otherwise it cannot be traversed from front to back
            // without adding a substantial amount of indexing within the file.
            // Therefore, even deleted records still exist within the journal,
            // they are simply marked as deleted. But the record size is still
            // part of the record so it can be skipped over when read back in

            // First locate an appropriate location. It must match exactly
            // or be equal in size to the record to write and a minimal record
            // which is just a header (5 bytes). So, HEADER + DATA + HEADER,
            // or (data length + 10 bytes).

            // Is there an exact match?
            LinkedList<Long> records = emptyIdx.get(recordLength);
            if (records != null && !records.isEmpty()) {
                location.offset = records.remove();

                // No need to append an empty record, just return offset
                location.newEmptyRecordSize = -1;
                return location;
            }

            // No exact size match, find one just large enough
            for (Integer size : this.emptyIdx.keySet()) {
                if (size >= recordLength + (Header.HEADER_SIZE * 2)) {
                    records = emptyIdx.get(size);
                    //if ( records == null ) {
                    if (records == null || records.size() == 0) {
                        // This was the last empty record of this size
                        // so delete the entry in the index and continue
                        // searching for a larger empty region (if any)
                        emptyIdx.remove(size);
                        continue;
                    }

                    location.offset = records.remove();

                    // We need to append an empty record after the new record
                    // taking the size of the header into account
                    location.newEmptyRecordSize =
                            (size - recordLength) - Header.HEADER_SIZE;

                    // Store the new empty record's offset
                    storeEmptyRecord(location.offset + recordLength,
                            location.newEmptyRecordSize);

                    return location;
                }
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
        }

        return location;
    }

    public boolean deleteMessage(Message msg) {
        JetStreamMessage jsMsg = (JetStreamMessage) msg;
        return deleteMessage(jsMsg.getMsgId());
    }

    public boolean deleteMessage(String msgId) {
        Long offset = (long) -1;
        byte type = 0;
        int length = -1;
        long filesize = -1;

        try {
            if (trackPerformance) {
                JetStream.getInstance().startIntervalTimer(false);
            }

            synchronized (this) {
                filesize = journal.length();

                // Locate the message in the journal
                offset = recordIndex.get(msgId);
                if (offset == null) {
                    return false;
                }
                journal.seek(offset);

                // Set the record as inactive and read the rest of the header
                journal.writeByte(Persistence.INACTIVE_RECORD);
                type = journal.readByte();
                length = journal.readInt();

                // Store the empty record for later use
                storeEmptyRecord(offset, length);

                // perhaps zero-out the bytes?

                // Remove from the journal index
                recordIndex.remove(msgId);
            }

            if (trackPerformance) {
                trackWorstDeleteTimes(JetStream.getInstance().endIntervalTimer(false));
            }

            return true;
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception:", e);
            logger.severe("deleteMessage data, offset=" + offset + ", type=" + type + ", length=" + length + ", filesize=" + filesize);
            try {
                logger.severe("current journal data, filePointer=" + journal.getFilePointer()
                        + ", filesize=" + journal.length());
            }
            catch (Exception e1) {
            }
        }

        return false;
    }

    private void trackWorstDeleteTimes(long interval) {
        if (interval > worstDeleteTime) {
            worstDeleteTime = interval;
        }
        if (++deleteCount >= 1000) {
            deleteCount = 0;
            logger.info("Worst delete time=" + worstDeleteTime);
            worstDeleteTime = 0;
        }
    }

    // Returns the list of active messages from the journal with
    // sending order preserved
    public TreeMap<String, Message> getMessages() {
        synchronized (this) {
            TreeMap<String, Message> messages = new TreeMap<String, Message>();

            for (String msgId : this.recordIndex.keySet()) {
                messages.put(msgId, getRecord(msgId));
            }

            return messages;
        }
    }

    public Message getRecord(String key) {
        //logger.info("getRecord called");
        JetStreamMessage msg = null;
        try {
            Long offset = recordIndex.get(key);
            if (offset != null) {
                // Jump to this record's offset within the journal file
                journal.seek(offset);

                // First, read in the header
                byte active = journal.readByte();
                byte type = journal.readByte();
                int recordLength = journal.readInt();

                // Next, read in the data (msgID & data)
                byte[] bytes = new byte[journal.readInt()];
                journal.read(bytes);
                String msgId = new String(bytes);
                //String msgId = journal.readUTF();

                String text = null;
                if (type == Persistence.TEXT_RECORD) {
                    // String
                    bytes = new byte[journal.readInt()];
                    journal.read(bytes);
                    text = new String(bytes);
                    //text = journal.readUTF();
                }
                else {
                    // Bytes
                    bytes = new byte[recordLength - msgId.length()];
                    journal.read(bytes);
                }

                // Reconstruct a message object from the data
                if (text != null) {
                    msg = new JetStreamTextMessage(text);
                }
                else {
                    msg = new JetStreamObjectMessage(bytes);
                }

                msg.setMsgId(msgId);
                msg.setFilename("");
                msg.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
        }

        return msg;
    }

    private void storeEmptyRecord(long offset, int length) {
        // Store the empty record in an index. Look to see if there
        // are other records of the same size (in a LinkedList). If
        // so, add this one to the end of the linked list
        //
        LinkedList<Long> emptyRecs = emptyIdx.get(length);
        if (emptyRecs == null) {
            // There are no other records of this size. Add an entry
            // in the hash table for this new linked list of records
            emptyRecs = new LinkedList<Long>();
            emptyIdx.put(length, emptyRecs);
        }

        // Add the pointer (file offset) to the new empty record
        emptyRecs.add(offset);
    }
}
