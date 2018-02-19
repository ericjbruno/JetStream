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

import com.allure.JetStream.JetStream;
import com.allure.JetStream.JetStreamMessage;
import com.allure.JetStream.JetStreamObjectMessage;
import com.allure.JetStream.JetStreamTextMessage;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.DeliveryMode;
import javax.jms.Message;

public class NIOPersistence implements Persistence {

    private static final Logger logger = Logger.getLogger("RAFPersistence");
    public static String JOURNAL_VERSION = "JETSTREAMJOURNALVERSION_2";

    // The message journal is where persisted messages are stored
    // Intend to move the bulk of the internals out of this class
    // and hide implementation within its own class
    //
    protected RandomAccessFile journal = null;
    protected FileChannel channel = null;
    protected MappedByteBuffer mbb = null;
    protected static final int PAGE_SIZE = 1024 * 1024; // 1 megabyte page size
    protected long currentEnd = 0;

    // Keep an index by name of each destination in the journal
    //
    public HashMap<String, Long> recordIndex =
            new HashMap<String, Long>();

    // Keep an index of LinkedList, where the index key is the size
    // of the empty record. The LinkedList contains pointers (offsets) to
    // the empty records
    //
    public TreeMap<Integer, LinkedList<Long>> emptyIdx =
            new TreeMap<Integer, LinkedList<Long>>();
    
    public static byte[] clearBytes = null;
    
    public static class Header {
        //   Boolean    - Active record indicator
        //   Byte       - Message type (0=Empty, 1=Bytes, 2=String)
        //   Integer    - Size of record's payload (not header)

        byte active;            // 1 byte
        int clientPort;         // 4 bytes
        int clientLocalPort;    // 4 bytes
        byte type;              // 1 byte
        int size;               // 4 bytes
        public static final int HEADER_SIZE = 14;
    }

    /*
     private static final int DEFAULT_DATASIZE = 65536;
     private ByteBuffer header = ByteBuffer.allocateDirect(Header.HEADER_SIZE);
     private ByteBuffer header2 = ByteBuffer.allocateDirect(Header.HEADER_SIZE);
     private ByteBuffer data = ByteBuffer.allocateDirect(DEFAULT_DATASIZE);

     // just a new record
     private ByteBuffer[] srcs = { header, data };
     // a new record and an empty record header
     private ByteBuffer[] srcs2 = { header, data, header2 };
     */
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
    private NIOPersistence() {
    }

    public NIOPersistence(String folder, String name) {
        this(folder, name, false);
    }

    public NIOPersistence(String folder, String name, boolean reuseExisting) {
        logger.setParent(JetStream.getInstance().getLogger());
        this.journalFolder = folder;
        this.journalName = name;
        createMessageJournal(reuseExisting);
    }

    @Override
    public synchronized long getRecordCount() {
        return this.recordIndex.size();
    }

    @Override
    public synchronized long getEmptyCount() {
        return this.emptyIdx.size();
    }

    @Override
    public String getName() {
        return this.journalName;
    }

    @Override
    public String getFolder() {
        return this.journalFolder;
    }

    @Override
    public synchronized long getFilesize() {
        try {
            //return journal.length();
            return channel.size();
        }
        catch (Exception e) {
            return 0;
        }
    }

    protected final boolean createMessageJournal(boolean reuseExisting) {
        //logger.trace();

        try {
            // First create the directory
            File queuePath = new File(journalFolder);
            boolean created = queuePath.mkdir();
            if (!created) {
                // It may have failed because the directory already existed
                if (!queuePath.exists()) {
                    System.out.println("!!!! DIRECTORY CREATE FAILED");
                }
            }

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

            //System.out.println("*** Creating journal: " + filename.toString());
            journal = new RandomAccessFile(filename.toString(), "rw");
            long filesize = PAGE_SIZE;
            journal.setLength(filesize);

            channel = journal.getChannel();
            mbb = channel.map(FileChannel.MapMode.READ_WRITE, 0, filesize);

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
                writeJournalHeader(journal);

                currentEnd = journal.getFilePointer();
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
        }

        return false;
    }

    // Empties the journal file and resets indexes
    @Override
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
            try {
                if (JetStream.fineLevelLogging) {
                    logger.info("Setting journal length to zero (to clear)");
                }

                journal.setLength(0);
                journal.setLength(PAGE_SIZE);
            }
            catch (Exception e) {
                // Under Windows, you cannot reset the file using
                // setLength. We need to zero out the file manually
                if (JetStream.fineLevelLogging) {
                    logger.info("Filling journal with zeros (on Windows?)");
                }

                journal.seek(0);
                if (clearBytes == null) {
                    clearBytes = new byte[PAGE_SIZE];
                }
                journal.write(clearBytes);
            }

            journal.seek(0);

            //
            // Write some journal header data
            //
            writeJournalHeader(journal);

            mbb = channel.map(FileChannel.MapMode.READ_WRITE, 0, journal.length());

            currentEnd = journal.getFilePointer();
        }
        catch (EOFException eof) {
        }
        catch (IOException io) {
        }
    }

    private void writeJournalHeader(RandomAccessFile journal) throws IOException {
        // write the journal version number to the file
        journal.writeUTF(NIOPersistence.JOURNAL_VERSION);

        // write the journal name to the file
        journal.writeUTF(journalName);

        // identify the journal as belonging to this server's run
        // (to avoid it from being recovered by the recovery thread)
        journal.writeLong(JetStream.startTime);
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

        int recordSize = 0;

        try {
            long filesize = journal.length();
            String version = journal.readUTF();
            String name = journal.readUTF();
            Long createTime = journal.readLong();
            currentEnd = journal.getFilePointer();
            mbb.position((int) currentEnd);
            while (mbb.position() < filesize) {
                //
                // Attempt to read a record
                //

                // Read the header fields
                byte[] bytes;
                String text = "";

                // Read the next record, recording the current offset
                long offset = mbb.position();
                boolean active = true;
                if (mbb.get() == INACTIVE_RECORD) {
                    active = false;
                }
                int clientPort = mbb.getInt();
                int clientLocalPort = mbb.getInt();
                byte type = mbb.get();
                if (type == 0) {
                    break; // end of file
                }
                int datalen = mbb.getInt();
                recordSize = Header.HEADER_SIZE + datalen;

                String msgId;

                if (!active) {
                    // Inactive record, index it and then skip over it
                    storeEmptyRecord(offset, datalen);
                    mbb.position((int) offset + datalen);
                }
                else {
                    // This is an active record, get the msg id (not data)
                    bytes = new byte[mbb.getInt()];
                    mbb.get(bytes);
                    msgId = new String(bytes);

                    // Store the journal record offset in the index
                    recordIndex.put(msgId, offset);
                }

                // Update the end-of-data pointer
                if (offset + recordSize > currentEnd) {
                    currentEnd += recordSize;
                }
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Exception", e);
            try {
                StringBuffer sb = new StringBuffer("Persist data: ");
                sb.append(" Journal: " + journalName);
                sb.append(", length: " + channel.size());
                sb.append(", currentEnd: " + currentEnd);
                sb.append(", recordSize: " + recordSize);
                logger.info(sb.toString());
            }
            catch (Exception ee) {
            }
        }
    }

    @Override
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
        int recordSize = 0;

        try {
            if (trackPerformance) {
                JetStream.getInstance().startIntervalTimer(false);
            }

            JetStreamMessage jsMsg = (JetStreamMessage) msg;

            // Grab the payload and determine the record size (msgID + data)
            int datalen = 0;
            byte[] msgid = null;
            byte[] text = null;
            byte[] bytes = null;

            // Are there any message properties?
            /*
             if ( netMsg.props != null ) {
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
             * 
             */

            if (msg instanceof JetStreamObjectMessage) {
                bytes = ((JetStreamObjectMessage) msg).getBytes();
                datalen += bytes.length;
            }
            else {
                JetStreamTextMessage textMsg = (JetStreamTextMessage) msg;
                text = textMsg.getText().getBytes();
                datalen += text.length + 4; // 4 bytes to store strlen
            }

            // The msgId is part of the data (and the data size)
            msgid = jsMsg.getMsgId().getBytes();
            datalen += msgid.length + 4; // 4 bytes to store strlen

            recordSize = datalen + Header.HEADER_SIZE;

            synchronized (this) {
                // Always persist messages at the end of the journal unless
                // there's an empty position large enough within the journal
                JournalLocationData location = getStorageLocation(datalen);
                if (location.offset == -1) {
                    // We need to add to the end of the journal. Seek there
                    // now only if we're not already there
                    long currentPos = mbb.position();
                    if (currentPos != currentEnd) {
                        mbb.position((int) currentEnd);
                        currentPos = currentEnd;
                    }

                    // Check to see if we need to grow the journal file
                    long journalLen = channel.size();
                    if ((currentPos + recordSize) >= journalLen) {
                        // GROWING FILE BY ANOTHER PAGE
                        if (JetStream.fineLevelLogging) {
                            logger.info("Expanding journal size");
                        }
                        mbb.force();
                        journal.setLength(journalLen + PAGE_SIZE);
                        channel = journal.getChannel();
                        journalLen = channel.size();
                        mbb = channel.map(FileChannel.MapMode.READ_WRITE, 0, journalLen);

                        // Since we remmaped the file, double-check the position
                        currentPos = mbb.position();
                        if (currentPos != currentEnd) {
                            mbb.position((int) currentEnd);
                        }
                    }

                    location.offset = currentEnd;//journalLen;

                    // Only increment currentEnd when appending a record
                    currentEnd += recordSize;
                }
                else {
                    // Seek to the returned insertion point
                    mbb.position((int) location.offset);
                }

                //
                // Store the record by key and save the offset
                //

                // Fist begin writing the header
                mbb.put(ACTIVE_RECORD);// indicate an active record
                mbb.putInt(jsMsg.clientPort);
                mbb.putInt(jsMsg.clientLocalPort);

                if (msg instanceof JetStreamTextMessage) {
                    // more header information
                    mbb.put(TEXT_RECORD); // save message type TEXT
                    mbb.putInt(datalen);

                    // Data portion
                    mbb.putInt(msgid.length);
                    mbb.put(msgid);

                    mbb.putInt(text.length);
                    mbb.put(text);
                }
                else {
                    // more header information
                    mbb.put(OBJ_RECORD); // save message type OBJECT
                    mbb.putInt(datalen);

                    // Data portion
                    mbb.putInt(msgid.length);
                    mbb.put(msgid);

                    mbb.put(bytes);
                }

                //currentEnd += recordSize;

                // Next, see if we need to append an empty record if we inserted
                // this new record at an empty location
                if (location.newEmptyRecordSize == -1) {
                    // Done. No trailing empty record needed
                    if (JetStream.fineLevelLogging) {
                        logger.info("Bytes written to journal: " + recordSize);
                    }
                }
                else {
                    // Write the header and data for the new record, as well
                    // as header indicating an empty record
                    mbb.put(INACTIVE_RECORD); // inactive record
                    mbb.put(EMPTY_RECORD); // save message type EMPTY
                    mbb.putInt(location.newEmptyRecordSize);
                    if (channel.position() > currentEnd) //currentEnd += Header.HEADER_SIZE;
                    {
                        currentEnd = channel.position();
                    }

                    if (JetStream.fineLevelLogging) {
                        logger.info("Bytes written to journal: " + recordSize + Header.HEADER_SIZE);
                    }
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
            logger.log(Level.SEVERE, "Exception", e);
            logger.severe("Exception: " + e.toString());
            try {
                StringBuffer sb = new StringBuffer("Persist data: ");
                sb.append(" Journal: " + journalName);
                sb.append(", length: " + channel.size());
                sb.append(", currentEnd: " + currentEnd);
                sb.append(", recordSize: " + recordSize);
                logger.info(sb.toString());
            }
            catch (Exception ee) {
            }
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

    @Override
    public boolean deleteMessage(Message msg) {
        JetStreamMessage jsMsg = (JetStreamMessage) msg;
        return deleteMessage(jsMsg.getMsgId());
    }

    @Override
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
                filesize = channel.size();

                // Locate the message in the journal
                offset = recordIndex.get(msgId);
                if (offset == null) {
                    return false;
                }
                mbb.position(offset.intValue());

                // read the header (to get record length) then set it as inactive
                byte active = mbb.get();
                int clientPort = mbb.getInt();
                int clientLocalPort = mbb.getInt();
                type = mbb.get();
                length = mbb.getInt();

                // Mark the record as inactive by re-writing the header
                mbb.position(offset.intValue());
                mbb.put(INACTIVE_RECORD);
                mbb.putInt(0); // clientPort
                mbb.putInt(0); // clientLocalPort
                mbb.put(type);
                mbb.putInt(length);

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
            logger.log(Level.SEVERE, "Exception", e);

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

    // Returns the list of active messages from the journal with
    // sending order preserved
    @Override
    public TreeMap<String, Message> getMessages() {
        synchronized (this) {
            TreeMap<String, Message> messages = new TreeMap<String, Message>();
            if (Thread.interrupted() == true) {
                if (JetStream.fineLevelLogging) {
                    logger.info("NIO Journal being acessed by an interrupted thread - flag cleared");
                }
            }


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
                mbb.position(offset.intValue());

                // First, read in the header
                byte active = mbb.get();
                if (active != 1) {
                    return null;
                }

                int clientPort = mbb.getInt();
                int clientLocalPort = mbb.getInt();
                byte type = mbb.get();
                if (type != 1 && type != 2) {
                    return null;
                }

                int recordLength = mbb.getInt();

                // Start reading the buffer
                byte[] bytes = new byte[mbb.getInt()];
                mbb.get(bytes);
                String msgId = new String(bytes);

                String text = null;
                if (type == TEXT_RECORD) {
                    // String
                    bytes = new byte[mbb.getInt()];
                    mbb.get(bytes);
                    text = new String(bytes);
                }
                else {
                    // Bytes
                    bytes = new byte[recordLength - msgId.length() - 4];
                    mbb.get(bytes);
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
                msg.clientPort = clientPort;
                msg.clientLocalPort = clientLocalPort;
            }
            else {
                System.out.println("OFFSET IS NULL");
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
