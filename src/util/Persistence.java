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

import java.util.*;
import javax.jms.*;

public interface Persistence {

    public static final byte INACTIVE_RECORD = 0;
    public static final byte ACTIVE_RECORD = 1;
    public static final byte EMPTY_RECORD = 0;
    public static final byte OBJ_RECORD = 1;
    public static final byte TEXT_RECORD = 2;

    // Get Journal stats
    public long getRecordCount();

    public long getEmptyCount();

    public String getName();

    public String getFolder();

    public long getFilesize();

    // Operate on journal
    public boolean persistMessage(Message msg);

    public boolean deleteMessage(Message msg);

    public boolean deleteMessage(String msgId);

    public TreeMap<String, Message> getMessages();

    public Message getRecord(String key);

    public void resetJournal();
}
