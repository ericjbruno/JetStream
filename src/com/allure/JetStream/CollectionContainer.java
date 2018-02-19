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
 * 
 * 
 * Description:  This class acts as a container of data used in the MapMessage
 * and Message classes for message body and property storage 
 */
package com.allure.JetStream;

import java.util.*;
import java.io.*;

public class CollectionContainer implements Serializable {

    public Hashtable objVals = new Hashtable();

    public CollectionContainer() {
    }

    protected long getNumeric(String name) throws Exception {
        if (objVals != null) {
            Object val = objVals.get(name);
            if (val != null) {
                if (val instanceof Long) {
                    return ((Long) val).longValue();
                }
                if (val instanceof Integer) {
                    return ((Integer) val).intValue();
                }
                if (val instanceof Short) {
                    return ((Short) val).shortValue();
                }
                if (val instanceof Byte) {
                    return ((Byte) val).byteValue();
                }
            }
        }

        return 0;
    }

    public boolean getBoolean(String name) {
        if (objVals != null) {
            Boolean val = (Boolean) objVals.get(name);
            if (val != null) {
                return val.booleanValue();
            }
        }

        return false;
    }

    public byte getByte(String name) throws Exception {
        return (byte) getNumeric(name);
    }

    public short getShort(String name) throws Exception {
        return (short) getNumeric(name);
    }

    public int getInt(String name) throws Exception {
        return (int) getNumeric(name);
    }

    public long getLong(String name) throws Exception {
        return getNumeric(name);
    }

    public float getFloat(String name) throws Exception {
        if (objVals != null) {
            Float val = (Float) objVals.get(name);
            if (val != null) {
                return val.floatValue();
            }
        }

        return 0;
    }

    public double getDouble(String name) throws Exception {
        if (objVals != null) {
            Object val = objVals.get(name);
            if (val != null) {
                if (val instanceof Double) {
                    return ((Double) val).doubleValue();
                }
                if (val instanceof Float) {
                    return ((Float) val).floatValue();
                }
            }
        }

        return 0;
    }

    public char getChar(String name) throws Exception {
        if (objVals != null) {
            Character val = (Character) objVals.get(name);
            if (val != null) {
                return val.charValue();
            }
        }

        return ' ';
    }

    public String getString(String name) throws Exception {
        if (objVals != null) {
            Object val = objVals.get(name);
            if (val != null) {
                return val.toString();
            }
        }

        return null;
    }

    public byte[] getBytes(String name) throws Exception {
        if (objVals != null) {
            Object val = objVals.get(name);
            if (val != null) {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(val);
                oos.close();
                bos.close();
                return bos.toByteArray();
            }
        }

        return null;
    }

    public Object getObject(String name) throws Exception {
        if (objVals != null) {
            return objVals.get(name);
        }

        return null;
    }

    public Enumeration getMapNames() throws Exception {
        if (objVals != null) {
            return objVals.keys();
        }

        return null;
    }

    public void setBoolean(String name, boolean value) {
        objVals.put(name, new Boolean(value));
    }

    public void setByte(String name, byte value) {
        objVals.put(name, new Byte(value));
    }

    public void setShort(String name, short value) {
        objVals.put(name, new Short(value));
    }

    public void setChar(String name, char value) {
        objVals.put(name, new Character(value));
    }

    public void setInt(String name, int value) {
        objVals.put(name, new Integer(value));
    }

    public void setLong(String name, long value) {
        objVals.put(name, new Long(value));
    }

    public void setFloat(String name, float value) {
        objVals.put(name, new Float(value));
    }

    public void setDouble(String name, double value) {
        objVals.put(name, new Double(value));
    }

    public void setString(String name, String value) {
        objVals.put(name, new String(value));
    }

    public void setBytes(String name, byte[] value) throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream(value);
        ObjectInputStream inStream = new ObjectInputStream(bais);
        Serializable serObj = (Serializable) inStream.readObject();
        inStream.close();
        objVals.put(name, serObj);
    }

    public void setBytes(String name, byte[] value, int offset, int length) 
			throws Exception {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = value[offset + i];
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream inStream = new ObjectInputStream(bais);
        Serializable serObj = (Serializable) inStream.readObject();
        inStream.close();
        objVals.put(name, serObj);
    }

    public void setObject(String name, Object value) {
        // Sets an object value with the specified name into the Map.
        objVals.put(name, value);
    }

    public boolean itemExists(String name) {
        // Indicates whether an item exists in this object.
        //return objVals.contains(name);
        return objVals.containsKey(name);
    }

    public void clear() {
        objVals.clear();
    }
}