package com.yahoo.ycsb.db;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.Map.Entry;
import java.io.File;

import com.yahoo.ycsb.*;

import com.yahoo.ycsb.measurements.Measurements;
import fct.thesis.database.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import thrift.server.AbortException;
import thrift.server.DBService;
import thrift.server.NoSuchKeyException;

/**
 * Created by gomes on 17/03/15.
 */
public class DatabaseClient extends DB implements TxDB {

    private Properties props;
    TTransport transport;
    DBService.Client client;

    public DatabaseClient() {}

    private static final int OK = 0;
    private static final int ERROR = -1;

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    public void init() throws DBException {
        props = getProperties();

        String ip = props.getProperty(Config.SERVER_IP,Config.SERVER_IP_DEFAULT);
        int port = Integer.parseInt(props.getProperty(Config.SERVER_PORT,Config.SERVER_PORT_DEFAULT));

        System.out.println("Connect to: "+ip+":"+port);

        try {
            transport = new TSocket(ip, port);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            client = new DBService.Client(protocol);
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException x) {
            x.printStackTrace();
        }
    }

    @Override
    public void cleanup() throws DBException {
        super.cleanup();
        transport.close();
    }

    @Override
    public UUID beginTx() {
        try {
            return new UUID(0L,client.txn_begin());
        } catch (TException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public int read(String table, String key, Set<String> fields,
                    HashMap<String, ByteIterator> result) {
        try {
            Map<String, ByteBuffer> v = client.get(key);

            if (v != null) {
                if (fields != null) {
                    for (String field : fields) {
                        result.put(field, new StringByteIterator(new String(v.get(field).array(), Charset.forName("UTF-8"))));
                    }
                } else {
                    for (String field : v.keySet()) {
                        result.put(field, new StringByteIterator(new String(v.get(field).array(), Charset.forName("UTF-8"))));
                    }
                }
            }

        } catch (NoSuchKeyException e) {
            return OK;
        } catch (AbortException e){
            return ERROR;
        } catch (TException e) {
            e.printStackTrace();
            return ERROR;
        }

        return OK;
    }

    @Override
    public int scan(String table, String startkey, int recordcount,
                    Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        System.out.println("scan " + recordcount + " records from key: " + startkey + " on table: " + table);
        return OK;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
//        System.out.println("updatekey: " + key + " from table: " + table);

        Map<String, ByteBuffer> v = null;
        try {
            try {
                v = client.get(key);
            } catch (NoSuchKeyException e) {
            }

            if (v!=null) {
                if (values != null) {
                    String value = "";
                    for (String k : values.keySet()) {
                        value = values.get(k).toString();
                        v.put(k, ByteBuffer.wrap(value.getBytes(Charset.forName("UTF-8"))));
                    }
                    client.put(key, v);
                }
            }
        } catch (AbortException e){
            return ERROR;
        } catch (TException e) {
            e.printStackTrace();
            return ERROR;
        }

        return OK;
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {

        try {
            Map<String, ByteBuffer> v = new HashMap<String, ByteBuffer>();
            String value = "";
            for (String k : values.keySet()) {
                value = values.get(k).toString();
                v.put(k, ByteBuffer.wrap(value.getBytes(Charset.forName("UTF-8"))));
            }

            client.put(key, v);
        } catch (AbortException e){
            return ERROR;
        } catch (TException e) {
            e.printStackTrace();
            return ERROR;
        }
        return OK;
    }

    @Override
    public int delete(String table, String key) {
        System.out.println("deletekey: " + key + " from table: " + table);
        return OK;
    }

    @Override
    public int commit(UUID txid) {
        try {
            if (client.txn_commit())
                return OK;
            else
                return ERROR;
        } catch (AbortException e){
            return ERROR;
        } catch (TException e) {
            e.printStackTrace();
            return ERROR;
        }
    }


}
