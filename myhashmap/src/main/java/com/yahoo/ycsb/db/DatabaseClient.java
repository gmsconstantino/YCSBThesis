package com.yahoo.ycsb.db;

import java.util.*;

import com.yahoo.ycsb.*;

import com.yahoo.ycsb.measurements.Measurements;
import fct.thesis.database.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import thrift.DatabaseSingleton;

/**
 * Created by gomes on 17/03/15.
 */
public class DatabaseClient extends DB implements TxDB {

    Database<Integer, HashMap<String, String>> db;
    final TransactionFactory.type TYPE = DatabaseSingleton.getInstance().getType();
    Transaction<Integer, HashMap<String, String>> t;

    public DatabaseClient() {}

    private static final int OK = 0;
    private static final int ERROR = -1;

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    public void init() throws DBException {
        db = DatabaseSingleton.getDatabase();
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void cleanup() throws DBException
    {
    }

    @Override
    public UUID beginTx() {
        t = db.newTransaction(TYPE);
        return new UUID(0L,t.getId());
    }

    @Override
    public int read(String table, String key, Set<String> fields,
                    HashMap<String, ByteIterator> result) {
        try {
            HashMap<String, String> v = t.get(Integer.parseInt(key));

            if (v != null) {
                if (fields != null) {
                    for (String field : fields) {
                        result.put(field, new StringByteIterator(v.get(field)));
                    }
                } else {
                    for (String field : v.keySet()) {
                        result.put(field, new StringByteIterator(v.get(field)));
                    }
                }
            }

        } catch(TransactionTimeoutException e){
            return ERROR;
        } catch (TransactionAbortException e){
            return ERROR;
//        } catch (NullPointerException e){ //TODO: null devido ao GC nmsi mal feito
//            return ERROR;
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

        HashMap<String,String> v = new HashMap<String, String>();
        try {
            if (values != null) {
                String value = "";
                for (String k : values.keySet()) {
                    value = values.get(k).toString();
                    v.put(k, value);
                }
                t.put(Integer.parseInt(key), v);
            } else {
                return ERROR;
            }
        } catch(TransactionTimeoutException e){
            return ERROR;
        } catch (TransactionAbortException e){
            return ERROR;
//        } catch (NullPointerException e){ //TODO: null devido ao GC nmsi mal feito
//            return ERROR;
        }

        return OK;
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {

        try {
            HashMap<String, String> v = new HashMap<String, String>();
            String value = "";
            for (String k : values.keySet()) {
                value = values.get(k).toString();
                v.put(k, value);
            }

            t.put(Integer.parseInt(key), v);
        } catch(TransactionTimeoutException e){
            return ERROR;
        } catch (TransactionAbortException e){
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
            if (t.commit())
                return OK;
            else
                return ERROR;
        } catch(TransactionTimeoutException e){
            return ERROR;
        } catch (TransactionAbortException e){
            return ERROR;
        }
    }


}
