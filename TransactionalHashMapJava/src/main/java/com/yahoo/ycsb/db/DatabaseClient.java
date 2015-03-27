package com.yahoo.ycsb.db;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.io.File;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

import database.Database;
import database.Transaction;
import database.TransactionFactory;

/**
 * Created by gomes on 17/03/15.
 */
public class DatabaseClient extends DB {

    public static final String VERBOSE="myhashmap.verbose";
    public static final String VERBOSE_DEFAULT="true";

    Database<String, HashMap<String, String>> db;
    final TransactionFactory.type TYPE = dbSingleton.getInstance().getType();

    boolean verbose;

    public DatabaseClient() {}

    private static final int OK = 0;
    private static final int ERROR = 1;

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    public void init() throws DBException {
        db = dbSingleton.getDatabase();
        verbose = Boolean.parseBoolean(getProperties().getProperty(VERBOSE, VERBOSE_DEFAULT));
    }

    @Override
    public int read(String table, String key, Set<String> fields,
                    HashMap<String, ByteIterator> result) {

        if (verbose)
            System.out.print("READ "+table+" "+key+" [ ");

        Transaction<String, HashMap<String, String>> t = db.newTransaction(TYPE);
        HashMap<String,String> v = t.get(key);
        if(!t.commit()) {
            if (verbose)
                System.out.println("] - Abort");
            return ERROR;
        }

        if (v != null) {
            if (fields != null) {
                for (String f : fields) {
                    if (verbose)
                        System.out.print(f + "=" + v.get(f).toString() + " ");
                }
            } else {
                for (String f : v.keySet()) {
                    if (verbose)
                        System.out.print(f + "=" + v.get(f).toString() + " ");
                }
            }
        }

        if (verbose)
            System.out.println("]");
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

        Transaction<String, HashMap<String, String>> t = db.newTransaction(TYPE);
        HashMap<String,String> v = t.get_to_update(key);

        if (v!=null) {
            if (verbose)
                System.out.print("UPDATE " + table + " " + key + " [ ");
            if (values != null) {
                String value = "";
                for (String k : values.keySet()) {
                    value = values.get(k).toString();
                    if (verbose)
                        System.out.print(k + "=" + value + " ");
                    v.put(k, value);
                }
                t.put(key, v);
            }
            if (verbose)
                System.out.println("]");
        }

        if(t.commit())
            return OK;
        else
            return ERROR;
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {

        HashMap<String,String> v = new HashMap<String, String>();
        String value = "";
        for (String k : values.keySet()){
            value = values.get(k).toString();
            v.put(k,value);
        }

        if (verbose)
            System.out.println("insertkey: " + key + " from table: " + table + " values: "+v.toString());

        Transaction t = db.newTransaction(TYPE);
        t.put(key, v);
        if(t.commit())
            return OK;
        else
            return ERROR;
    }

    @Override
    public int delete(String table, String key) {
        System.out.println("deletekey: " + key + " from table: " + table);
        return OK;
    }


}
