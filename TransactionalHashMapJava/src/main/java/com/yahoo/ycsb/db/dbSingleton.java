package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import database.Database;
import database.TransactionFactory;

import java.util.HashMap;

/**
 * Created by gomes on 26/03/15.
 */
public class dbSingleton {

    Database<String, HashMap<String, ByteIterator>> db;
    TransactionFactory.type type;

    private static dbSingleton ourInstance = new dbSingleton();

    public static dbSingleton getInstance() {
        return ourInstance;
    }

    public static Database getDatabase() {
        return getInstance().getDb();
    }

    public static void setTransactionype(TransactionFactory.type type){
        getInstance().setType(type);
    }

    private dbSingleton() {
        type = TransactionFactory.type.TWOPL;
        db = new Database<String, HashMap<String, ByteIterator>>();
    }

    private Database<String, HashMap<String, ByteIterator>> getDb() {
        return db;
    }

    public TransactionFactory.type getType() {
        return type;
    }

    public void setType(TransactionFactory.type type) {
        this.type = type;
    }
}
