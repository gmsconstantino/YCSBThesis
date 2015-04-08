package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import database.Database;
import database.TransactionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Created by gomes on 26/03/15.
 */
public class dbSingleton {

    private static final Logger logger = LoggerFactory.getLogger(dbSingleton.class);

    Database<String, HashMap<String, ByteIterator>> db;
    TransactionFactory.type type;

    private static dbSingleton ourInstance = null;

    public static dbSingleton getInstance() {
        if (ourInstance == null){
            logger.info("Created DB Singleton");
            ourInstance = new dbSingleton();
        }
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
