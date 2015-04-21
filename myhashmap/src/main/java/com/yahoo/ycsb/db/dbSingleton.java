package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import fct.thesis.database.Database;
import fct.thesis.database.DatabaseFactory;
import fct.thesis.database.TransactionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Created by gomes on 26/03/15.
 */
public class dbSingleton {

    private static final Logger logger = LoggerFactory.getLogger(dbSingleton.class);

    Database<Long, HashMap<String, ByteIterator>> db;
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
        db = (Database<Long, HashMap<String, ByteIterator>>) DatabaseFactory.createDatabase(type);
    }

    private Database<Long, HashMap<String, ByteIterator>> getDb() {
        return db;
    }

    public TransactionFactory.type getType() {
        return type;
    }

    public void setType(TransactionFactory.type type) {
        this.type = type;
        db = (Database<Long, HashMap<String, ByteIterator>>) DatabaseFactory.createDatabase(type);
    }
}
