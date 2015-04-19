/**
 * Copyright (c) 2013 NOVA-LINCS
 * @Author Joao Leitao & Henrique Moniz
 */

package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.*;
import com.yahoo.ycsb.measurements.Measurements;

import java.util.*;

public class MyTxWorkload extends TxWorkload {


    
    /**
     * Do one insert operation. Because it will be called concurrently from multiple client threads, this 
     * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each 
     * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
     * effects other than DB operations and mutations on threadstate. Mutations to threadstate do not need to be
     * synchronized, since each thread has its own threadstate instance.
     */
    @Override
    public boolean doInsert(DB db, Object threadstate) {
    	TxDB txdb = (TxDB) db;
		int keynum=keysequence.nextInt();
		
		UUID txid = txdb.beginTx();
		txdb.insert(table, buildKeyName(keynum), txBuildValues());
		if(txdb.commit(txid) != 0)
			return false;
    	
    	return true;
    }
    
	@Override
	public boolean doTransaction(DB db, Object threadstate) {
		TxDB txdb = (TxDB) db;
		int txSize = transactionSize.nextInt();
		int numberOfReads = (int) Math.round(txSize * txread);
		int numberOfWrites = txSize - numberOfReads;
		Vector<Integer> keysRead = new Vector<Integer>();
		Vector<Integer> keysWrite = new Vector<Integer>();
		int k;
		for(int i = 0; i < numberOfReads; i++) {
			k = txNextKeynum();
			while(keysRead.contains(k)) k = txNextKeynum();
			keysRead.add(k);
		}
		int i = 0;
		for(; i< numberOfWrites; i++) {
			k = txNextKeynum();
			while(keysWrite.contains(k) && keysRead.contains(k)) k = txNextKeynum();
			keysWrite.add(k);
		}
		
		Set<String> fields = new HashSet<String>(1);
		fields.add(field);
		
		HashMap<String, ByteIterator> values = txBuildValues();

		//do the transaction here.
		long st = System.nanoTime();
		
		UUID tid = txdb.beginTx();
		
		for(Integer key: keysRead) {
			txdb.read(table, buildKeyName(key), fields, new HashMap<String,ByteIterator>());
		}
		
		for(Integer key: keysWrite) {
			txdb.update(table, buildKeyName(key), values);
		}
		
		if(txdb.commit(tid) == 0) {
			long en = System.nanoTime();
			Measurements.getMeasurements().measure("Tx", (int)((en-st)/1000));		
			Measurements.getMeasurements().reportReturnCode("Tx", 0);
		} else {
			Measurements.getMeasurements().reportReturnCode("Tx", -1);
		}
		return true;
	}
}
