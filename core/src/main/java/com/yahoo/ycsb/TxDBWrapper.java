/**
 * Copyright (c) 2013 NOVA-LINCS
 * @Author Joao Leitao & Henrique Moniz
 */

package com.yahoo.ycsb;

import java.util.HashMap;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

import com.yahoo.ycsb.measurements.Measurements;

public class TxDBWrapper extends DBWrapper implements TxDB {

	TxDB txdb;
	Measurements _measurements;
	
	public TxDBWrapper(DB db) {
		super(db);
		txdb = (TxDB) db;
		_measurements=Measurements.getMeasurements();
	}
	
	

	@Override
	public UUID beginTx() {
		long st=System.nanoTime();
		UUID txid = txdb.beginTx();
		long en=System.nanoTime();
		_measurements.measure("BEGIN",(int)((en-st)/1000));
		return txid;
	}

	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		
		return super.read(table, key, fields, result);
	}

	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		
		return super.scan(table, startkey, recordcount, fields, result);
	}

	@Override
	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {
		
		return super.update(table, key, values);
	}

	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {
		
		return super.insert(table, key, values);
	}

	@Override
	public int delete(String table, String key) {
		return super.delete(table, key);
	}

	@Override
	public int commit(UUID txid) {
		long st=System.nanoTime();
		int res = txdb.commit(txid);
		long en=System.nanoTime();
		_measurements.measure("COMMIT",(int)((en-st)/1000));
		_measurements.reportReturnCode("COMMIT",res);
		return res;
	}

}
