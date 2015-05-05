/**
 * Copyright (c) 2013 NOVA-LINCS
 * @Author Joao Leitao & Henrique Moniz
 */

package com.yahoo.ycsb;

import com.yahoo.ycsb.measurements.Measurements;

import java.util.HashMap;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

public class TxDBWrapperRestricted extends DBWrapper implements TxDB {

	TxDB txdb;
	Measurements _measurements;

	public TxDBWrapperRestricted(DB db) {
		super(db);
		txdb = (TxDB) db;
		_measurements=Measurements.getMeasurements();
	}

    public void cleanup() throws DBException
    {
        _db.cleanup();
    }

	@Override
	public UUID beginTx() {
		UUID txid = txdb.beginTx();
		return txid;
	}

	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
        return txdb.read(table, key, fields, result);
	}

	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        int res=txdb.scan(table, startkey, recordcount, fields, result);
        return res;
	}

	@Override
	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {
        int res=txdb.update(table, key, values);
        return res;
	}

	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {
        int res=txdb.insert(table, key, values);
        return res;
	}

	@Override
	public int delete(String table, String key) {
        int res=txdb.delete(table, key);
        return res;
	}

	@Override
	public int commit(UUID txid) {
		int res = txdb.commit(txid);
		return res;
	}

}
