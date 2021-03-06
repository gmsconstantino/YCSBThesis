/**
 * Copyright (c) 2013 NOVA-LINCS
 * @Author Joao Leitao & Henrique Moniz
 */

package com.yahoo.ycsb.workloads;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

import com.yahoo.ycsb.TxDB;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.RandomByteIterator;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.ExponentialGenerator;
import com.yahoo.ycsb.generator.HotspotIntegerGenerator;
import com.yahoo.ycsb.generator.IntegerGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.SkewedLatestGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.measurements.Measurements;

public class TxWorkload extends CoreWorkload {

	int txminsize;
	int txmaxsize;
	double txread;
	double txnonblindwrite;

    Properties props;
	IntegerGenerator transactionSize;
	IntegerGenerator keychooser;
	int recordcount;
	CounterGenerator transactioninsertkeysequence;
	Random rand;
	
	public static final String TX_SIZE_MIN_PROPERTY = "txminsize";
	public static final long   TX_SIZE_MIN_PROPERTY_DEFAULT = 2;
	
	public static final String TX_SIZE_MAX_PROPERTY = "txmaxsize";
	public static final long   TX_SIZE_MAX_PROPERTY_DEFAULT = 4;
	
	public static final String READ_FRACTION_PROPERTY = "txread";
	public static final double READ_FRACTION_PROPERTY_DEFAULT = 0.5;
	
	public static final String NON_BLIND_WRITE_FRACTION_PROTERTY = "txnoblind";
	public static final double NON_BLIND_WRITE_FRACTION_PROPERTY_DEFAULT = 0.5;	

	public static final String TX_SIZE_DISTRIBUTION_PROPERTY = "txsizedist";
	public static final String TX_SIZE_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

	public static final String RAND_SEED_PROPERTY = "rseed";
	public static int rseed = 12345;
	/**
	 * The name of the database field to run queries against. //We assume a single field (i.e. column) is accessed in all operations
	 */
	public static final String FIELDNAME_PROPERTY="field";

	/**
	 * The default name of the database table to run queries against.
	 */
	public static final String FIELDNAME_PROPERTY_DEFAULT="column";

	public static String field;
	
	/**
	 * Control for the field size used in the workloads.
	 */
	public static final String FIELDSIZE_PROPERTY="fieldsize";
	public static final int FIELDSIZE_PROPERTY_DEFAULT = 1024; //bytes
	public static int fieldsize;
	
	CounterGenerator keysequence;
	
	@Override
	public void init(Properties p) throws WorkloadException
	{
		super.init(p);

        props = p;
		
		txminsize  = Integer.parseInt(    p.getProperty(TX_SIZE_MIN_PROPERTY, TX_SIZE_MIN_PROPERTY_DEFAULT+""));
		txmaxsize  = Integer.parseInt(    p.getProperty(TX_SIZE_MAX_PROPERTY, TX_SIZE_MAX_PROPERTY_DEFAULT+""));
		txread     = Double.parseDouble(p.getProperty(READ_FRACTION_PROPERTY,READ_FRACTION_PROPERTY_DEFAULT+""));
		txnonblindwrite = Double.parseDouble(p.getProperty(NON_BLIND_WRITE_FRACTION_PROTERTY, NON_BLIND_WRITE_FRACTION_PROPERTY_DEFAULT+""));
		field      = p.getProperty(FIELDNAME_PROPERTY, FIELDNAME_PROPERTY_DEFAULT);
		fieldsize  = Integer.parseInt(	  p.getProperty(FIELDSIZE_PROPERTY, FIELDSIZE_PROPERTY_DEFAULT+""));
		
		String txsizedist = p.getProperty(TX_SIZE_DISTRIBUTION_PROPERTY, TX_SIZE_DISTRIBUTION_PROPERTY_DEFAULT);
		if(txsizedist.compareTo("uniform") == 0) {
			transactionSize = new UniformIntegerGenerator(txminsize, txmaxsize);
		} else if(txsizedist.compareTo("zipfian") == 0) {
			transactionSize = new ZipfianGenerator(txminsize, txmaxsize);
		} else {
			throw new WorkloadException("Distribution \"" + txsizedist + "\" not allowed for Tx size distribution");
		}
		
		recordcount = Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY));
		transactioninsertkeysequence=new CounterGenerator(recordcount);
		double insertproportion=Double.parseDouble(p.getProperty(INSERT_PROPORTION_PROPERTY,INSERT_PROPORTION_PROPERTY_DEFAULT));	
		String requestdistrib = p.getProperty(REQUEST_DISTRIBUTION_PROPERTY,REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
		
		if (requestdistrib.compareTo("uniform")==0)
		{
			keychooser=new UniformIntegerGenerator(0,recordcount-1);
		}
		else if (requestdistrib.compareTo("zipfian")==0)
		{
			//it does this by generating a random "next key" in part by taking the modulus over the number of keys
			//if the number of keys changes, this would shift the modulus, and we don't want that to change which keys are popular
			//so we'll actually construct the scrambled zipfian generator with a keyspace that is larger than exists at the beginning
			//of the test. that is, we'll predict the number of inserts, and tell the scrambled zipfian generator the number of existing keys
			//plus the number of predicted keys as the total keyspace. then, if the generator picks a key that hasn't been inserted yet, will
			//just ignore it and pick another key. this way, the size of the keyspace doesn't change from the perspective of the scrambled zipfian generator
			
			int opcount=Integer.parseInt(p.getProperty(Client.OPERATION_COUNT_PROPERTY));
			int expectednewkeys=(int)(((double)opcount)*insertproportion*2.0); //2 is fudge factor
			
			keychooser=new ScrambledZipfianGenerator(recordcount+expectednewkeys);
		}
		else if (requestdistrib.compareTo("latest")==0)
		{
			keychooser=new SkewedLatestGenerator(transactioninsertkeysequence);
		}
		else if (requestdistrib.equals("hotspot")) 
		{
      double hotsetfraction = Double.parseDouble(p.getProperty(
          HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
      double hotopnfraction = Double.parseDouble(p.getProperty(
          HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
      keychooser = new HotspotIntegerGenerator(0, recordcount - 1, 
          hotsetfraction, hotopnfraction);
    }
		else
		{
			throw new WorkloadException("Unknown request distribution \""+requestdistrib+"\"");
		}
		
		rseed = Integer.parseInt( p.getProperty(RAND_SEED_PROPERTY, rseed+""));
		rand = new Random(rseed);
		keysequence=new CounterGenerator(Integer.parseInt(p.getProperty(INSERT_START_PROPERTY,INSERT_START_PROPERTY_DEFAULT)));
	}
	
    int txNextKeynum() {
        int keynum = 0;
        if(keychooser instanceof ExponentialGenerator) {
            do
                {
                    keynum=transactioninsertkeysequence.lastInt() - keychooser.nextInt();
                }
            while(keynum < 0);
        } else {
            do
                {
                    keynum=keychooser.nextInt();
                }
            while (keynum > transactioninsertkeysequence.lastInt());
        }
        return keynum;
    }
    
    HashMap<String, ByteIterator> txBuildValues() {
 		HashMap<String,ByteIterator> values=new HashMap<String,ByteIterator>(1);

 		ByteIterator data= new RandomByteIterator(fieldsize);
 		values.put(field,data);
 		
		return values;
	}
    
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

        long st = System.nanoTime();
        UUID txid = txdb.beginTx();
		txdb.insert(table, buildKeyName(keynum), txBuildValues());

        if(txdb.commit(txid) == 0) {
            long en = System.nanoTime();
            Measurements.getMeasurements().measure("Tx", (int)((en-st)/1000));
            Measurements.getMeasurements().reportReturnCode("Tx", 0);
        } else {
            Measurements.getMeasurements().reportReturnCode("Tx", -1);
            return false;
        }
    	
    	return true;
    }
    
    public String buildKeyName(long keynum) {
        return keynum+"";
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
		if(txnonblindwrite > 0) {
			int numberOfNonBlind = (int) Math.round(numberOfWrites * txnonblindwrite);
			@SuppressWarnings("unchecked")
			Vector<Integer>keysReadClone = (Vector<Integer>) keysRead.clone();
			for(; i < numberOfNonBlind && keysRead.size() > 0 && i < numberOfWrites; i++)
				keysWrite.add(keysReadClone.remove(rand.nextInt(keysReadClone.size())));
		}
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
