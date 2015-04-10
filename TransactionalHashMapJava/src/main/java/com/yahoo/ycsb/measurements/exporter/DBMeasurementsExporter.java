/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */
package com.yahoo.ycsb.measurements.exporter;

import com.yahoo.ycsb.db.Config;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.*;
import java.util.HashMap;
import java.util.Properties;

/**
 * Export measurements into a machine readable JSON file.
 */
public class DBMeasurementsExporter implements MeasurementsExporter
{

    HashMap<String, String> data;

    public DBMeasurementsExporter(OutputStream os, Properties props) throws IOException
    {
        data = new HashMap<String, String>();

        data.put("workloads", "\""+props.getProperty("nameworkload","test")+"\"");
        data.put("recordcount", props.getProperty("recordcount","-1"));
        data.put("operationcount", props.getProperty("operationcount","-1"));
        data.put("threads", props.getProperty("threadcount","-1"));
        data.put("distribution", "\""+props.getProperty("requestdistribution")+"\"");
        data.put("transactiontype", "\""+props.getProperty("transaction.type","TWOPL")+"\"");

        if(props.getProperty("op","load").equals("run"))
            data.put("run", "1");
        else
            data.put("run", "0");
    }

    public void write(String metric, String measurement, int i) throws IOException
    {
        writeMeasurement(metric, measurement, i);
    }

    public void write(String metric, String measurement, double d) throws IOException
    {
        writeMeasurement(metric, measurement, d);
    }

    private void writeMeasurement(String metric, String measurement, double d) {
        if (metric.equals("OVERALL")){
            if (measurement.equals("RunTime(ms)")){
                data.put("runtime", d+"");
            } else if (measurement.equals("Throughput(ops/sec)")){
                data.put("throughput", d+"");
            }
        } else if (metric.equals("INSERT")){
            if (measurement.equals("Operations")){
                data.put("n_inserts", d+"");
            } else if (measurement.equals("AverageLatency(us)")){
                data.put("avg_inserts", d+"");
            } else if (measurement.equals("MinLatency(us)")){
                data.put("min_inserts", d+"");
            } else if (measurement.equals("MaxLatency(us)")){
                data.put("max_inserts", d+"");
            }
        } else if (metric.equals("READ")){
            if (measurement.equals("Operations")){
                data.put("n_reads", d+"");
            } else if (measurement.equals("AverageLatency(us)")){
                data.put("avg_reads", d+"");
            } else if (measurement.equals("MinLatency(us)")){
                data.put("min_reads", d+"");
            } else if (measurement.equals("MaxLatency(us)")){
                data.put("max_reads", d+"");
            }
        } else if (metric.equals("UPDATE")){
            if (measurement.equals("Operations")){
                data.put("n_updates", d+"");
            } else if (measurement.equals("AverageLatency(us)")){
                data.put("avg_updates", d+"");
            } else if (measurement.equals("MinLatency(us)")){
                data.put("min_updates", d+"");
            } else if (measurement.equals("MaxLatency(us)")){
                data.put("max_updates", d+"");
            }
        }
    }

    @Override
    public void flush() throws IOException {
//        String query = "insert into benchmark(workloads,recordcount,operationcount,threads,run,distribution,n_inserts,avg_inserts," +
//                "min_inserts, max_inserts, n_reads, avg_reads, min_reads, max_reads," +
//                "n_updates, avg_updates, min_update, max_updates) values ()";
        String query = "insert into benchmark(";
        String values = " values (";
        int t = data.size()-1;
        for (String key : data.keySet()){
            query += key + (t>0?",":")");
            values += data.get(key) + (t>0?",":")");
            t--;
        }
        query += values;

        System.out.println(query);

        try {

            Class.forName(Config.dbClass);
            Connection connection = DriverManager.getConnection(Config.dbUrl,
                    Config.username, Config.password);
            Statement statement = connection.createStatement();
            statement.execute(query);
            connection.close();
            System.out.println("Export Ok");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void close() throws IOException
    {
    }

}
