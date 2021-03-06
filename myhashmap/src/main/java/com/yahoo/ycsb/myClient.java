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

package com.yahoo.ycsb;


import com.yahoo.ycsb.measurements.Measurements;
import fct.thesis.database.TransactionFactory;
import fct.thesis.database.TransactionTypeFactory;
import org.apache.commons.lang3.text.StrSubstitutor;
import thrift.DatabaseSingleton;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;



/**
 * Main class for executing YCSB.
 */
public class myClient
{
    public static final String INTERACTIVE ="interactive";
    public static final String INTERACTIVE_DEFAULT ="false";

    private static boolean interactive;

    public static void main(String[] args) {

        Properties props=new Properties();

        readArguments(args, props);

        interactive =  Boolean.parseBoolean(props.getProperty(INTERACTIVE, INTERACTIVE_DEFAULT));

        if (props.getProperty("threadcount")==null) {
            String newargs[] = new String[args.length + 2];
            System.arraycopy(args, 0, newargs, 0, args.length);
            newargs[newargs.length - 2] = "-p";
            newargs[newargs.length - 1] = "threadcount=1";
            args = newargs;
        }

        String path = props.getProperty("exportfile");
        String finalName;
        int index_ext = 0;
        if (path!=null){
            File f = new File(path);
            // Assumindo que tem extensao
            index_ext = f.getName().lastIndexOf(".");
            path = f.getAbsolutePath();
            finalName = path.substring(0,path.lastIndexOf("/"))+"/"+f.getName().substring(0,index_ext)+"_${exec}."+f.getName().substring(index_ext+1);
            System.out.println(finalName);
            props.setProperty("exportfile",finalName);
        }

        System.out.println("Database Transactions Type : "+ props.getProperty("transaction.type","TWOPL"));
        TransactionFactory.type type = TransactionTypeFactory.getType(props.getProperty("transaction.type", "TWOPL"));
        DatabaseSingleton.setTransactionype(type);

        System.out.println("Loading...\n");

        if (path!=null){
            HashMap<String,String> h = new HashMap<String, String>();
            h.put("exec","load");
            StrSubstitutor sub = new StrSubstitutor(h);
            String fpath = sub.replace(props.getProperty("exportfile"));

            for (int i = 0; i < args.length; i++) {
                if (args[i].startsWith("exportfile"))
                    args[i] = "exportfile="+fpath;
            }

        }

        Client.exit = false;

        // Loading
        Client.main(args);

//        System.out.println("Press ENTER to continue!!");
//        Scanner in = new Scanner(System.in);
//        in.nextLine();

        System.out.println("Running...\n");

        HashMap<String, String> h = new HashMap<String, String>();
        h.put("exec", "run");
        StrSubstitutor sub = new StrSubstitutor(h);
        String fpath = sub.replace(props.getProperty("exportfile"));

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-load"))
                args[i] = "-t";
            else if (args[i].startsWith("exportfile"))
                args[i] = "exportfile="+fpath;
        }

        Measurements.getMeasurements().cleanMeasurements();

        Workload.addCleanupHook(new WorkloadCleanup() {
            @Override
            public void run() {
                DatabaseSingleton.getDatabase().cleanup();
            }
        });

        // Running
        Client.main(args);


        if (interactive){
            System.out.println();

            ArrayList<String> oldArgs = new ArrayList<String>(Arrays.asList(args));
            ArrayList<String> tempArgs = new ArrayList<String>();

            for (int i = 0; i < oldArgs.size(); i++) {
                if (oldArgs.get(i).compareTo("-P") == 0){
                    tempArgs.add(oldArgs.get(i));
                    tempArgs.add(oldArgs.get(i + 1));
                } else if (oldArgs.get(i).compareTo("-db") == 0){
                    tempArgs.add(oldArgs.get(i));
                    tempArgs.add(oldArgs.get(i + 1));
                }  else if (oldArgs.get(i).compareTo("-p") == 0){
                    tempArgs.add(oldArgs.get(i));
                    tempArgs.add(oldArgs.get(i + 1));
                } else if (oldArgs.get(i).compareTo("-table") == 0){
                    tempArgs.add(oldArgs.get(i));
                    tempArgs.add(oldArgs.get(i + 1));
                }
            }

            String[] newArgs = new String[tempArgs.size()];
            tempArgs.toArray(newArgs);
            CommandLine.main(newArgs);
        }

    }

    private static void readArguments(String args[], Properties props) {
        //parse arguments
        int argindex=0;

        while (argindex < args.length) {
            if (args[argindex].compareTo("-P") == 0) {
                argindex++;
                if (argindex >= args.length) {
                    System.exit(0);
                }
                String propfile = args[argindex];
                argindex++;

                Properties myfileprops = new Properties();
                try {
                    myfileprops.load(new FileInputStream(propfile));
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                    System.exit(0);
                }

                //Issue #5 - remove call to stringPropertyNames to make compilable under Java 1.5
                for (Enumeration e = myfileprops.propertyNames(); e.hasMoreElements(); ) {
                    String prop = (String) e.nextElement();

                    props.setProperty(prop, myfileprops.getProperty(prop));
                }

            } else if (args[argindex].compareTo("-p") == 0) {
                argindex++;
                if (argindex >= args.length) {
                    System.exit(0);
                }
                int eq = args[argindex].indexOf('=');
                if (eq < 0) {
                    System.exit(0);
                }

                String name = args[argindex].substring(0, eq);
                String value = args[argindex].substring(eq + 1);
                props.put(name, value);
//                System.out.println("["+name+"]=["+value+"]");
                argindex++;
            } else {
                argindex++;
            }
        }
    }
}
