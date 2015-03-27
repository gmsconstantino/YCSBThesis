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


import com.yahoo.ycsb.db.dbSingleton;
import com.yahoo.ycsb.measurements.Measurements;
import database.TransactionFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.Permission;
import java.util.*;

//import org.apache.log4j.BasicConfigurator;


/**
 * Main class for executing YCSB.
 */
public class myClient
{
    public static final String INTERACTIVE ="interactive";
    public static final String INTERACTIVE_DEFAULT ="false";

    private static class ExitTrappedException extends SecurityException { }

    private static void forbidSystemExitCall() {
        final SecurityManager securityManager = new SecurityManager() {
            public void checkPermission( Permission permission ) {
                if( "exitVM".equals( permission.getName() ) ) {
                    throw new ExitTrappedException() ;
                }
            }
            @Override
            public void checkExit(int status)
            {
                super.checkExit(status);
                throw new ExitTrappedException();
            }
        } ;
        System.setSecurityManager( securityManager ) ;
    }

    private static void enableSystemExitCall() {
        System.setSecurityManager( null ) ;
    }

    public static void main(String[] args) {

        Properties props=new Properties();

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

        System.out.println("Database Transactions Type : "+ props.getProperty("transaction.type","TWOPL"));
        TransactionFactory.type type = TransactionTypeFactory.getType(props.getProperty("transaction.type","TWOPL"));
        dbSingleton.setTransactionype(type);

        boolean interactive =  Boolean.parseBoolean(props.getProperty(INTERACTIVE, INTERACTIVE_DEFAULT));

        System.out.println("Loading...\n");

        forbidSystemExitCall() ;
        try {
            Client.main(args);
        } catch( ExitTrappedException e ) {
        } finally {
//            enableSystemExitCall() ;
        }

        System.out.println("\nRunning...\n");


        for (int i=0; i < args.length ; i++){
            if (args[i].equals("-load"))
                args[i]="-t";
        }

        Measurements.getMeasurements().cleanMeasurements();

        try {
            Client.main(args);
        } catch( ExitTrappedException e ) {
        } finally {
            enableSystemExitCall() ;
        }

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
}
