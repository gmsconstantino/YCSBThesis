package com.yahoo.ycsb;

import java.security.Permission;

/**
 * Created by gomes on 25/03/15.
 */
public class myClientTest {

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
        System.out.println("Loading...");

        forbidSystemExitCall() ;
        try {
            Client.main(args);
        } catch( ExitTrappedException e ) {
        } finally {
//            enableSystemExitCall() ;
        }

        System.out.println("Running...");


        for (int i=0; i < args.length ; i++){
            if (args[i].equals("-load"))
                args[i]="-t";
        }

        try {
            Client.main(args);
        } catch( ExitTrappedException e ) {
        } finally {
            enableSystemExitCall() ;
        }

    }

}
