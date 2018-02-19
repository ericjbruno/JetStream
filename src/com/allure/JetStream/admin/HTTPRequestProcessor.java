/*
 * This file is part of JetStreamQ.
 *    
 * Copyright 2010-2013 Allure Technology, Inc. All Rights Reserved.
 * www.alluretechnology.com
 *
 * JetStreamQ is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 2 of the License.
 *
 * JetStreamQ is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with JetStreamQ. If not, see <http://www.gnu.org/licenses/>.
 */
package com.allure.JetStream.admin;

import java.net.*;
import java.io.*;
import java.util.*;
import java.text.*;
import com.allure.JetStream.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.logging.Level;
import java.util.logging.Logger;
import util.*;

public class HTTPRequestProcessor implements Runnable {
    //private JetStreamLog logger = new JetStreamLog("HTTPRequestProcessor");

    private Logger logger = Logger.getLogger("HTTPRequestProcessor");
    private java.util.Date serverStartDate = new java.util.Date();
    private static List pool = new LinkedList();
    private int port = 8081;
    String host = null;
    private static final int MAIN = 1;
    private static final int SHUTDOWN = 2;
    private static final int GC = 3;

    ///////////////////////////////////////////////////////////////////////////
    // Methods
    public HTTPRequestProcessor(File documentRootDirectory,
            String indexFileName, int port) {
        if (documentRootDirectory.isFile()) {
            throw new IllegalArgumentException(
                    "documentRootDirectory must be a directory, not a file");
        }
    }

    public HTTPRequestProcessor(int port) {
        this.port = port;
    }

    public static void processRequest(Socket request) {
        synchronized (pool) {
            pool.add(pool.size(), request);
            pool.notifyAll();
        }
    }
    private static boolean listen = true;

    public static void shutdown() {
        listen = false;
    }

    public void run() {
        if (JetStream.fineLevelLogging) {
            logger.info("Admin server request processor running");
        }

        while (listen) {
            Socket connection;

            synchronized (pool) {
                while (pool.isEmpty()) {
                    try {
                        pool.wait();
                    }
                    catch (InterruptedException e) {
                        return;
                    }
                }

                if (listen == false) {
                    return;
                }

                connection = (Socket) pool.remove(0);
            }

            try {
                DataOutputStream out =
                        new DataOutputStream(connection.getOutputStream());
                BufferedInputStream bis =
                        new BufferedInputStream(connection.getInputStream());
                InputStreamReader in = new InputStreamReader(bis, "ASCII");

                // Get the request string from the socket
                //
                StringBuffer sb = new StringBuffer();
                String request = null;
                while (in.ready()) {
                    int c = in.read();
                    sb.append((char) c);
                    //System.out.print(c);
                }

                //System.out.println(sb.toString());
                //System.out.println("\n");

                // Parse out the request and host information
                if (sb != null) {
                    int n = sb.indexOf("\n");
                    int r = sb.indexOf("\r");
                    int start = 0;
                    int end;
                    if (r < n) {
                        end = r;
                    }
                    else {
                        end = n;
                    }
                    if (end > 0) {
                        request = sb.substring(start, end);
                    }

                    // Determine host server name from URL
                    if (host == null) {
                        start = sb.indexOf("Host:");
                        end = sb.indexOf("8081");
                        if (start > 0 && end > 0) {
                            host = sb.substring(start + 6, end + 4);
                        }
                    }
                }

                /*
                 System.out.println("-Request:" + request);
                 System.out.println("-Host:" + host);
                 */


                // Determine the request type by parsing the string
                //
                int requestType = determineRequest(request);

                // Handle the request and display the correct admine page
                //
                String str = "";
                switch (requestType) {
                    case MAIN:
                        str = getMainPage();
                        break;
                    case SHUTDOWN:
                        str = handleServerShutdown();
                        break;
                    case GC:
                        handleGC();
                        str = getMainPage();
                        break;

                    default:
                        str = "ERROR";
                }

                out.writeBytes(str); //write(str);
                out.flush();
                out.close();
            }
            catch (IOException e) {
                logger.log(Level.SEVERE, "Exception", e);
            }
            finally {
                // Want to ensure that no exceptions occur in here
                //
//                try { 
//                    connection.close(); 
//                } catch (IOException e) {} 
            }
        }

    }

    private int determineRequest(String request) {
        try {
            int start = request.indexOf('/');
            int end = request.indexOf("?");
            if (end == -1) {
                end = request.indexOf("HTTP/1.1");
            }

            String command = request.substring(start + 1, end);

            if (command.contains("shutdown ")) {
                return SHUTDOWN;
            }
            else if (command.contains("gc ")) {
                return GC;
            }
        }
        catch (Exception e) {
        }
        return MAIN;
    }

    private String getHeader() {
        String str = "HTTP/1.0 200 OK\r\n  ";
        str += "Connection: close\r\n";
        str += "Server: JetStreamAdmin\r\n";
        str += "Content-Type: text/html\r\n\r\n";
        str += "<HTML>";
        str += "<BODY>";
        str += "<a href=\"http://" + host + "\"><H2>JetStream Server - " + host + "</H2></a>";
        str += "Version " + JetStream.getInstance().getVersion() + "<br>";
        str += "Copyright (c) 2010-2013, <b>Allure Technology, Inc.</b><br>";
        return str;
    }

    private String getMainPage() {
        Runtime rt = Runtime.getRuntime();
        MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();

        // Get free and total memory
        //
        String totalMemory;
        String freeMemory;
        try {
            Locale loc = new Locale("en", "US");
            NumberFormat numf = NumberFormat.getInstance(loc);

            totalMemory = numf.format(rt.totalMemory());
            freeMemory = numf.format(rt.freeMemory());
        }
        catch (Exception e) {
            totalMemory = "" + rt.totalMemory();
            freeMemory = "" + rt.freeMemory();
        }

        String str = getHeader();
        str += "<P><B>Server Information:</B><br>";
        str += "System CPUs/cores: " + rt.availableProcessors() + "<br>";
        str += "OS: " + System.getProperty("os.name") + ", " + System.getProperty("os.arch") + ", " + System.getProperty("os.version") + "<br>";
        str += "Server started on: " + serverStartDate + "<br>";
        str += "Journal home folder: " + JetStream.getInstance().ROOT_FOLDER + "<br>";
        str += "User home folder: " + System.getProperty("user.home") + "<br>";
        str += "<P><B>Java Information:</B><br>";
        str += "Java runtime version: " + System.getProperty("java.version");
        str += "-" + System.getProperty("java.vm.version") + "<br>";
        str += "JVM total memory: " + totalMemory + " bytes<br>";
        str += "JVM free memory: " + freeMemory + " bytes<br>";
        str += "Heap mem: " + memBean.getHeapMemoryUsage() + "<br>";
        str += "Other mem: " + memBean.getNonHeapMemoryUsage() + "<br>";
        //str += "--------------------------------------------------------<br>";
        str += "<P><B>Local Server Information:</B><br>";

        StringBuffer sb = new StringBuffer();

        if (JetStream.isIsolatedMode()) {
            sb.append("<br> <B>Running in Isolated Mode!</B> <br>");
        }
        sb.append("Local client count: ");
        sb.append(JetStream.getInstance().getNetworkClientCount());
        sb.append("<br>");
        sb.append("Remote server count: ");
        sb.append(JetStream.getInstance().getRemoteServerCount());
        sb.append("<br>");
        sb.append("Outstanding send requests: ");
        sb.append(JetStream.getInstance().getOutstandingQueueSends());
        sb.append("<br>");
        sb.append("To be sent: ");
        sb.append(JetStream.getInstance().getQueueSchedulerRequestCount());

        sb.append("<br>");

        sb.append("<Table border=1 cellspacing=0>");
        sb.append("<Caption>Local Queues</Caption>");

        sb.append("<TH>Name</TH>");
        sb.append("<TH>Pending</TH>");
        sb.append("<TH>State</TH>");
        sb.append("<TH>Clients</TH>");
        sb.append("<TH>Consumers</TH>");
        sb.append("<TH>In Memory</TH>");
        sb.append("<TH>Journal:records</TH>");
        sb.append("<TH>Journal:slots</TH>");
        sb.append("<TH>Journal:size</TH>");
        sb.append("<TH>Delivered</TH>");

        try {
            // Output Queue diagnostics
            Collection qs = JetStream.getInstance().getQueues().values();
            Iterator iter = qs.iterator();
            while (iter.hasNext()) {
                JetStreamQueue jsQueue = (JetStreamQueue) iter.next();

                // Output message count and suspend state
                sb.append("<TR>");
                sb.append("<TD>");
                sb.append(jsQueue.getName());
                sb.append("</TD>");

                sb.append("<TD>");
                sb.append(jsQueue.getMessageCount());
                sb.append("</TD>");

                sb.append("<TD>");
                if (jsQueue.isSuspended()) {
                    sb.append("Active");
                }
                else {
                    sb.append("Waiting");
                }
                sb.append("</TD>");

                sb.append("<TD>" + jsQueue.getClientListSize() + "</TD>");
                sb.append("<TD>" + jsQueue.getConsumerSize() + "</TD>");
                sb.append("<TD>" + jsQueue.getMessagesInMemory() + "</TD>");

                sb.append("<TD>");
                sb.append(jsQueue.getJournalRecordCount());
                sb.append("</TD>");

                sb.append("<TD>");
                sb.append(jsQueue.getJournalEmptyCount());
                sb.append("</TD>");

                sb.append("<TD>");
                sb.append(jsQueue.getJournalFileSize());
                sb.append("</TD>");

                sb.append("<TD>");
                sb.append(jsQueue.getDeliveredCount());
                sb.append("</TD>");

                sb.append("</TR>");
            }

            sb.append("</Table>");


            // Output HA information
            for (JetStream.RemoteServer server : JetStream.getInstance().getServersByAddress().values()) {
                if (server.link == null) {
                    continue;
                }

                sb.append("<br><b>Remote Server Information:</b>");
                sb.append("<br>============================================ <br>");
                sb.append("Server '");
                sb.append(server.link.hostAddr);
                sb.append("', online=");
                sb.append(server.online);
                sb.append("<br>");

                for (Persistence journal : server.journals.values()) {
                    synchronized (journal) {
                        sb.append("--HA Journal '");
                        sb.append(journal.getName());
                        sb.append("', folder=");
                        sb.append(journal.getFolder());
                        sb.append(" (idx size=");
                        sb.append(journal.getRecordCount());
                        sb.append(", emptyIdx size=");
                        sb.append(journal.getEmptyCount());
                        sb.append(", filesize=");
                        sb.append(journal.getFilesize());
                        sb.append(")<br>");
                    }
                }
            }

            // Check for mem leaks
            /*
             // Check JetStreamQueueScheduler states
             Iterator<JetStream.JetStreamQueueScheduler> schedIter =
             JetStream.getInstance().getQueueSchedulers().iterator();
             while ( schedIter.hasNext() ) {
             JetStream.JetStreamQueueScheduler sched = schedIter.next();
             sb.append("JetStreamQueueScheduler thread state = ");
             sb.append(sched.getState());
             StackTraceElement[] stack = sched.getStackTrace();
             System.out.println("Stack trace for " + sched.toString() );
             for ( StackTraceElement elem : stack ) {
             System.out.println("\t"+elem.getClassName()+"."+elem.getMethodName()+"(Line:"+elem.getLineNumber()+")");
             }
             sb.append("<br>");
             }
             */
        }
        catch (Exception e) {
        }

        str += sb.toString();

        // Add links for stopping and starting components as well
        // as the entire JTS server itself
        //
        str += "<P><B>Server Operation:</B><br>";
        str += "<a href=\"http://" + host + "/gc\" target=\"_parent\">";
        str += "Force GC";
        str += "</a>";
        str += "<P><a href=\"http://" + host + "/shutdown\" target=\"_parent\">";
        str += "Shutdown this server<br>";
        str += "</a>";
        str += "</BODY>";
        str += "</HTML>";

        return str;
    }

    private String getConfigPage() {
        String str = getHeader();
        str += "<P><B>Server Configuration:</B><br>";
        str += "<Table border>";
        str += "<tr>";
        str += "<th>Property</th>";
        str += "<th>Value</th>";
        str += "</tr>";
        str += "<tr>";
        str += "<td>Server Name</td>";
        str += "<td>" + JetStream.getInstance().SERVER_HOSTNAME + "</td>";
        str += "</tr>";
        str += "<tr>";
        str += "<td>Server Address</td>";
        str += "<td>" + JetStream.getInstance().addr + "</td>";
        str += "</tr>";
        str += "<tr>";
        str += "<td>Admin port</td>";
        str += "<td>" + "?" + "</td>";
        str += "</tr>";
        str += "<tr>";
        str += "<td>Log file</td>";
        str += "<td>" + "/java/beacon/logs/servera.log" + "</td>";
        str += "</tr>";
        str += "<tr>";
        str += "<td>Min Java heap size </td>";
        str += "<td>" + "32M" + "</td>";
        str += "</tr>";
        str += "<tr>";
        str += "<td>Max Java heap size </td>";
        str += "<td>" + "64M" + "</td>";
        str += "</tr>";
        str += "</Table>";

        str += "<P><a href=\"http://localhost:" + port + "\" target=\"_parent\">";
        str += "Back to main admin page<br>";
        str += "</a>";
        str += "<P><a href=\"http://localhost:" + port + "/shutdown\" target=\"_parent\">";
        str += "Shutdown this server<br>";
        str += "</a>";
        str += "</BODY>";
        str += "</HTML>";
        return str;
    }

    private String getServersPage() {
        String str = getHeader();
        str += "<P><B>Clustered Servers (click on a server name for more detail)::</B><br>";
        str += "<Table border>";
        str += "<tr>";
        str += "<th>Server Name</th>";
        str += "<th>Address</th>";
        str += "<th>Registered?</th>";
        str += "<th>Status</th>";
        str += "</tr>";

        /*
         // Iterate through all clustered server partners
         //
         HashMap partners = partnerMgr.getAllPartners();
         Collection values = partners.values();
         Iterator iter = values.iterator();
         while ( iter.hasNext() )
         {
         PartnerManager.Partner partner = 
         (PartnerManager.Partner)iter.next();
            
         if ( BeaconProperties.getLocalServerName().equals(partner.name) )
         continue;

         str += "<tr>";
         str += "<td><a href=\"http://localhost:"
         + port+"/partnerinfo?" + partner.name 
         + "\" target=\"_parent\">"+partner.name+"</a></td>";
         str += "<td>"+partner.addr+"</td>";

         if ( partner.registered )
         {
         str += "<td>TRUE</td>";
         if ( partner.failed )
         str += "<td>FAILED</td>";
         else
         str += "<td>ALIVE</td>";
         }
         else
         {
         str += "<td>FALSE</td>";
         if ( partner.failed )
         str += "<td>FAILED</td>";
         else
         str += "<td>N/A</td>";
         }
            
         str += "</tr>";
         }
         */

        str += "</Table>";
        str += "<P><a href=\"http://localhost:" + port + "\" target=\"_parent\">";
        str += "Back to main admin page<br>";
        str += "</a>";
        str += "<P><a href=\"http://localhost:" + port + "/shutdown\" target=\"_parent\">";
        str += "Shutdown this server<br>";
        str += "</a>";
        str += "</BODY>";
        str += "</HTML>";

        return str;
    }

    private String getComponentsPage() {
        String str = getHeader();
        str += "<P><B>Hosted Components:</B><br>";
        str += "<a href=\"http://localhost:" + port + "/stopcomponent?all\" target=\"_parent\">";
        str += "Stop all hosted components<br>";
        str += "</a>";
        str += "<a href=\"http://localhost:" + port + "/startcomponent?all\" target=\"_parent\">";
        str += "Start all hosted components<br>";
        str += "</a>";
        str += "<Table border>";
        str += "<tr>";
        str += "<th>Type</th>";
        str += "<th>Class name</th>";
        str += "<th>Topic name</th>";
        str += "<th>Health</th>";
        str += "<th>Intended host</th>";
        str += "<th>Action</th>";
        str += "</tr>";

        /*
         // Add local and failed components to the table
         //
         Vector<ComponentManager.Component> components = componentMgr.getLocalComponents();
         Vector<ComponentManager.Component> failedComps = componentMgr.getFailedComponents();
         Object[] objs = failedComps.toArray();
         for ( int i = 0; i < objs.length; i++ )
         components.add((ComponentManager.Component)objs[i]);

         int cnt = components.size();
         for ( int p = 0; p < cnt; p++ )
         {
         ComponentManager.Component compInfo = 
         (ComponentManager.Component)components.elementAt( p );

         String state = "N/A";
         ContentComponent comp = null;
         if ( compInfo.objRef instanceof ContentComponent )
         {
         comp = (ContentComponent)compInfo.objRef;
         state = comp.getState().name();
         if ( comp.getState() == ContentComponent.State.ERROR )
         state = "ERROR";
         else if ( comp.getState() == ContentComponent.State.GOOD )
         state = "RUNNING";
         else if ( comp.getState() == ContentComponent.State.STOPPED )
         state = "STOPPED";
         else
         state = comp.getState().name();
         }
            
         str += "<tr>";
         if ( comp != null )
         str += "<td>JMS</td>";
         else
         str += "<td>RMI</td>";
         str += "<td> "+compInfo.name+" </td>";
         if ( compInfo.destination != null )
         str += "<td> "+compInfo.destination+" </td>";
         else
         str += "<td>N/A</td>";

         if ( compInfo.objRef != null )
         str += "<td> "+ state +" </td>";
         else
         str += "<td> Stopped </td>";
            
         str += "<td> "+compInfo.partnerName+" </td>";

         if ( compInfo.objRef != null 
         &&  ! state.equalsIgnoreCase("STOPPED") )
         {
         str += "<td><a href=\"http://localhost:"
         + port+"/stopcomponent?" + compInfo.name 
         + "&" + compInfo.destination
         + "\" target=\"_parent\"> Stop </a></td>";
         }
         else
         {
         str += "<td><a href=\"http://localhost:"
         + port+"/startcomponent?" + compInfo.name 
         + "&" + compInfo.destination
         + "\" target=\"_parent\"> Start </a></td>";
         }

         str += "</tr>";
         }
         */

        str += "</Table>";
        str += "<P><a href=\"http://localhost:" + port + "\" target=\"_parent\">";
        str += "Back to main admin page<br>";
        str += "</a>";
        str += "<P><a href=\"http://localhost:" + port + "/shutdown\" target=\"_parent\">";
        str += "Shutdown this server<br>";
        str += "</a>";
        str += "</BODY>";
        str += "</HTML>";
        return str;
    }

    private String getPartnerInfoPage(String request) {
        // Parse partner name from the request string
        //
        int start = request.indexOf('?');
        int end = request.indexOf("HTTP/1.1");

        String partnerName = request.substring(start + 1, end - 1);

        /*
         PartnerManager.Partner partner = 
         partnerMgr.getPartnerData( partnerName );
        
         String str = getHeader();
         str += "<P><B>Partner Information:</B><br>";
         str += "Partner Name: " + partner.name + "<br>";
         str += "Partner Address: " + partner.addr + "<br>";

         if ( partner.registered )
         {
         str += "Partner registered: YES<br>";
         if ( partner.failed )
         str += "Partner status: FAILED<br>";
         else
         str += "Partner status: GOOD<br>";
         }
         else
         {
         str += "Partner registered: NO<br>";
         if ( partner.failed )
         str += "Partner status: FAILED<br>";
         else
         str += "Partner status: N/A<br>";
         }

        
         str += "<P><B>Partner hosted components:</B><br>";
         str += "<Table border>";
         str += "<tr>";
         str += "<th>Class name</th>";
         str += "<th>Topic name</th>";
         str += "</tr>";
        
         Vector destinationList = componentMgr.getPartnerComponents( partnerName );
         int count = destinationList.size();
         for ( int x = 0; x < count; x++ )
         {
         ConfigFileParser.ComponentData data =
         (ConfigFileParser.ComponentData)destinationList.elementAt( x );

         str += "<tr>";
         str += "<td> "+data.className+" </td>";
         str += "<td> "+data.destinationName+" </td>";
         str += "</tr>";
         }

         str += "</Table>";
         str += "<P><a href=\"http://localhost:" + port + "\" target=\"_parent\">";
         str += "Back to main admin page<br>";
         str += "</a>";
         str += "<P><a href=\"http://localhost:" + port + "/shutdown\" target=\"_parent\">";
         str += "Shutdown this server<br>";
         str += "</a>";
         str += "</BODY>";
         str += "</HTML>";
         return str;
         }

         private String handleStartStopComponent(int action, String request)
         {
         String componentName = "";
         String componentTopic = "";

         // Parse component name and destination from the request string
         //
         int start = request.indexOf('?');
         int end = request.indexOf('&', start + 1);
         if ( end == -1 )
         end = request.indexOf("HTTP/1.1") - 1;

         componentName = request.substring(start + 1, end);
        
         // Ignore any destination parameter if the caller wants start or stop
         // all components
         //
         if (!componentName.equals("all") )
         {
         start = request.indexOf('&');
         end = request.indexOf("HTTP/1.1") - 1;
         componentTopic = request.substring(start + 1, end);
         }
        
         Vector components = componentMgr.getLocalComponents();
         Vector failedComps = componentMgr.getFailedComponents();
         Object[] objs = failedComps.toArray();
         for ( int i = 0; i < objs.length; i++ )
         components.add(objs[i]);

         int cnt = components.size();
         for ( int p = 0; p < cnt; p++ )
         {
         ComponentManager.Component compInfo = 
         (ComponentManager.Component)components.elementAt( p );
        
         String destination = compInfo.destination;
         String name = compInfo.name;
            
         // Check to see if this is the correct component name and destination
         // to stop. If the request was to stop ALL components then
         // stop it regardless of name and destination
         //
         ContentComponent comp = (ContentComponent)compInfo.objRef;
         if ( componentName.equals( "all" ) 
         || (name.equals( componentName ) && destination.equals( componentTopic )))
         {
         try {
         switch ( action )
         {
         case STOPCOMPONENT:
         // Make sure the component is running first
         //
         if ( comp.getState() != ContentComponent.State.STOPPED )
         comp.Stop();
         break;

         case STARTCOMPONENT:
         // Make sure the component is really stopped first
         //
         if ( comp.getState() == ContentComponent.State.STOPPED )
         comp.Start();
         break;
         }
         }
         catch ( Exception e ) {
         logger.log("Exception starting component " + componentName
         + ": " + e.getMessage() );
         }
                
         if ( !componentName.equals( "all" ) )
         {
         // Unless we're stopping all components, we've found
         // the one component we wish to stop, so break the loop
         //
         break;
         }
         }
         }
        
         try { Thread.sleep(1000); } catch ( Exception e ) { }
         */

        Date now = new Date();
        String str = "HTTP/1.1 303 See Other\r\n";
        str += "Date: " + now + "\r\n";
        str += "Server: Beacon Server 1.0\r\n";
        str += "Content-type: text/html\r\n";
        str += "Content-Location: http://localhost:" + port + "/comps\r\n";
        str += "Location: http://localhost:" + port + "/comps\r\n\r\n";

        return str;
    }

    private String handleServerShutdown() {
        String str = getHeader();
        str += "<P><B>Server shutting down...</B><br>";
        System.exit(128);
        return str;
    }

    private void handleGC() {
        System.gc();
        System.runFinalization();
    }

    public static String guessContentTypeFromName(String name) {
        if (name.endsWith(".html") || name.endsWith(".htm")) {
            return "text/html";
        }
        else if (name.endsWith(".txt") || name.endsWith(".java")) {
            return "text/plain";
        }
        else if (name.endsWith(".gif")) {
            return "image/gif";
        }
        else if (name.endsWith(".class")) {
            return "application/octet-stream";
        }
        else if (name.endsWith(".jpg") || name.endsWith(".jpeg")) {
            return "image/jpeg";
        }
        else {
            return "text/plain";
        }
    }
}
