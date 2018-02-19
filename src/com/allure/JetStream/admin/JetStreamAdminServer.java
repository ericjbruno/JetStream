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

import java.io.*;
import java.net.*;
import java.util.logging.Logger;

public class JetStreamAdminServer extends Thread {

    private File documentRootDirectory;
    private String indexFileName = "index.html";
    private ServerSocket server;
    private int numThreads = 50;
    private int port = 0;
    private Logger logger = Logger.getLogger("JetStreamAdminServer");

    public JetStreamAdminServer(File documentRootDirectory, int port,
            String indexFileName) throws IOException {
        this.setName("JetStreamAdminServer");
        this.documentRootDirectory = documentRootDirectory;
        this.indexFileName = indexFileName;
        this.port = port;
        this.server = new ServerSocket(port);

        if (com.allure.JetStream.JetStream.fineLevelLogging) {
            logger.info("Started Admin server on port " + port);
        }
    }

    public JetStreamAdminServer(File documentRootDirectory, int port) throws IOException {
        this(documentRootDirectory, port, "index.html");
    }

    public JetStreamAdminServer(File documentRootDirectory) throws IOException {
        this(documentRootDirectory, 80, "index.html");
    }

    public JetStreamAdminServer(int port) throws IOException {
        this(null, port, "index.html");
    }
    private boolean listen = true;
    Thread reqProc = null;

    public void shutdown() {
        listen = false;
        HTTPRequestProcessor.shutdown();
        reqProc.interrupt();
    }

    public void run() {
        for (int i = 0; i < numThreads; i++) {
            if (documentRootDirectory != null) {
                reqProc = new Thread(
                        new HTTPRequestProcessor(
                        documentRootDirectory,
                        indexFileName,
                        port));
            }
            else {
                reqProc = new Thread(
                        new HTTPRequestProcessor(
                        port));
            }

            reqProc.setName("HTTPRequestProcessor");
            reqProc.start();

            while (listen) {
                try {
                    server.setSoTimeout(1000);
                    Socket request = null;
                    try {
                        request = server.accept();
                    }
                    catch (SocketTimeoutException ste) {
                        if (listen) {
                            continue;
                        }
                        return;
                    }

                    if (request != null) {
                        HTTPRequestProcessor.processRequest(request);
                    }
                }
                catch (IOException e) {
                }
            }
        }
    }
}
