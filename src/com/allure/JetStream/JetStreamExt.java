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
package com.allure.JetStream;

import java.util.logging.Logger;

/**
 *
 * @author ericjbruno
 */
public class JetStreamExt {

    private static Logger logger = Logger.getLogger("JetStreamExt");

    public JetStreamExt() {
        logger.setParent(JetStream.getInstance().getLogger());
    }

    public void shutdownServer() {
        logger.info("Shutdown requested");
        if (JetStream.getInstance().isServer()) {
            logger.info("Closing all worker threads");
            JetStream.getInstance().shutdown();
        }
    }
}
