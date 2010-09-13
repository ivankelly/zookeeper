/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zookeeper.test.fd;

import junit.framework.Assert;

import org.apache.zookeeper.common.fd.FailureDetector;
import org.apache.zookeeper.common.fd.MessageType;
import org.apache.zookeeper.common.fd.SlicedPingFailureDetector;
import org.junit.Before;
import org.junit.Test;

public class SlicedHeartbeatFDTest {

    private FailureDetector fD;

    @Before
    public void init() {
        this.fD = new SlicedPingFailureDetector(100);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPingSentNoMonitoredRegistered() {
        fD.messageSent("id", 10, MessageType.PING);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPingReceivedNoMonitoredRegistered() {
        fD.messageReceived("id", 10, MessageType.PING);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIdleTimeNoMonitoredRegistered() {
        fD.getIdleTime("id", 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTimeToNextPingNoMonitoredRegistered() {
        fD.getTimeToNextPing("id", 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTimeoutNoMonitoredRegistered() {
        fD.getTimeout("id");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegisterMonitoredTwice() {
        fD.registerMonitored("id", 100, 100);
        fD.registerMonitored("id", 100, 100);
    }

    @Test
    public void testNoPingSentOrReceived() {
        Assert.assertFalse(fD.isFailed("id", 0));
        fD.registerMonitored("id", 0, 100);

        Assert.assertFalse(fD.isFailed("id", 10));
        Assert.assertFalse(fD.shouldPing("id", 10));
        Assert.assertEquals(10, fD.getIdleTime("id", 10));
        Assert.assertEquals(40, fD.getTimeToNextPing("id", 10)); // timeout/2

        Assert.assertFalse(fD.isFailed("id", 80));
        Assert.assertTrue(fD.shouldPing("id", 80));
        Assert.assertEquals(80, fD.getIdleTime("id", 80));
        // no ping sent, negative time to next ping
        Assert.assertEquals(-30, fD.getTimeToNextPing("id", 80));

        //one interval grace period, should fail at t=200
        Assert.assertFalse(fD.isFailed("id", 120));

        Assert.assertTrue(fD.isFailed("id", 220));
        
        fD.releaseMonitored("id");
        Assert.assertFalse(fD.isFailed("id", 250));
        Assert.assertFalse(fD.shouldPing("id", 250));
    }

    @Test
    public void testPingObjects() {
        // no hb, works like a fixed fd
        Assert.assertFalse(fD.isFailed("id", 0));
        fD.registerMonitored("id", 0, 100);

        Assert.assertFalse(fD.isFailed("id", 10));
        Assert.assertFalse(fD.shouldPing("id", 10));
        Assert.assertEquals(10, fD.getIdleTime("id", 10));
        Assert.assertEquals(40, fD.getTimeToNextPing("id", 10)); // timeout/2

        Assert.assertFalse(fD.isFailed("id", 50));
        Assert.assertTrue(fD.shouldPing("id", 50));
        Assert.assertEquals(80, fD.getIdleTime("id", 80));
        Assert.assertEquals(-30, fD.getTimeToNextPing("id", 80));

        fD.messageSent("id", 200, MessageType.PING);
        
        // We give a one interval grace period
        Assert.assertTrue(fD.isFailed("id", 220));
        Assert.assertFalse(fD.shouldPing("id", 220));
        Assert.assertEquals(220, fD.getIdleTime("id", 220));
        Assert.assertEquals(30, fD.getTimeToNextPing("id", 220));
    }

    @Test
    public void testPingReceiving() {
        Assert.assertFalse(fD.isFailed("id", 0));
        fD.registerMonitored("id", 0, 100);

        Assert.assertFalse(fD.isFailed("id", 10));
        Assert.assertEquals(10, fD.getIdleTime("id", 10));
        Assert.assertEquals(40, fD.getTimeToNextPing("id", 10)); // timeout/2

        fD.messageSent("id", 50, MessageType.PING);

        Assert.assertFalse(fD.isFailed("id", 60));
        Assert.assertEquals(80, fD.getIdleTime("id", 80));

        fD.messageReceived("id", 90, MessageType.PING);

        Assert.assertFalse(fD.isFailed("id", 120));
        Assert.assertEquals(30, fD.getIdleTime("id", 120));
        
        Assert.assertTrue(fD.isFailed("id", 210));
    }

}
