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

import java.util.List;

import junit.framework.Assert;

import org.apache.zookeeper.common.fd.BertierFailureDetector;
import org.apache.zookeeper.common.fd.FailureDetector;
import org.junit.Before;
import org.junit.Test;

public class BertierFDTest {

    private FailureDetector fD;

    @Before
    public void init() {
        this.fD = new BertierFailureDetector();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPingNoMonitoredRegistered() {
        fD.pingSent("id", 10l);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHeartbeatNoMonitoredRegistered() {
        fD.heartbeatReceived("id", 10l);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIdleTimeNoMonitoredRegistered() {
        fD.getIdleTime("id", 10l);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTimeToNextPingNoMonitoredRegistered() {
        fD.getTimeToNextPing("id", 10l);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTimeOutNoMonitoredRegistered() {
        fD.getTimeout("id");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegisterMonitoredTwice() {
        fD.registerMonitored("id", 100, 100);
        fD.registerMonitored("id", 100, 100);
    }

    @Test
    public void testNoPingNoHeartbeat() {
        // no hb, works like a fixed fd
        Assert.assertTrue(fD.getFailedObjects(0).isEmpty());
        fD.registerMonitored("id", 0, 100);

        List<String> failedObjects = fD.getFailedObjects(10);
        List<String> objectsToPing = fD.getObjectsToPing(10);

        Assert.assertTrue(failedObjects.isEmpty());
        Assert.assertTrue(objectsToPing.isEmpty());
        Assert.assertEquals(10, fD.getIdleTime("id", 10));
        Assert.assertEquals(40, fD.getTimeToNextPing("id", 10)); // timeout/2

        failedObjects = fD.getFailedObjects(80);
        objectsToPing = fD.getObjectsToPing(80);

        Assert.assertTrue(failedObjects.isEmpty());
        Assert.assertTrue(objectsToPing.contains("id"));
        Assert.assertEquals(80, fD.getIdleTime("id", 80));
        // no ping sent, negative time to next ping
        Assert.assertEquals(-30, fD.getTimeToNextPing("id", 80));

        failedObjects = fD.getFailedObjects(120);
        Assert.assertTrue(failedObjects.contains("id"));

        fD.release("id");
        failedObjects = fD.getFailedObjects(140);
        objectsToPing = fD.getObjectsToPing(140);
        Assert.assertTrue(failedObjects.isEmpty());
        Assert.assertTrue(objectsToPing.isEmpty());
    }

    @Test
    public void testPingObjects() {
        // no hb, works like a fixed fd
        Assert.assertTrue(fD.getFailedObjects(0).isEmpty());
        fD.registerMonitored("id", 0, 100);

        List<String> failedObjects = fD.getFailedObjects(10);
        List<String> objectsToPing = fD.getObjectsToPing(10);

        Assert.assertTrue(failedObjects.isEmpty());
        Assert.assertTrue(objectsToPing.isEmpty());
        Assert.assertEquals(10, fD.getIdleTime("id", 10));
        Assert.assertEquals(40, fD.getTimeToNextPing("id", 10)); // timeout/2

        failedObjects = fD.getFailedObjects(50);
        objectsToPing = fD.getObjectsToPing(50);

        Assert.assertTrue(failedObjects.isEmpty());
        Assert.assertTrue(objectsToPing.contains("id"));
        Assert.assertEquals(80, fD.getIdleTime("id", 80));
        Assert.assertEquals(-30, fD.getTimeToNextPing("id", 80));

        fD.pingSent("id", 90);

        failedObjects = fD.getFailedObjects(120);
        objectsToPing = fD.getObjectsToPing(120);

        Assert.assertTrue(failedObjects.contains("id"));
        Assert.assertTrue(objectsToPing.isEmpty());
        Assert.assertEquals(120, fD.getIdleTime("id", 120));
        Assert.assertEquals(20, fD.getTimeToNextPing("id", 120));
    }

    @Test
    public void testHeartbeatReceiving() {
        Assert.assertTrue(fD.getFailedObjects(0).isEmpty());
        fD.registerMonitored("id", 0, 100);

        Assert.assertEquals(0, fD.getIdleTime("id", 0));
        Assert.assertEquals(100, fD.getTimeout("id"));

        fD.pingSent("id", 50);
        fD.heartbeatReceived("id", 60);

        //only one heartbeat was received, static timeout is used
        Assert.assertEquals(100, fD.getTimeout("id"));

        fD.pingSent("id", 100);
        fD.heartbeatReceived("id", 120);

        // delay = timeout/4 = 25
        // error = now - ea - delay = 120 - 100 - 25 = -5
        // delay = delay + gamma*error = 25 + 0.1*(-5) = 25
        // var = var + gamma(|error| - var) = 0 + 0.1*(5 - 0) = 0.5
        // alpha = beta*delay + phi*var = 1*25 + 4*0.5 = 27

        // ea = 60 + 120 = 180
        // t = ea + alpha = 180 + 27 = 207
        // timeout = t - lastHeard = 87
        Assert.assertEquals(87, fD.getTimeout("id"));

        fD.pingSent("id", 150);
        fD.heartbeatReceived("id", 190);

        // error = now - ea - delay = 190 - 180 - 25 = -15
        // delay = delay + gamma*error = 25 + 0.1*-15 = 24
        // var = var + gamma(|error| - var) = 0.5 + 0.1*(15 - 0.5) = 1.95
        // alpha = beta*delay + phi*var = 1*24 + 4*1.95 = 32

        // ea = (60 + 70)/2 + 190 = 255
        // t = ea + alpha = 255 + 31.8 = 287
        // timeout = t - lastHeard = 142
        Assert.assertEquals(97, fD.getTimeout("id"));
    }

    @Test
    public void testAppHeartbeatReceiving() {
        Assert.assertTrue(fD.getFailedObjects(0).isEmpty());
        fD.registerMonitored("id", 0, 100);

        Assert.assertEquals(0, fD.getIdleTime("id", 0));
        Assert.assertEquals(100, fD.getTimeout("id"));

        fD.appMessageReceived("id", 60);
        Assert.assertEquals(100, fD.getTimeout("id"));

        Assert.assertTrue(fD.getFailedObjects(60).isEmpty());
        Assert.assertTrue(fD.getFailedObjects(120).isEmpty());
        Assert.assertTrue(fD.getFailedObjects(170).contains("id"));
    }

    @Test
    public void testAppHeartbeatReceivingAdaptiveTo() {
        fD.registerMonitored("id", 0, 100);

        fD.pingSent("id", 50);
        fD.heartbeatReceived("id", 60);

        fD.pingSent("id", 100);
        fD.heartbeatReceived("id", 120);

        // delay = timeout/4 = 25
        // error = now - ea - delay = 120 - 100 - 25 = -5
        // delay = delay + gamma*error = 25 + 0.1*(-5) = 25
        // var = var + gamma(|error| - var) = 0 + 0.1*(5 - 0) = 0.5
        // alpha = beta*delay + phi*var = 1*25 + 4*0.5 = 27

        // ea = 60 + 120 = 180
        // t = ea + alpha = 180 + 27 = 207
        // timeout = t - lastHeard = 87
        Assert.assertEquals(87, fD.getTimeout("id"));

        fD.appMessageReceived("id", 160);
        Assert.assertEquals(87, fD.getTimeout("id"));

        // should have failed if no app message was received
        Assert.assertTrue(fD.getFailedObjects(240).isEmpty());
        Assert.assertTrue(fD.getFailedObjects(250).contains("id"));
    }
}
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

import java.util.List;

import junit.framework.Assert;

import org.apache.zookeeper.common.fd.BertierFailureDetector;
import org.apache.zookeeper.common.fd.FailureDetector;
import org.junit.Before;
import org.junit.Test;

public class BertierFDTest {

    private FailureDetector fD;

    @Before
    public void init() {
        this.fD = new BertierFailureDetector();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPingNoMonitoredRegistered() {
        fD.pingSent("id", 10l);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHeartbeatNoMonitoredRegistered() {
        fD.heartbeatReceived("id", 10l);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIdleTimeNoMonitoredRegistered() {
        fD.getIdleTime("id", 10l);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTimeToNextPingNoMonitoredRegistered() {
        fD.getTimeToNextPing("id", 10l);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTimeOutNoMonitoredRegistered() {
        fD.getTimeout("id");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegisterMonitoredTwice() {
        fD.registerMonitored("id", 100, 100);
        fD.registerMonitored("id", 100, 100);
    }

    @Test
    public void testNoPingNoHeartbeat() {
        // no hb, works like a fixed fd
        Assert.assertTrue(fD.getFailedObjects(0).isEmpty());
        fD.registerMonitored("id", 0, 100);

        List<String> failedObjects = fD.getFailedObjects(10);
        List<String> objectsToPing = fD.getObjectsToPing(10);

        Assert.assertTrue(failedObjects.isEmpty());
        Assert.assertTrue(objectsToPing.isEmpty());
        Assert.assertEquals(10, fD.getIdleTime("id", 10));
        Assert.assertEquals(40, fD.getTimeToNextPing("id", 10)); // timeout/2

        failedObjects = fD.getFailedObjects(80);
        objectsToPing = fD.getObjectsToPing(80);

        Assert.assertTrue(failedObjects.isEmpty());
        Assert.assertTrue(objectsToPing.contains("id"));
        Assert.assertEquals(80, fD.getIdleTime("id", 80));
        // no ping sent, negative time to next ping
        Assert.assertEquals(-30, fD.getTimeToNextPing("id", 80));

        failedObjects = fD.getFailedObjects(120);
        Assert.assertTrue(failedObjects.contains("id"));

        fD.release("id");
        failedObjects = fD.getFailedObjects(140);
        objectsToPing = fD.getObjectsToPing(140);
        Assert.assertTrue(failedObjects.isEmpty());
        Assert.assertTrue(objectsToPing.isEmpty());
    }

    @Test
    public void testPingObjects() {
        // no hb, works like a fixed fd
        Assert.assertTrue(fD.getFailedObjects(0).isEmpty());
        fD.registerMonitored("id", 0, 100);

        List<String> failedObjects = fD.getFailedObjects(10);
        List<String> objectsToPing = fD.getObjectsToPing(10);

        Assert.assertTrue(failedObjects.isEmpty());
        Assert.assertTrue(objectsToPing.isEmpty());
        Assert.assertEquals(10, fD.getIdleTime("id", 10));
        Assert.assertEquals(40, fD.getTimeToNextPing("id", 10)); // timeout/2

        failedObjects = fD.getFailedObjects(50);
        objectsToPing = fD.getObjectsToPing(50);

        Assert.assertTrue(failedObjects.isEmpty());
        Assert.assertTrue(objectsToPing.contains("id"));
        Assert.assertEquals(80, fD.getIdleTime("id", 80));
        Assert.assertEquals(-30, fD.getTimeToNextPing("id", 80));

        fD.pingSent("id", 90);

        failedObjects = fD.getFailedObjects(120);
        objectsToPing = fD.getObjectsToPing(120);

        Assert.assertTrue(failedObjects.contains("id"));
        Assert.assertTrue(objectsToPing.isEmpty());
        Assert.assertEquals(120, fD.getIdleTime("id", 120));
        Assert.assertEquals(20, fD.getTimeToNextPing("id", 120));
    }

    @Test
    public void testHeartbeatReceiving() {
        Assert.assertTrue(fD.getFailedObjects(0).isEmpty());
        fD.registerMonitored("id", 0, 100);

        Assert.assertEquals(0, fD.getIdleTime("id", 0));
        Assert.assertEquals(100, fD.getTimeout("id"));

        fD.pingSent("id", 50);
        fD.heartbeatReceived("id", 60);

        //only one heartbeat was received, static timeout is used
        Assert.assertEquals(100, fD.getTimeout("id"));

        fD.pingSent("id", 100);
        fD.heartbeatReceived("id", 120);

        // delay = timeout/4 = 25
        // error = now - ea - delay = 120 - 100 - 25 = -5
        // delay = delay + gamma*error = 25 + 0.1*(-5) = 25
        // var = var + gamma(|error| - var) = 0 + 0.1*(5 - 0) = 0.5
        // alpha = beta*delay + phi*var = 1*25 + 4*0.5 = 27

        // ea = 60 + 120 = 180
        // t = ea + alpha = 180 + 27 = 207
        // timeout = t - lastHeard = 87
        Assert.assertEquals(87, fD.getTimeout("id"));

        fD.pingSent("id", 150);
        fD.heartbeatReceived("id", 190);

        // error = now - ea - delay = 190 - 180 - 25 = -15
        // delay = delay + gamma*error = 25 + 0.1*-15 = 24
        // var = var + gamma(|error| - var) = 0.5 + 0.1*(15 - 0.5) = 1.95
        // alpha = beta*delay + phi*var = 1*24 + 4*1.95 = 32

        // ea = (60 + 70)/2 + 190 = 255
        // t = ea + alpha = 255 + 31.8 = 287
        // timeout = t - lastHeard = 142
        Assert.assertEquals(97, fD.getTimeout("id"));
    }

    @Test
    public void testAppHeartbeatReceiving() {
        Assert.assertTrue(fD.getFailedObjects(0).isEmpty());
        fD.registerMonitored("id", 0, 100);

        Assert.assertEquals(0, fD.getIdleTime("id", 0));
        Assert.assertEquals(100, fD.getTimeout("id"));

        fD.appMessageReceived("id", 60);
        Assert.assertEquals(100, fD.getTimeout("id"));

        Assert.assertTrue(fD.getFailedObjects(60).isEmpty());
        Assert.assertTrue(fD.getFailedObjects(120).isEmpty());
        Assert.assertTrue(fD.getFailedObjects(170).contains("id"));
    }

    @Test
    public void testAppHeartbeatReceivingAdaptiveTo() {
        fD.registerMonitored("id", 0, 100);

        fD.pingSent("id", 50);
        fD.heartbeatReceived("id", 60);

        fD.pingSent("id", 100);
        fD.heartbeatReceived("id", 120);

        // delay = timeout/4 = 25
        // error = now - ea - delay = 120 - 100 - 25 = -5
        // delay = delay + gamma*error = 25 + 0.1*(-5) = 25
        // var = var + gamma(|error| - var) = 0 + 0.1*(5 - 0) = 0.5
        // alpha = beta*delay + phi*var = 1*25 + 4*0.5 = 27

        // ea = 60 + 120 = 180
        // t = ea + alpha = 180 + 27 = 207
        // timeout = t - lastHeard = 87
        Assert.assertEquals(87, fD.getTimeout("id"));

        fD.appMessageReceived("id", 160);
        Assert.assertEquals(87, fD.getTimeout("id"));

        // should have failed if no app message was received
        Assert.assertTrue(fD.getFailedObjects(240).isEmpty());
        Assert.assertTrue(fD.getFailedObjects(250).contains("id"));
    }
}
