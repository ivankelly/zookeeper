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

import org.apache.zookeeper.common.fd.BertierFailureDetector;
import org.apache.zookeeper.common.fd.FailureDetector;
import org.apache.zookeeper.common.fd.MessageType;
import org.junit.Before;
import org.junit.Test;

public class BertierFDTest {

    private FailureDetector fD;

    @Before
    public void init() {
        this.fD = new BertierFailureDetector();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPingSentNoMonitoredRegistered() {
        fD.messageSent("id", 10l, MessageType.PING);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPingReceivedNoMonitoredRegistered() {
        fD.messageReceived("id", 10l, MessageType.PING);
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
    public void testNoPingSentOrReceived() {
        // no hb, works like a fixed fd
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

        Assert.assertTrue(fD.isFailed("id", 120));

        fD.releaseMonitored("id");
        Assert.assertFalse(fD.isFailed("id", 140));
        Assert.assertFalse(fD.shouldPing("id", 140));
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

        fD.messageSent("id", 90, MessageType.PING);

        Assert.assertTrue(fD.isFailed("id", 120));
        Assert.assertFalse(fD.shouldPing("id", 120));
        Assert.assertEquals(120, fD.getIdleTime("id", 120));
        Assert.assertEquals(20, fD.getTimeToNextPing("id", 120));
    }

    @Test
    public void testPingReceiving() {
        Assert.assertFalse(fD.isFailed("id", 0));
        fD.registerMonitored("id", 0, 100);

        Assert.assertEquals(0, fD.getIdleTime("id", 0));
        Assert.assertEquals(100, fD.getTimeout("id"));

        fD.messageSent("id", 50, MessageType.PING);
        fD.messageReceived("id", 60, MessageType.PING);

        //only one ping was received, static timeout is used
        Assert.assertEquals(100, fD.getTimeout("id"));

        fD.messageSent("id", 100, MessageType.PING);
        fD.messageReceived("id", 120, MessageType.PING);

        // delay = timeout/4 = 25
        // error = now - ea - delay = 120 - 100 - 25 = -5
        // delay = delay + gamma*error = 25 + 0.1*(-5) = 25
        // var = var + gamma(|error| - var) = 0 + 0.1*(5 - 0) = 0.5
        // alpha = beta*delay + phi*var = 1*25 + 4*0.5 = 27

        // ea = 60 + 120 = 180
        // t = ea + alpha = 180 + 27 = 207
        // timeout = t - lastHeard = 87
        Assert.assertEquals(87, fD.getTimeout("id"));

        fD.messageSent("id", 150, MessageType.PING);
        fD.messageReceived("id", 190, MessageType.PING);

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
    public void testAppMessageReceiving() {
        Assert.assertFalse(fD.isFailed("id", 0));
        fD.registerMonitored("id", 0, 100);

        Assert.assertEquals(0, fD.getIdleTime("id", 0));
        Assert.assertEquals(100, fD.getTimeout("id"));

        fD.messageReceived("id", 60, MessageType.APPLICATION);
        Assert.assertEquals(100, fD.getTimeout("id"));

        Assert.assertFalse(fD.isFailed("id", 60));
        Assert.assertFalse(fD.isFailed("id", 120));
        Assert.assertTrue(fD.isFailed("id", 170));
    }

    @Test
    public void testAppMessageReceivingAdaptiveTo() {
        fD.registerMonitored("id", 0, 100);

        fD.messageSent("id", 50, MessageType.PING);
        fD.messageReceived("id", 60, MessageType.PING);

        fD.messageSent("id", 100, MessageType.PING);
        fD.messageReceived("id", 120, MessageType.PING);

        // delay = timeout/4 = 25
        // error = now - ea - delay = 120 - 100 - 25 = -5
        // delay = delay + gamma*error = 25 + 0.1*(-5) = 25
        // var = var + gamma(|error| - var) = 0 + 0.1*(5 - 0) = 0.5
        // alpha = beta*delay + phi*var = 1*25 + 4*0.5 = 27

        // ea = 60 + 120 = 180
        // t = ea + alpha = 180 + 27 = 207
        // timeout = t - lastHeard = 87
        Assert.assertEquals(87, fD.getTimeout("id"));

        fD.messageReceived("id", 160, MessageType.APPLICATION);
        Assert.assertEquals(87, fD.getTimeout("id"));

        // should have failed if no app message was received
        Assert.assertFalse(fD.isFailed("id", 240));
        Assert.assertTrue(fD.isFailed("id", 250));
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

import junit.framework.Assert;

import org.apache.zookeeper.common.fd.BertierFailureDetector;
import org.apache.zookeeper.common.fd.FailureDetector;
import org.apache.zookeeper.common.fd.MessageType;
import org.junit.Before;
import org.junit.Test;

public class BertierFDTest {

    private FailureDetector fD;

    @Before
    public void init() {
        this.fD = new BertierFailureDetector();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPingSentNoMonitoredRegistered() {
        fD.messageSent("id", 10l, MessageType.PING);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPingReceivedNoMonitoredRegistered() {
        fD.messageReceived("id", 10l, MessageType.PING);
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
    public void testNoPingSentOrReceived() {
        // no hb, works like a fixed fd
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

        Assert.assertTrue(fD.isFailed("id", 120));

        fD.releaseMonitored("id");
        Assert.assertFalse(fD.isFailed("id", 140));
        Assert.assertFalse(fD.shouldPing("id", 140));
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

        fD.messageSent("id", 90, MessageType.PING);

        Assert.assertTrue(fD.isFailed("id", 120));
        Assert.assertFalse(fD.shouldPing("id", 120));
        Assert.assertEquals(120, fD.getIdleTime("id", 120));
        Assert.assertEquals(20, fD.getTimeToNextPing("id", 120));
    }

    @Test
    public void testPingReceiving() {
        Assert.assertFalse(fD.isFailed("id", 0));
        fD.registerMonitored("id", 0, 100);

        Assert.assertEquals(0, fD.getIdleTime("id", 0));
        Assert.assertEquals(100, fD.getTimeout("id"));

        fD.messageSent("id", 50, MessageType.PING);
        fD.messageReceived("id", 60, MessageType.PING);

        //only one ping was received, static timeout is used
        Assert.assertEquals(100, fD.getTimeout("id"));

        fD.messageSent("id", 100, MessageType.PING);
        fD.messageReceived("id", 120, MessageType.PING);

        // delay = timeout/4 = 25
        // error = now - ea - delay = 120 - 100 - 25 = -5
        // delay = delay + gamma*error = 25 + 0.1*(-5) = 25
        // var = var + gamma(|error| - var) = 0 + 0.1*(5 - 0) = 0.5
        // alpha = beta*delay + phi*var = 1*25 + 4*0.5 = 27

        // ea = 60 + 120 = 180
        // t = ea + alpha = 180 + 27 = 207
        // timeout = t - lastHeard = 87
        Assert.assertEquals(87, fD.getTimeout("id"));

        fD.messageSent("id", 150, MessageType.PING);
        fD.messageReceived("id", 190, MessageType.PING);

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
    public void testAppMessageReceiving() {
        Assert.assertFalse(fD.isFailed("id", 0));
        fD.registerMonitored("id", 0, 100);

        Assert.assertEquals(0, fD.getIdleTime("id", 0));
        Assert.assertEquals(100, fD.getTimeout("id"));

        fD.messageReceived("id", 60, MessageType.APPLICATION);
        Assert.assertEquals(100, fD.getTimeout("id"));

        Assert.assertFalse(fD.isFailed("id", 60));
        Assert.assertFalse(fD.isFailed("id", 120));
        Assert.assertTrue(fD.isFailed("id", 170));
    }

    @Test
    public void testAppMessageReceivingAdaptiveTo() {
        fD.registerMonitored("id", 0, 100);

        fD.messageSent("id", 50, MessageType.PING);
        fD.messageReceived("id", 60, MessageType.PING);

        fD.messageSent("id", 100, MessageType.PING);
        fD.messageReceived("id", 120, MessageType.PING);

        // delay = timeout/4 = 25
        // error = now - ea - delay = 120 - 100 - 25 = -5
        // delay = delay + gamma*error = 25 + 0.1*(-5) = 25
        // var = var + gamma(|error| - var) = 0 + 0.1*(5 - 0) = 0.5
        // alpha = beta*delay + phi*var = 1*25 + 4*0.5 = 27

        // ea = 60 + 120 = 180
        // t = ea + alpha = 180 + 27 = 207
        // timeout = t - lastHeard = 87
        Assert.assertEquals(87, fD.getTimeout("id"));

        fD.messageReceived("id", 160, MessageType.APPLICATION);
        Assert.assertEquals(87, fD.getTimeout("id"));

        // should have failed if no app message was received
        Assert.assertFalse(fD.isFailed("id", 240));
        Assert.assertTrue(fD.isFailed("id", 250));
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

import junit.framework.Assert;

import org.apache.zookeeper.common.fd.BertierFailureDetector;
import org.apache.zookeeper.common.fd.FailureDetector;
import org.apache.zookeeper.common.fd.MessageType;
import org.junit.Before;
import org.junit.Test;

public class BertierFDTest {

    private FailureDetector fD;

    @Before
    public void init() {
        this.fD = new BertierFailureDetector();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPingSentNoMonitoredRegistered() {
        fD.messageSent("id", 10l, MessageType.PING);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPingReceivedNoMonitoredRegistered() {
        fD.messageReceived("id", 10l, MessageType.PING);
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
    public void testNoPingSentOrReceived() {
        // no hb, works like a fixed fd
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

        Assert.assertTrue(fD.isFailed("id", 120));

        fD.releaseMonitored("id");
        Assert.assertFalse(fD.isFailed("id", 140));
        Assert.assertFalse(fD.shouldPing("id", 140));
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

        fD.messageSent("id", 90, MessageType.PING);

        Assert.assertTrue(fD.isFailed("id", 120));
        Assert.assertFalse(fD.shouldPing("id", 120));
        Assert.assertEquals(120, fD.getIdleTime("id", 120));
        Assert.assertEquals(20, fD.getTimeToNextPing("id", 120));
    }

    @Test
    public void testPingReceiving() {
        Assert.assertFalse(fD.isFailed("id", 0));
        fD.registerMonitored("id", 0, 100);

        Assert.assertEquals(0, fD.getIdleTime("id", 0));
        Assert.assertEquals(100, fD.getTimeout("id"));

        fD.messageSent("id", 50, MessageType.PING);
        fD.messageReceived("id", 60, MessageType.PING);

        //only one ping was received, static timeout is used
        Assert.assertEquals(100, fD.getTimeout("id"));

        fD.messageSent("id", 100, MessageType.PING);
        fD.messageReceived("id", 120, MessageType.PING);

        // delay = timeout/4 = 25
        // error = now - ea - delay = 120 - 100 - 25 = -5
        // delay = delay + gamma*error = 25 + 0.1*(-5) = 25
        // var = var + gamma(|error| - var) = 0 + 0.1*(5 - 0) = 0.5
        // alpha = beta*delay + phi*var = 1*25 + 4*0.5 = 27

        // ea = 60 + 120 = 180
        // t = ea + alpha = 180 + 27 = 207
        // timeout = t - lastHeard = 87
        Assert.assertEquals(87, fD.getTimeout("id"));

        fD.messageSent("id", 150, MessageType.PING);
        fD.messageReceived("id", 190, MessageType.PING);

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
    public void testAppMessageReceiving() {
        Assert.assertFalse(fD.isFailed("id", 0));
        fD.registerMonitored("id", 0, 100);

        Assert.assertEquals(0, fD.getIdleTime("id", 0));
        Assert.assertEquals(100, fD.getTimeout("id"));

        fD.messageReceived("id", 60, MessageType.APPLICATION);
        Assert.assertEquals(100, fD.getTimeout("id"));

        Assert.assertFalse(fD.isFailed("id", 60));
        Assert.assertFalse(fD.isFailed("id", 120));
        Assert.assertTrue(fD.isFailed("id", 170));
    }

    @Test
    public void testAppMessageReceivingAdaptiveTo() {
        fD.registerMonitored("id", 0, 100);

        fD.messageSent("id", 50, MessageType.PING);
        fD.messageReceived("id", 60, MessageType.PING);

        fD.messageSent("id", 100, MessageType.PING);
        fD.messageReceived("id", 120, MessageType.PING);

        // delay = timeout/4 = 25
        // error = now - ea - delay = 120 - 100 - 25 = -5
        // delay = delay + gamma*error = 25 + 0.1*(-5) = 25
        // var = var + gamma(|error| - var) = 0 + 0.1*(5 - 0) = 0.5
        // alpha = beta*delay + phi*var = 1*25 + 4*0.5 = 27

        // ea = 60 + 120 = 180
        // t = ea + alpha = 180 + 27 = 207
        // timeout = t - lastHeard = 87
        Assert.assertEquals(87, fD.getTimeout("id"));

        fD.messageReceived("id", 160, MessageType.APPLICATION);
        Assert.assertEquals(87, fD.getTimeout("id"));

        // should have failed if no app message was received
        Assert.assertFalse(fD.isFailed("id", 240));
        Assert.assertTrue(fD.isFailed("id", 250));
    }
}
