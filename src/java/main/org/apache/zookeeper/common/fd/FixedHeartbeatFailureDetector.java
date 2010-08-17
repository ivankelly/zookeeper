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
package org.apache.zookeeper.common.fd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A simple implementation of a heartbeat failure detector. 
 * This implementation relies on the application to set the 
 * timeouts of the monitored objects, and
 * these timeouts remain static.
 */
public class FixedHeartbeatFailureDetector implements FailureDetector {

    private Map<String, Monitored> monitoreds = new HashMap<String, Monitored>();

    @Override
    public void heartbeatReceived(String id, long now) {
        Monitored monitored = getMonitored(id);
        monitored.lastHeard = now;
    }

    @Override
    public void updateHeartbeatSample(String id, long now,
            long interArrivalMean, long interArrivalStdDev) {
        heartbeatReceived(id, now);
    }
    
    @Override
    public void pingSent(String id, long now) {
        Monitored monitored = getMonitored(id);
        monitored.lastSent = now;
    }

    @Override
    public void registerMonitored(String id, long now, long timeout) {
        Monitored monitored = monitoreds.get(id);
        if (monitored != null) {
            throw new IllegalArgumentException("Object with id " + id
                    + " is already monitored");
        }
        monitored = new Monitored(id);
        monitored.lastHeard = now;
        monitored.lastSent = now;
        monitored.timeout = timeout;

        monitoreds.put(id, monitored);
    }

    @Override
    public void setTimeout(String id, long timeout) {
        Monitored monitored = getMonitored(id);
        monitored.timeout = timeout;
    }

    @Override
    public List<String> getFailedObjects(long now) {
        List<String> failed = new ArrayList<String>();

        for (Monitored monitored : monitoreds.values()) {
            Long timeout = monitored.timeout;

            if (getIdleTime(monitored.id, now) >= timeout) {
                failed.add(monitored.id);
            }
        }

        return failed;
    }

    @Override
    public List<String> getObjectsToPing(long now) {
        List<String> toBePinged = new ArrayList<String>();

        for (String monitoredId : monitoreds.keySet()) {
            long timeToNextPing = getTimeToNextPing(monitoredId, now);

            if (timeToNextPing <= 0) {
                toBePinged.add(monitoredId);
            }
        }

        return toBePinged;
    }

    @Override
    public long getIdleTime(String id, long now) {
        Monitored monitored = getMonitored(id);
        return now - monitored.lastHeard;
    }

    @Override
    public long getTimeout(String id) {
        return getMonitored(id).timeout;
    }

    @Override
    public long getTimeToNextPing(String id, long now) {
        Monitored monitored = getMonitored(id);
        Long lastPinged = monitored.lastSent;
        Long timeout = monitored.timeout;

        return timeout / 2 - (now - lastPinged);
    }

    @Override
    public void release(String id) {
        monitoreds.remove(id);
    }

    private Monitored getMonitored(String id) {
        Monitored monitored = monitoreds.get(id);
        if (monitored == null) {
            throw new IllegalArgumentException(
                    "The monitored must be registered first");
        }
        return monitored;
    }

    private static class Monitored {
        String id;

        long timeout;
        long lastSent;
        long lastHeard;

        public Monitored(String id) {
            this.id = id;
        }
    }

    @Override
    public void appMessageReceived(String id, long now) {
        heartbeatReceived(id, now);
    }

    @Override
    public void appMessageSent(String id, long now) {
        pingSent(id, now);
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
package org.apache.zookeeper.common.fd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A simple implementation of a heartbeat failure detector. 
 * This implementation relies on the application to set the 
 * timeouts of the monitored objects, and
 * these timeouts remain static.
 */
public class FixedHeartbeatFailureDetector implements FailureDetector {

    private Map<String, Monitored> monitoreds = new HashMap<String, Monitored>();

    @Override
    public void heartbeatReceived(String id, long now) {
        Monitored monitored = getMonitored(id);
        monitored.lastHeard = now;
    }

    @Override
    public void updateHeartbeatSample(String id, long now,
            long interArrivalMean, long interArrivalStdDev) {
        heartbeatReceived(id, now);
    }
    
    @Override
    public void pingSent(String id, long now) {
        Monitored monitored = getMonitored(id);
        monitored.lastSent = now;
    }

    @Override
    public void registerMonitored(String id, long now, long timeout) {
        Monitored monitored = monitoreds.get(id);
        if (monitored != null) {
            throw new IllegalArgumentException("Object with id " + id
                    + " is already monitored");
        }
        monitored = new Monitored(id);
        monitored.lastHeard = now;
        monitored.lastSent = now;
        monitored.timeout = timeout;

        monitoreds.put(id, monitored);
    }

    @Override
    public void setTimeout(String id, long timeout) {
        Monitored monitored = getMonitored(id);
        monitored.timeout = timeout;
    }

    @Override
    public List<String> getFailedObjects(long now) {
        List<String> failed = new ArrayList<String>();

        for (Monitored monitored : monitoreds.values()) {
            Long timeout = monitored.timeout;

            if (getIdleTime(monitored.id, now) >= timeout) {
                failed.add(monitored.id);
            }
        }

        return failed;
    }

    @Override
    public List<String> getObjectsToPing(long now) {
        List<String> toBePinged = new ArrayList<String>();

        for (String monitoredId : monitoreds.keySet()) {
            long timeToNextPing = getTimeToNextPing(monitoredId, now);

            if (timeToNextPing <= 0) {
                toBePinged.add(monitoredId);
            }
        }

        return toBePinged;
    }

    @Override
    public long getIdleTime(String id, long now) {
        Monitored monitored = getMonitored(id);
        return now - monitored.lastHeard;
    }

    @Override
    public long getTimeout(String id) {
        return getMonitored(id).timeout;
    }

    @Override
    public long getTimeToNextPing(String id, long now) {
        Monitored monitored = getMonitored(id);
        Long lastPinged = monitored.lastSent;
        Long timeout = monitored.timeout;

        return timeout / 2 - (now - lastPinged);
    }

    @Override
    public void release(String id) {
        monitoreds.remove(id);
    }

    private Monitored getMonitored(String id) {
        Monitored monitored = monitoreds.get(id);
        if (monitored == null) {
            throw new IllegalArgumentException(
                    "The monitored must be registered first");
        }
        return monitored;
    }

    private static class Monitored {
        String id;

        long timeout;
        long lastSent;
        long lastHeard;

        public Monitored(String id) {
            this.id = id;
        }
    }

    @Override
    public void appMessageReceived(String id, long now) {
        heartbeatReceived(id, now);
    }

    @Override
    public void appMessageSent(String id, long now) {
        pingSent(id, now);
    }

}
