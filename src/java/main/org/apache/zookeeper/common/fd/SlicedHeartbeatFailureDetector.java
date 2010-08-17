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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A simple implementation of a heartbeat failure detector
 * based on time slices. Monitored objects are grouped by
 * their expiration time in time slices. When a time slice
 * expire, all the objects of that slice are considered failed.
 * While updating a monitored, this failure detector 
 * always rounds up the time slice to provide a sort of grace period.
 * This implementation relies on the application to set the 
 * timeouts of the monitored objects, and
 * these timeouts remain static.
 */
public class SlicedHeartbeatFailureDetector implements FailureDetector {

    private Map<String, Monitored> monitoreds = new HashMap<String, Monitored>();
    private Map<Long, MonitoredSet> monitoredSets = new TreeMap<Long, MonitoredSet>();
    private final long sliceSize;

    protected static final long DEFAULT_SLICE_SIZE = 3000;
    
    /**
     * Create a SlicedHeartbeatFailureDetector with specified 
     * time slice size in milliseconds.
     * 
     * @param sliceSize the time slice size
     */
    public SlicedHeartbeatFailureDetector(long sliceSize) {
        this.sliceSize = sliceSize;
    }
    
    /**
     * Create a SlicedHeartbeatFailureDetector with default 
     * slice size of 3000 milliseconds.
     */
    public SlicedHeartbeatFailureDetector() {
        this(DEFAULT_SLICE_SIZE);
    }
    
    
    @Override
    public void heartbeatReceived(String id, long now) {
        Monitored monitored = getMonitored(id);
        long expireTime = roundToInterval(now + monitored.timeout);
        monitored.lastHeard = now;
        
        if (monitored.expirationSlice >= expireTime) {
            return;
        }
        
        MonitoredSet set = monitoredSets.get(monitored.expirationSlice);
        if (set != null) {
            set.monitoreds.remove(monitored);
        }
        
        monitored.expirationSlice = expireTime;
        set = monitoredSets.get(monitored.expirationSlice);
        if (set == null) {
            set = new MonitoredSet();
            monitoredSets.put(expireTime, set);
        }
        set.monitoreds.add(monitored);
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
        monitored.timeout = timeout;

        monitoreds.put(id, monitored);
        
        heartbeatReceived(id, now);
    }

    @Override
    public void setTimeout(String id, long timeout) {
        Monitored monitored = getMonitored(id);
        monitored.timeout = timeout;
    }

    @Override
    public List<String> getFailedObjects(long now) {
        List<String> failed = new ArrayList<String>();

        for (Long expirationTime : monitoredSets.keySet()) {
            if (expirationTime > now) {
                break;
            }
            MonitoredSet set = monitoredSets.get(expirationTime);
            for (Monitored monitored : set.monitoreds) {
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
        Monitored monitored = monitoreds.get(id);
        if (monitored != null) {
            MonitoredSet set = monitoredSets.get(monitored.expirationSlice);
            if (set != null) {
                set.monitoreds.remove(monitored);
            }
        }
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
        long expirationSlice;
        
        public Monitored(String id) {
            this.id = id;
        }
    }
    
    private static class MonitoredSet {
        HashSet<Monitored> monitoreds = new HashSet<Monitored>();
    }

    @Override
    public void appMessageReceived(String id, long now) {
        heartbeatReceived(id, now);
    }

    @Override
    public void appMessageSent(String id, long now) {
        pingSent(id, now);
    }
    
    private long roundToInterval(long time) {
        // We give a one interval grace period
        return (time / sliceSize + 1) * sliceSize;
    }

    @Override
    public void updateHeartbeatSample(String id, long lastHbTimestamp,
            long interArrivalMean, long interArrivalStdDev) {
        heartbeatReceived(id, lastHbTimestamp);
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A simple implementation of a heartbeat failure detector
 * based on time slices. Monitored objects are grouped by
 * their expiration time in time slices. When a time slice
 * expire, all the objects of that slice are considered failed.
 * While updating a monitored, this failure detector 
 * always rounds up the time slice to provide a sort of grace period.
 * This implementation relies on the application to set the 
 * timeouts of the monitored objects, and
 * these timeouts remain static.
 */
public class SlicedHeartbeatFailureDetector implements FailureDetector {

    private Map<String, Monitored> monitoreds = new HashMap<String, Monitored>();
    private Map<Long, MonitoredSet> monitoredSets = new TreeMap<Long, MonitoredSet>();
    private final long sliceSize;

    protected static final long DEFAULT_SLICE_SIZE = 3000;
    
    /**
     * Create a SlicedHeartbeatFailureDetector with specified 
     * time slice size in milliseconds.
     * 
     * @param sliceSize the time slice size
     */
    public SlicedHeartbeatFailureDetector(long sliceSize) {
        this.sliceSize = sliceSize;
    }
    
    /**
     * Create a SlicedHeartbeatFailureDetector with default 
     * slice size of 3000 milliseconds.
     */
    public SlicedHeartbeatFailureDetector() {
        this(DEFAULT_SLICE_SIZE);
    }
    
    
    @Override
    public void heartbeatReceived(String id, long now) {
        Monitored monitored = getMonitored(id);
        long expireTime = roundToInterval(now + monitored.timeout);
        monitored.lastHeard = now;
        
        if (monitored.expirationSlice >= expireTime) {
            return;
        }
        
        MonitoredSet set = monitoredSets.get(monitored.expirationSlice);
        if (set != null) {
            set.monitoreds.remove(monitored);
        }
        
        monitored.expirationSlice = expireTime;
        set = monitoredSets.get(monitored.expirationSlice);
        if (set == null) {
            set = new MonitoredSet();
            monitoredSets.put(expireTime, set);
        }
        set.monitoreds.add(monitored);
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
        monitored.timeout = timeout;

        monitoreds.put(id, monitored);
        
        heartbeatReceived(id, now);
    }

    @Override
    public void setTimeout(String id, long timeout) {
        Monitored monitored = getMonitored(id);
        monitored.timeout = timeout;
    }

    @Override
    public List<String> getFailedObjects(long now) {
        List<String> failed = new ArrayList<String>();

        for (Long expirationTime : monitoredSets.keySet()) {
            if (expirationTime > now) {
                break;
            }
            MonitoredSet set = monitoredSets.get(expirationTime);
            for (Monitored monitored : set.monitoreds) {
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
        Monitored monitored = monitoreds.get(id);
        if (monitored != null) {
            MonitoredSet set = monitoredSets.get(monitored.expirationSlice);
            if (set != null) {
                set.monitoreds.remove(monitored);
            }
        }
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
        long expirationSlice;
        
        public Monitored(String id) {
            this.id = id;
        }
    }
    
    private static class MonitoredSet {
        HashSet<Monitored> monitoreds = new HashSet<Monitored>();
    }

    @Override
    public void appMessageReceived(String id, long now) {
        heartbeatReceived(id, now);
    }

    @Override
    public void appMessageSent(String id, long now) {
        pingSent(id, now);
    }
    
    private long roundToInterval(long time) {
        // We give a one interval grace period
        return (time / sliceSize + 1) * sliceSize;
    }

    @Override
    public void updateHeartbeatSample(String id, long lastHbTimestamp,
            long interArrivalMean, long interArrivalStdDev) {
        heartbeatReceived(id, lastHbTimestamp);
    }

}
