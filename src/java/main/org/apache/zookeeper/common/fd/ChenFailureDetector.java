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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Failure detector implementation according to 
 * Chen method, as described in its paper 'On the 
 * Quality of Service of Failure Detectors'. Chen's method 
 * uses the average of the received heartbeats dates added 
 * to a safety margin parameter called alpha in order to 
 * estimated the next timeout.
 */
public class ChenFailureDetector implements FailureDetector {

    protected static final long DEFAULT_ALPHA = 1250;

    /**
     * Sampling window size
     */
    private static final int N = 1000;
    
    
    private Map<String, Monitored> monitoreds = new HashMap<String, Monitored>();
    private long alpha;
    
    /**
     * Create a ChenFailureDetector with specified alpha parameter
     * @param alpha safety margin parameter
     */
    public ChenFailureDetector(long alpha) {
        this.alpha = alpha;
    }
    
    /**
     * Create a ChenFailureDetector with default values 
     * for the alpha parameter: alpha = 1250 ms
     */
    public ChenFailureDetector() {
        this(DEFAULT_ALPHA);
    }
    
    @Override
    public long getIdleTime(String id, long now) {
        Monitored monitored = getMonitored(id);
        return now - monitored.lastHeard;
    }

    @Override
    public List<String> getFailedObjects(long now) {
        List<String> failed = new ArrayList<String>();

        for (Monitored monitored : monitoreds.values()) {
            if (now > monitored.lastHeard + getTimeout(monitored.id)) {
                failed.add(monitored.id);
            }
        }

        return failed;
    }

    @Override
    public List<String> getObjectsToPing(long now) {
        List<String> toBePinged = new ArrayList<String>();

        for (String monitored : monitoreds.keySet()) {
            long timeToNextPing = getTimeToNextPing(monitored, now);

            if (timeToNextPing <= 0) {
                toBePinged.add(monitored);
            }
        }

        return toBePinged;
    }

    @Override
    public long getTimeToNextPing(String id, long now) {
        Monitored monitored = getMonitored(id);
        return (monitored.lastSent + monitored.eta) - now;
    }

    @Override
    public long getTimeout(String id) {
        Monitored monitored = getMonitored(id);
        if (monitored.heartbeats.size() < 2) {
            return monitored.staticTimeout;
        }

        return monitored.timeout;
    }

    @Override
    public void heartbeatReceived(String id, long now) {
        Monitored monitored = getMonitored(id);

        List<Heartbeat> list = monitored.heartbeats;
        addHeartbeat(new Heartbeat(now - monitored.lastHeard), list);

        updateMonitoredTimeout(now, monitored);
    }
    
    @Override
    public void updateHeartbeatSample(String id, long lastHbTimestamp, 
            long interArrivalMean, long interArrivalStdDev) {
        Monitored monitored = getMonitored(id);
        
        List<Heartbeat> list = monitored.heartbeats;
        list.clear();
        
        addHeartbeat(new Heartbeat(interArrivalMean), list);
        updateMonitoredTimeout(lastHbTimestamp, monitored);
    }

    private void updateMonitoredTimeout(long now, Monitored monitored) {
        
        if (monitored.heartbeats.size() >= 2) {
            Double eA = calcEA(monitored, now);
            long t = eA.longValue() + alpha;
            monitored.timeout = t - now;
        }
        
        monitored.lastHeard = now;
    }

    private Double calcEA(Monitored monitored, long now) {
        double avgIA = 0;
        Iterator<Heartbeat> iterator = monitored.heartbeats.iterator();
        iterator.next();
        
        while (iterator.hasNext()) {
            avgIA += iterator.next().interarrival / (monitored.heartbeats.size() - 1);
        }
        
        return now + avgIA;
    }

    private Monitored getMonitored(String id) {
        Monitored monitored = monitoreds.get(id);
        if (monitored == null) {
            throw new IllegalArgumentException(
                    "The monitored must be registered first");
        }
        return monitored;
    }

    private void addHeartbeat(Heartbeat hb, List<Heartbeat> list) {
        if (list.size() == N) {
            list.remove(0);
        }
        list.add(hb);
    }

    @Override
    public void pingSent(String id, long now) {
        Monitored monitored = getMonitored(id);
        monitored.lastSent = now;
    }

    @Override
    public void appMessageReceived(String id, long now) {
        Monitored monitored = getMonitored(id);
        monitored.lastHeard = now;
    }

    @Override
    public void appMessageSent(String id, long now) {
        pingSent(id, now);
    }

    @Override
    public void release(String id) {
        monitoreds.remove(id);
    }

    @Override
    public void registerMonitored(String id, long now, long timeout) {
        Monitored monitored = monitoreds.get(id);
        if (monitored != null) {
            throw new IllegalArgumentException("Object with id " + id
                    + " is already monitored");
        }
        monitored = new Monitored(id);

        monitored.eta = timeout / 2;
        monitored.lastHeard = now;
        monitored.lastSent = now;

        monitored.staticTimeout = timeout;
        monitored.timeout = timeout;

        monitoreds.put(id, monitored);

    }

    @Override
    public void setTimeout(String id, long timeout) {
        Monitored monitored = getMonitored(id);
        monitored.staticTimeout = timeout;
        monitored.eta = timeout / 2;
    }

    private static class Monitored {

        private String id;
        List<Heartbeat> heartbeats = new LinkedList<Heartbeat>();
        long lastSent; //any msg sent
        long lastHeard; //any msg received

        long staticTimeout; //static timeout
        long timeout; //dynamic timeout

        long eta; //interrogation delay

        public Monitored(String id) {
            this.id = id;
        }
    }

    private static class Heartbeat {
        long interarrival;

        public Heartbeat(long interarrival) {
            this.interarrival = interarrival;
        }
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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Failure detector implementation according to 
 * Chen method, as described in its paper 'On the 
 * Quality of Service of Failure Detectors'. Chen's method 
 * uses the average of the received heartbeats dates added 
 * to a safety margin parameter called alpha in order to 
 * estimated the next timeout.
 */
public class ChenFailureDetector implements FailureDetector {

    protected static final long DEFAULT_ALPHA = 1250;

    /**
     * Sampling window size
     */
    private static final int N = 1000;
    
    
    private Map<String, Monitored> monitoreds = new HashMap<String, Monitored>();
    private long alpha;
    
    /**
     * Create a ChenFailureDetector with specified alpha parameter
     * @param alpha safety margin parameter
     */
    public ChenFailureDetector(long alpha) {
        this.alpha = alpha;
    }
    
    /**
     * Create a ChenFailureDetector with default values 
     * for the alpha parameter: alpha = 1250 ms
     */
    public ChenFailureDetector() {
        this(DEFAULT_ALPHA);
    }
    
    @Override
    public long getIdleTime(String id, long now) {
        Monitored monitored = getMonitored(id);
        return now - monitored.lastHeard;
    }

    @Override
    public List<String> getFailedObjects(long now) {
        List<String> failed = new ArrayList<String>();

        for (Monitored monitored : monitoreds.values()) {
            if (now > monitored.lastHeard + getTimeout(monitored.id)) {
                failed.add(monitored.id);
            }
        }

        return failed;
    }

    @Override
    public List<String> getObjectsToPing(long now) {
        List<String> toBePinged = new ArrayList<String>();

        for (String monitored : monitoreds.keySet()) {
            long timeToNextPing = getTimeToNextPing(monitored, now);

            if (timeToNextPing <= 0) {
                toBePinged.add(monitored);
            }
        }

        return toBePinged;
    }

    @Override
    public long getTimeToNextPing(String id, long now) {
        Monitored monitored = getMonitored(id);
        return (monitored.lastSent + monitored.eta) - now;
    }

    @Override
    public long getTimeout(String id) {
        Monitored monitored = getMonitored(id);
        if (monitored.heartbeats.size() < 2) {
            return monitored.staticTimeout;
        }

        return monitored.timeout;
    }

    @Override
    public void heartbeatReceived(String id, long now) {
        Monitored monitored = getMonitored(id);

        List<Heartbeat> list = monitored.heartbeats;
        addHeartbeat(new Heartbeat(now - monitored.lastHeard), list);

        updateMonitoredTimeout(now, monitored);
    }
    
    @Override
    public void updateHeartbeatSample(String id, long lastHbTimestamp, 
            long interArrivalMean, long interArrivalStdDev) {
        Monitored monitored = getMonitored(id);
        
        List<Heartbeat> list = monitored.heartbeats;
        list.clear();
        
        addHeartbeat(new Heartbeat(interArrivalMean), list);
        updateMonitoredTimeout(lastHbTimestamp, monitored);
    }

    private void updateMonitoredTimeout(long now, Monitored monitored) {
        
        if (monitored.heartbeats.size() >= 2) {
            Double eA = calcEA(monitored, now);
            long t = eA.longValue() + alpha;
            monitored.timeout = t - now;
        }
        
        monitored.lastHeard = now;
    }

    private Double calcEA(Monitored monitored, long now) {
        double avgIA = 0;
        Iterator<Heartbeat> iterator = monitored.heartbeats.iterator();
        iterator.next();
        
        while (iterator.hasNext()) {
            avgIA += iterator.next().interarrival / (monitored.heartbeats.size() - 1);
        }
        
        return now + avgIA;
    }

    private Monitored getMonitored(String id) {
        Monitored monitored = monitoreds.get(id);
        if (monitored == null) {
            throw new IllegalArgumentException(
                    "The monitored must be registered first");
        }
        return monitored;
    }

    private void addHeartbeat(Heartbeat hb, List<Heartbeat> list) {
        if (list.size() == N) {
            list.remove(0);
        }
        list.add(hb);
    }

    @Override
    public void pingSent(String id, long now) {
        Monitored monitored = getMonitored(id);
        monitored.lastSent = now;
    }

    @Override
    public void appMessageReceived(String id, long now) {
        Monitored monitored = getMonitored(id);
        monitored.lastHeard = now;
    }

    @Override
    public void appMessageSent(String id, long now) {
        pingSent(id, now);
    }

    @Override
    public void release(String id) {
        monitoreds.remove(id);
    }

    @Override
    public void registerMonitored(String id, long now, long timeout) {
        Monitored monitored = monitoreds.get(id);
        if (monitored != null) {
            throw new IllegalArgumentException("Object with id " + id
                    + " is already monitored");
        }
        monitored = new Monitored(id);

        monitored.eta = timeout / 2;
        monitored.lastHeard = now;
        monitored.lastSent = now;

        monitored.staticTimeout = timeout;
        monitored.timeout = timeout;

        monitoreds.put(id, monitored);

    }

    @Override
    public void setTimeout(String id, long timeout) {
        Monitored monitored = getMonitored(id);
        monitored.staticTimeout = timeout;
        monitored.eta = timeout / 2;
    }

    private static class Monitored {

        private String id;
        List<Heartbeat> heartbeats = new LinkedList<Heartbeat>();
        long lastSent; //any msg sent
        long lastHeard; //any msg received

        long staticTimeout; //static timeout
        long timeout; //dynamic timeout

        long eta; //interrogation delay

        public Monitored(String id) {
            this.id = id;
        }
    }

    private static class Heartbeat {
        long interarrival;

        public Heartbeat(long interarrival) {
            this.interarrival = interarrival;
        }
    }

}
