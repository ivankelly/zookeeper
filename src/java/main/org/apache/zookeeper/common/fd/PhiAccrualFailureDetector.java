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

import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.NormalDistribution;
import org.apache.commons.math.distribution.NormalDistributionImpl;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

/**
 * Failure detector implementation according
 * to the Phi Accrual method, as described in the paper
 * 'The Phi Accrual Failure Detector', from Hayashibara. 
 * This method uses the Normal distribution to estimate a phi
 * value from a heartbeat arrival time sampling window. This
 * phi value indicates a suspicion level of a certain object.
 * If the phi value exceeds a threshold defined by the 
 * application, the monitored object is assumed as failed.
 */
public class PhiAccrualFailureDetector implements FailureDetector {

    protected static final int DEFAULT_MINWINDOWSIZE = 500;
    protected static final double DEFAULT_THRESHOLD = 2.;
    
    private static final int N = 1000;
    private Map<String, Monitored> monitoreds = new HashMap<String, Monitored>();

    private final double threshold;
    private final int minWindowSize;

    /**
     * Create a BertierFailureDetector with default threshold value (2.)
     * and default minWindowSize (500)
     */
    public PhiAccrualFailureDetector() {
        this(DEFAULT_THRESHOLD, DEFAULT_MINWINDOWSIZE);
    }

    /**
     * Create a BertierFailureDetector with specified threshold.
     * 
     * @param threshold
     *            for the phi value. When the phi value exceeds this threshold
     *            for a certain monitored, the failure detector considers this
     *            object as failed.
     * @param minWindowSize
     *            the sampling window minimum size for the failure detector to
     *            become active. This lower bound gives the failure detector a
     *            warm-up period.
     */
    public PhiAccrualFailureDetector(double threshold, int minWindowSize) {
        this.threshold = threshold;
        this.minWindowSize = minWindowSize;
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
        if (monitored.useStaticTimeout) {
            return monitored.staticTimeout;
        } else {
            return monitored.timeout;
        }
    }

    private void updateTimeout(Monitored monitored) {

        double mean = monitored.interArrivalMean;
        double sd = monitored.interArrivalStdDev;

        if (sd == 0) {
            monitored.useStaticTimeout = true;
            return;
        }
        
        NormalDistribution normal = new NormalDistributionImpl(mean, sd);
        try {
            monitored.timeout = (long) normal.inverseCumulativeProbability(1 - Math
                    .pow(10, -threshold));
            monitored.useStaticTimeout = false;
        } catch (MathException e) {
            // cumulative probability can not be computed due to
            // convergence or other numerical errors.
            monitored.useStaticTimeout = true;
        }

    }

    @Override
    public void heartbeatReceived(String id, long now) {
        Monitored monitored = getMonitored(id);
        
        monitored.heartbeats.addValue(now - monitored.lastHeard);

        if (monitored.heartbeats.getN() > minWindowSize) {
            monitored.updateSampleStats();
            updateTimeout(monitored);
        }
        
        monitored.lastHeard = now;
    }
    
    @Override
    public void updateHeartbeatSample(String id, long lastHbTimestamp,
            long interArrivalMean, long interArrivalStdDev) {
        
        Monitored monitored = getMonitored(id);
        monitored.lastHeard = lastHbTimestamp;
        
        monitored.heartbeats.clear();
        
        monitored.interArrivalMean = interArrivalMean;
        monitored.interArrivalStdDev = interArrivalStdDev;
        
        updateTimeout(monitored);
    }

    @Override
    public void pingSent(String id, long now) {
        Monitored monitored = getMonitored(id);
        monitored.lastSent = now;
    }

    @Override
    public void release(String id) {
        monitoreds.remove(id);
    }

    @Override
    public void setTimeout(String id, long timeout) {
        Monitored monitored = getMonitored(id);
        monitored.eta = timeout / 2;
        monitored.staticTimeout = timeout;
    }

    private Monitored getMonitored(String id) {
        Monitored monitored = monitoreds.get(id);
        if (monitored == null) {
            throw new IllegalArgumentException(
                    "The monitored must be registered first");
        }
        return monitored;
    }

    @Override
    public void registerMonitored(String id, long now, long timeout) {
        Monitored monitored = monitoreds.get(id);
        if (monitored != null) {
            throw new IllegalArgumentException("Object with id " + id
                    + " is already monitored");
        }
        monitored = new Monitored(id);
        monitored.staticTimeout = timeout;
        monitored.timeout = timeout;

        monitored.lastHeard = now;
        monitored.eta = timeout / 2;
        monitored.lastSent = now;

        monitoreds.put(id, monitored);
    }

    private static class Monitored {

        String id;
        DescriptiveStatistics heartbeats = new DescriptiveStatistics(N);
        
        long interArrivalMean;
        long interArrivalStdDev;
        
        boolean useStaticTimeout = true;
        long lastSent; 
        long lastHeard;

        long staticTimeout; //static timeout
        long timeout; //dynamic timeout


        long eta; //interval between pings

        public Monitored(String id) {
            this.id = id;
        }

        void updateSampleStats() {
            interArrivalMean = (long) heartbeats.getMean();
            interArrivalStdDev = (long) heartbeats.getStandardDeviation();
        }
    }

    @Override
    public void appMessageReceived(String id, long now) {
        Monitored monitored = getMonitored(id);
        monitored.lastHeard = now;
    }

    @Override
    public void appMessageSent(String id, long now) {
        Monitored monitored = getMonitored(id);
        monitored.lastSent = now;
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

import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.NormalDistribution;
import org.apache.commons.math.distribution.NormalDistributionImpl;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

/**
 * Failure detector implementation according
 * to the Phi Accrual method, as described in the paper
 * 'The Phi Accrual Failure Detector', from Hayashibara. 
 * This method uses the Normal distribution to estimate a phi
 * value from a heartbeat arrival time sampling window. This
 * phi value indicates a suspicion level of a certain object.
 * If the phi value exceeds a threshold defined by the 
 * application, the monitored object is assumed as failed.
 */
public class PhiAccrualFailureDetector implements FailureDetector {

    protected static final int DEFAULT_MINWINDOWSIZE = 500;
    protected static final double DEFAULT_THRESHOLD = 2.;
    
    private static final int N = 1000;
    private Map<String, Monitored> monitoreds = new HashMap<String, Monitored>();

    private final double threshold;
    private final int minWindowSize;

    /**
     * Create a BertierFailureDetector with default threshold value (2.)
     * and default minWindowSize (500)
     */
    public PhiAccrualFailureDetector() {
        this(DEFAULT_THRESHOLD, DEFAULT_MINWINDOWSIZE);
    }

    /**
     * Create a BertierFailureDetector with specified threshold.
     * 
     * @param threshold
     *            for the phi value. When the phi value exceeds this threshold
     *            for a certain monitored, the failure detector considers this
     *            object as failed.
     * @param minWindowSize
     *            the sampling window minimum size for the failure detector to
     *            become active. This lower bound gives the failure detector a
     *            warm-up period.
     */
    public PhiAccrualFailureDetector(double threshold, int minWindowSize) {
        this.threshold = threshold;
        this.minWindowSize = minWindowSize;
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
        if (monitored.useStaticTimeout) {
            return monitored.staticTimeout;
        } else {
            return monitored.timeout;
        }
    }

    private void updateTimeout(Monitored monitored) {

        double mean = monitored.interArrivalMean;
        double sd = monitored.interArrivalStdDev;

        if (sd == 0) {
            monitored.useStaticTimeout = true;
            return;
        }
        
        NormalDistribution normal = new NormalDistributionImpl(mean, sd);
        try {
            monitored.timeout = (long) normal.inverseCumulativeProbability(1 - Math
                    .pow(10, -threshold));
            monitored.useStaticTimeout = false;
        } catch (MathException e) {
            // cumulative probability can not be computed due to
            // convergence or other numerical errors.
            monitored.useStaticTimeout = true;
        }

    }

    @Override
    public void heartbeatReceived(String id, long now) {
        Monitored monitored = getMonitored(id);
        
        monitored.heartbeats.addValue(now - monitored.lastHeard);

        if (monitored.heartbeats.getN() > minWindowSize) {
            monitored.updateSampleStats();
            updateTimeout(monitored);
        }
        
        monitored.lastHeard = now;
    }
    
    @Override
    public void updateHeartbeatSample(String id, long lastHbTimestamp,
            long interArrivalMean, long interArrivalStdDev) {
        
        Monitored monitored = getMonitored(id);
        monitored.lastHeard = lastHbTimestamp;
        
        monitored.heartbeats.clear();
        
        monitored.interArrivalMean = interArrivalMean;
        monitored.interArrivalStdDev = interArrivalStdDev;
        
        updateTimeout(monitored);
    }

    @Override
    public void pingSent(String id, long now) {
        Monitored monitored = getMonitored(id);
        monitored.lastSent = now;
    }

    @Override
    public void release(String id) {
        monitoreds.remove(id);
    }

    @Override
    public void setTimeout(String id, long timeout) {
        Monitored monitored = getMonitored(id);
        monitored.eta = timeout / 2;
        monitored.staticTimeout = timeout;
    }

    private Monitored getMonitored(String id) {
        Monitored monitored = monitoreds.get(id);
        if (monitored == null) {
            throw new IllegalArgumentException(
                    "The monitored must be registered first");
        }
        return monitored;
    }

    @Override
    public void registerMonitored(String id, long now, long timeout) {
        Monitored monitored = monitoreds.get(id);
        if (monitored != null) {
            throw new IllegalArgumentException("Object with id " + id
                    + " is already monitored");
        }
        monitored = new Monitored(id);
        monitored.staticTimeout = timeout;
        monitored.timeout = timeout;

        monitored.lastHeard = now;
        monitored.eta = timeout / 2;
        monitored.lastSent = now;

        monitoreds.put(id, monitored);
    }

    private static class Monitored {

        String id;
        DescriptiveStatistics heartbeats = new DescriptiveStatistics(N);
        
        long interArrivalMean;
        long interArrivalStdDev;
        
        boolean useStaticTimeout = true;
        long lastSent; 
        long lastHeard;

        long staticTimeout; //static timeout
        long timeout; //dynamic timeout


        long eta; //interval between pings

        public Monitored(String id) {
            this.id = id;
        }

        void updateSampleStats() {
            interArrivalMean = (long) heartbeats.getMean();
            interArrivalStdDev = (long) heartbeats.getStandardDeviation();
        }
    }

    @Override
    public void appMessageReceived(String id, long now) {
        Monitored monitored = getMonitored(id);
        monitored.lastHeard = now;
    }

    @Override
    public void appMessageSent(String id, long now) {
        Monitored monitored = getMonitored(id);
        monitored.lastSent = now;
    }
    
}
