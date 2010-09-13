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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Failure detector implementation according to Bertier method, 
 * as described in its paper 'Implementation and performance 
 * evaluation of an adaptable failure detector'. Bertier's method 
 * uses three parameters (gamma, beta and phi) to estimate a dynamic 
 * safety margin, which is added to the estimated next timeout.
 */
public class BertierFailureDetector implements FailureDetector {

    protected static final long DEFAULT_MODERATIONSTEP = 500;
    protected static final double DEFAULT_PHI = 4;
    protected static final double DEFAULT_BETA = 1;
    protected static final double DEFAULT_GAMMA = 0.1;

    /**
     * Sampling window size
     */
    private static final int N = 1000;

    // Failure detector params
    private final double gamma;
    private final double beta;
    private final double phi;
    private final long moderationStep;

    private Map<String, Monitored> monitoreds = new HashMap<String, Monitored>();


    /**
     * Create a BertierFailureDetector with default parameters values: gamma =
     * 0.1, beta = 1.0, phi = 4.0, moderationStep = 500.
     */
    public BertierFailureDetector() {
        this(DEFAULT_GAMMA, DEFAULT_BETA, DEFAULT_PHI, DEFAULT_MODERATIONSTEP);
    }

    /**
     * Create a BertierFailureDetector with specified gamma, beta and phi
     * parameters.
     * 
     * @param gamma
     *            Represents the importance of the last calculated error on the
     *            estimation of the safety margin.
     * @param beta
     *            Represents the importance of the calculated delay on the
     *            estimation of the safety margin.
     * @param phi
     *            Permits to ponder the variance on the estimation of the safety
     *            margin.
     * @param moderationStep
     *            The step to be added to timeout when a false suspicion is
     *            detected.
     */
    public BertierFailureDetector(double gamma, double beta, double phi, long moderationStep) {
        this.gamma = gamma;
        this.beta = beta;
        this.phi = phi;
        this.moderationStep = moderationStep;
    }
    
    /**
     * Create a BertierFailureDetector from a parameters map
     * @param parameters
     */
    public BertierFailureDetector(Map<String, String> parameters) {

        this(
                FailureDetectorOptParser.parseDouble(
                        BertierFailureDetector.DEFAULT_GAMMA, 
                        parameters.get("gamma")),
                FailureDetectorOptParser.parseDouble(
                        BertierFailureDetector.DEFAULT_BETA, 
                        parameters.get("beta")), 
                FailureDetectorOptParser.parseDouble(
                        BertierFailureDetector.DEFAULT_PHI,
                        parameters.get("phi")),
                FailureDetectorOptParser.parseLong(
                        BertierFailureDetector.DEFAULT_MODERATIONSTEP,
                        parameters.get("moderationstep")));
    }

    @Override
    public long getIdleTime(String id, long now) {
        Monitored monitored = getMonitored(id);
        return now - monitored.lastHeard;
    }

    @Override
    public boolean isFailed(String id, long now) {
        Monitored monitored = monitoreds.get(id);
        if (monitored == null) {
            return false;
        }
        
        if (now > monitored.lastHeard + getTimeout(monitored.id)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean shouldPing(String id, long now) {
        if (!monitoreds.containsKey(id)) {
            return false;
        }
        if (getTimeToNextPing(id, now) <= 0) {
            return true;
        }
        return false;
    }

    @Override
    public long getTimeToNextPing(String id, long now) {
        Monitored monitored = getMonitored(id);
        return (monitored.lastSent + monitored.eta) - now;
    }

    @Override
    public long getTimeout(String id) {
        Monitored monitored = getMonitored(id);
        if (monitored.pings.size() < 2) {
            return monitored.staticTimeout;
        }

        return monitored.timeout;
    }

    @Override
    public void updatePingSample(String id, long lastHbTimestamp, 
            long interArrivalMean, long interArrivalStdDev) {
        Monitored monitored = getMonitored(id);
        
        List<Ping> list = monitored.pings;
        
        list.clear();
        addPing(new Ping(interArrivalMean), list);
        
        updateMonitoredTimeout(lastHbTimestamp, monitored, false);
        monitored.lastHeard = lastHbTimestamp;
    }
    
    private void updateMonitoredTimeout(long now, Monitored monitored, boolean failed) {
        if (monitored.pings.size() >= 2) {
            
            monitored.error = (double) (now - monitored.eA - monitored.delay);
            monitored.delay = monitored.delay
                    + Math.round(gamma * monitored.error);
            monitored.var = monitored.var + gamma
                    * (Math.abs(monitored.error) - monitored.var);
            monitored.alpha = beta * ((double) monitored.delay) + phi
                    * monitored.var;
            
            monitored.eA = Math.round(calcEA(monitored, now));
            long t = monitored.eA + Math.round(monitored.alpha);
            
            if (failed) {
                monitored.deltaP += moderationStep;
            }
            
            monitored.timeout = t - now + monitored.deltaP;
        }
    }
    
    @Override
    public void messageReceived(String id, long now, MessageType type) {
        Monitored monitored = getMonitored(id);
        
        if (MessageType.PING.equals(type)) {
            boolean failed = now > monitored.lastHeard + getTimeout(monitored.id);
            addPing(new Ping(now - monitored.lastHeard),
                    monitored.pings);
            
            updateMonitoredTimeout(now, monitored, failed);
        }
        
        monitored.lastHeard = now;
    }

    @Override
    public void messageSent(String id, long now, MessageType type) {
        Monitored monitored = getMonitored(id);
        monitored.lastSent = now;
    }

    private Double calcEA(Monitored monitored, long now) {
        double avgIA = 0;
        Iterator<Ping> iterator = monitored.pings.iterator();
        iterator.next();
        
        while (iterator.hasNext()) {
            avgIA += iterator.next().interarrival / (monitored.pings.size() - 1);
        }
        
        return now + avgIA;
    }

    private void addPing(Ping hb, List<Ping> list) {
        if (list.size() == N) {
            list.remove(0);
        }
        list.add(hb);
    }

    @Override
    public void releaseMonitored(String id) {
        monitoreds.remove(id);
    }

    @Override
    public void setTimeout(String id, long timeout) {
        Monitored monitored = getMonitored(id);
        monitored.staticTimeout = timeout;
        monitored.eta = timeout / 2;
    }

    @Override
    public void registerMonitored(String id, long now, long timeout) {
        Monitored monitored = monitoreds.get(id);
        if (monitored != null) {
            throw new IllegalArgumentException("Object with id " + id
                    + " is already monitored");
        }
        monitored = new Monitored(id);
        monitored.lastSent = now;
        monitored.lastHeard = now;

        monitored.eta = timeout / 2;
        monitored.delay = timeout / 4;
        monitored.staticTimeout = timeout;
        monitored.timeout = timeout;
        
        //initialize with estimated arrival
        monitored.eA = now + timeout;

        monitoreds.put(id, monitored);
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

        List<Ping> pings = new LinkedList<Ping>();

        long lastSent; //any msg sent
        long lastHeard; //any msg received

        long eA; //estimated arrival

        long deltaP; //moderation parameter
        
        double alpha; //calculated safety margin
        double var; //magnitude between errors
        double error; //error of the last estimation

        long delay; //estimate margin
        long eta; //interrogation delay

        long timeout; //dynamic timeout
        long staticTimeout; //static timeout

        public Monitored(String id) {
            this.id = id;
        }
    }

    private static class Ping {
        long interarrival;

        public Ping(long interarrival) {
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Failure detector implementation according to Bertier method, 
 * as described in its paper 'Implementation and performance 
 * evaluation of an adaptable failure detector'. Bertier's method 
 * uses three parameters (gamma, beta and phi) to estimate a dynamic 
 * safety margin, which is added to the estimated next timeout.
 */
public class BertierFailureDetector implements FailureDetector {

    protected static final long DEFAULT_MODERATIONSTEP = 500;
    protected static final double DEFAULT_PHI = 4;
    protected static final double DEFAULT_BETA = 1;
    protected static final double DEFAULT_GAMMA = 0.1;

    /**
     * Sampling window size
     */
    private static final int N = 1000;

    // Failure detector params
    private final double gamma;
    private final double beta;
    private final double phi;
    private final long moderationStep;

    private Map<String, Monitored> monitoreds = new HashMap<String, Monitored>();


    /**
     * Create a BertierFailureDetector with default parameters values: gamma =
     * 0.1, beta = 1.0, phi = 4.0, moderationStep = 500.
     */
    public BertierFailureDetector() {
        this(DEFAULT_GAMMA, DEFAULT_BETA, DEFAULT_PHI, DEFAULT_MODERATIONSTEP);
    }

    /**
     * Create a BertierFailureDetector with specified gamma, beta and phi
     * parameters.
     * 
     * @param gamma
     *            Represents the importance of the last calculated error on the
     *            estimation of the safety margin.
     * @param beta
     *            Represents the importance of the calculated delay on the
     *            estimation of the safety margin.
     * @param phi
     *            Permits to ponder the variance on the estimation of the safety
     *            margin.
     * @param moderationStep
     *            The step to be added to timeout when a false suspicion is
     *            detected.
     */
    public BertierFailureDetector(double gamma, double beta, double phi, long moderationStep) {
        this.gamma = gamma;
        this.beta = beta;
        this.phi = phi;
        this.moderationStep = moderationStep;
    }
    
    /**
     * Create a BertierFailureDetector from a parameters map
     * @param parameters
     */
    public BertierFailureDetector(Map<String, String> parameters) {

        this(
                FailureDetectorOptParser.parseDouble(
                        BertierFailureDetector.DEFAULT_GAMMA, 
                        parameters.get("gamma")),
                FailureDetectorOptParser.parseDouble(
                        BertierFailureDetector.DEFAULT_BETA, 
                        parameters.get("beta")), 
                FailureDetectorOptParser.parseDouble(
                        BertierFailureDetector.DEFAULT_PHI,
                        parameters.get("phi")),
                FailureDetectorOptParser.parseLong(
                        BertierFailureDetector.DEFAULT_MODERATIONSTEP,
                        parameters.get("moderationstep")));
    }

    @Override
    public long getIdleTime(String id, long now) {
        Monitored monitored = getMonitored(id);
        return now - monitored.lastHeard;
    }

    @Override
    public boolean isFailed(String id, long now) {
        Monitored monitored = monitoreds.get(id);
        if (monitored == null) {
            return false;
        }
        
        if (now > monitored.lastHeard + getTimeout(monitored.id)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean shouldPing(String id, long now) {
        if (!monitoreds.containsKey(id)) {
            return false;
        }
        if (getTimeToNextPing(id, now) <= 0) {
            return true;
        }
        return false;
    }

    @Override
    public long getTimeToNextPing(String id, long now) {
        Monitored monitored = getMonitored(id);
        return (monitored.lastSent + monitored.eta) - now;
    }

    @Override
    public long getTimeout(String id) {
        Monitored monitored = getMonitored(id);
        if (monitored.pings.size() < 2) {
            return monitored.staticTimeout;
        }

        return monitored.timeout;
    }

    @Override
    public void updatePingSample(String id, long lastHbTimestamp, 
            long interArrivalMean, long interArrivalStdDev) {
        Monitored monitored = getMonitored(id);
        
        List<Ping> list = monitored.pings;
        
        list.clear();
        addPing(new Ping(interArrivalMean), list);
        
        updateMonitoredTimeout(lastHbTimestamp, monitored, false);
        monitored.lastHeard = lastHbTimestamp;
    }
    
    private void updateMonitoredTimeout(long now, Monitored monitored, boolean failed) {
        if (monitored.pings.size() >= 2) {
            
            monitored.error = (double) (now - monitored.eA - monitored.delay);
            monitored.delay = monitored.delay
                    + Math.round(gamma * monitored.error);
            monitored.var = monitored.var + gamma
                    * (Math.abs(monitored.error) - monitored.var);
            monitored.alpha = beta * ((double) monitored.delay) + phi
                    * monitored.var;
            
            monitored.eA = Math.round(calcEA(monitored, now));
            long t = monitored.eA + Math.round(monitored.alpha);
            
            if (failed) {
                monitored.deltaP += moderationStep;
            }
            
            monitored.timeout = t - now + monitored.deltaP;
        }
    }
    
    @Override
    public void messageReceived(String id, long now, MessageType type) {
        Monitored monitored = getMonitored(id);
        
        if (MessageType.PING.equals(type)) {
            boolean failed = now > monitored.lastHeard + getTimeout(monitored.id);
            addPing(new Ping(now - monitored.lastHeard),
                    monitored.pings);
            
            updateMonitoredTimeout(now, monitored, failed);
        }
        
        monitored.lastHeard = now;
    }

    @Override
    public void messageSent(String id, long now, MessageType type) {
        Monitored monitored = getMonitored(id);
        monitored.lastSent = now;
    }

    private Double calcEA(Monitored monitored, long now) {
        double avgIA = 0;
        Iterator<Ping> iterator = monitored.pings.iterator();
        iterator.next();
        
        while (iterator.hasNext()) {
            avgIA += iterator.next().interarrival / (monitored.pings.size() - 1);
        }
        
        return now + avgIA;
    }

    private void addPing(Ping hb, List<Ping> list) {
        if (list.size() == N) {
            list.remove(0);
        }
        list.add(hb);
    }

    @Override
    public void releaseMonitored(String id) {
        monitoreds.remove(id);
    }

    @Override
    public void setTimeout(String id, long timeout) {
        Monitored monitored = getMonitored(id);
        monitored.staticTimeout = timeout;
        monitored.eta = timeout / 2;
    }

    @Override
    public void registerMonitored(String id, long now, long timeout) {
        Monitored monitored = monitoreds.get(id);
        if (monitored != null) {
            throw new IllegalArgumentException("Object with id " + id
                    + " is already monitored");
        }
        monitored = new Monitored(id);
        monitored.lastSent = now;
        monitored.lastHeard = now;

        monitored.eta = timeout / 2;
        monitored.delay = timeout / 4;
        monitored.staticTimeout = timeout;
        monitored.timeout = timeout;
        
        //initialize with estimated arrival
        monitored.eA = now + timeout;

        monitoreds.put(id, monitored);
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

        List<Ping> pings = new LinkedList<Ping>();

        long lastSent; //any msg sent
        long lastHeard; //any msg received

        long eA; //estimated arrival

        long deltaP; //moderation parameter
        
        double alpha; //calculated safety margin
        double var; //magnitude between errors
        double error; //error of the last estimation

        long delay; //estimate margin
        long eta; //interrogation delay

        long timeout; //dynamic timeout
        long staticTimeout; //static timeout

        public Monitored(String id) {
            this.id = id;
        }
    }

    private static class Ping {
        long interarrival;

        public Ping(long interarrival) {
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Failure detector implementation according to Bertier method, 
 * as described in its paper 'Implementation and performance 
 * evaluation of an adaptable failure detector'. Bertier's method 
 * uses three parameters (gamma, beta and phi) to estimate a dynamic 
 * safety margin, which is added to the estimated next timeout.
 */
public class BertierFailureDetector implements FailureDetector {

    protected static final long DEFAULT_MODERATIONSTEP = 500;
    protected static final double DEFAULT_PHI = 4;
    protected static final double DEFAULT_BETA = 1;
    protected static final double DEFAULT_GAMMA = 0.1;

    /**
     * Sampling window size
     */
    private static final int N = 1000;

    // Failure detector params
    private final double gamma;
    private final double beta;
    private final double phi;
    private final long moderationStep;

    private Map<String, Monitored> monitoreds = new HashMap<String, Monitored>();


    /**
     * Create a BertierFailureDetector with default parameters values: gamma =
     * 0.1, beta = 1.0, phi = 4.0, moderationStep = 500.
     */
    public BertierFailureDetector() {
        this(DEFAULT_GAMMA, DEFAULT_BETA, DEFAULT_PHI, DEFAULT_MODERATIONSTEP);
    }

    /**
     * Create a BertierFailureDetector with specified gamma, beta and phi
     * parameters.
     * 
     * @param gamma
     *            Represents the importance of the last calculated error on the
     *            estimation of the safety margin.
     * @param beta
     *            Represents the importance of the calculated delay on the
     *            estimation of the safety margin.
     * @param phi
     *            Permits to ponder the variance on the estimation of the safety
     *            margin.
     * @param moderationStep
     *            The step to be added to timeout when a false suspicion is
     *            detected.
     */
    public BertierFailureDetector(double gamma, double beta, double phi, long moderationStep) {
        this.gamma = gamma;
        this.beta = beta;
        this.phi = phi;
        this.moderationStep = moderationStep;
    }
    
    /**
     * Create a BertierFailureDetector from a parameters map
     * @param parameters
     */
    public BertierFailureDetector(Map<String, String> parameters) {

        this(
                FailureDetectorOptParser.parseDouble(
                        BertierFailureDetector.DEFAULT_GAMMA, 
                        parameters.get("gamma")),
                FailureDetectorOptParser.parseDouble(
                        BertierFailureDetector.DEFAULT_BETA, 
                        parameters.get("beta")), 
                FailureDetectorOptParser.parseDouble(
                        BertierFailureDetector.DEFAULT_PHI,
                        parameters.get("phi")),
                FailureDetectorOptParser.parseLong(
                        BertierFailureDetector.DEFAULT_MODERATIONSTEP,
                        parameters.get("moderationstep")));
    }

    @Override
    public long getIdleTime(String id, long now) {
        Monitored monitored = getMonitored(id);
        return now - monitored.lastHeard;
    }

    @Override
    public boolean isFailed(String id, long now) {
        Monitored monitored = monitoreds.get(id);
        if (monitored == null) {
            return false;
        }
        
        if (now > monitored.lastHeard + getTimeout(monitored.id)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean shouldPing(String id, long now) {
        if (!monitoreds.containsKey(id)) {
            return false;
        }
        if (getTimeToNextPing(id, now) <= 0) {
            return true;
        }
        return false;
    }

    @Override
    public long getTimeToNextPing(String id, long now) {
        Monitored monitored = getMonitored(id);
        return (monitored.lastSent + monitored.eta) - now;
    }

    @Override
    public long getTimeout(String id) {
        Monitored monitored = getMonitored(id);
        if (monitored.pings.size() < 2) {
            return monitored.staticTimeout;
        }

        return monitored.timeout;
    }

    @Override
    public void updatePingSample(String id, long lastHbTimestamp, 
            long interArrivalMean, long interArrivalStdDev) {
        Monitored monitored = getMonitored(id);
        
        List<Ping> list = monitored.pings;
        
        list.clear();
        addPing(new Ping(interArrivalMean), list);
        
        updateMonitoredTimeout(lastHbTimestamp, monitored, false);
        monitored.lastHeard = lastHbTimestamp;
    }
    
    private void updateMonitoredTimeout(long now, Monitored monitored, boolean failed) {
        if (monitored.pings.size() >= 2) {
            
            monitored.error = (double) (now - monitored.eA - monitored.delay);
            monitored.delay = monitored.delay
                    + Math.round(gamma * monitored.error);
            monitored.var = monitored.var + gamma
                    * (Math.abs(monitored.error) - monitored.var);
            monitored.alpha = beta * ((double) monitored.delay) + phi
                    * monitored.var;
            
            monitored.eA = Math.round(calcEA(monitored, now));
            long t = monitored.eA + Math.round(monitored.alpha);
            
            if (failed) {
                monitored.deltaP += moderationStep;
            }
            
            monitored.timeout = t - now + monitored.deltaP;
        }
    }
    
    @Override
    public void messageReceived(String id, long now, MessageType type) {
        Monitored monitored = getMonitored(id);
        
        if (MessageType.PING.equals(type)) {
            boolean failed = now > monitored.lastHeard + getTimeout(monitored.id);
            addPing(new Ping(now - monitored.lastHeard),
                    monitored.pings);
            
            updateMonitoredTimeout(now, monitored, failed);
        }
        
        monitored.lastHeard = now;
    }

    @Override
    public void messageSent(String id, long now, MessageType type) {
        Monitored monitored = getMonitored(id);
        monitored.lastSent = now;
    }

    private Double calcEA(Monitored monitored, long now) {
        double avgIA = 0;
        Iterator<Ping> iterator = monitored.pings.iterator();
        iterator.next();
        
        while (iterator.hasNext()) {
            avgIA += iterator.next().interarrival / (monitored.pings.size() - 1);
        }
        
        return now + avgIA;
    }

    private void addPing(Ping hb, List<Ping> list) {
        if (list.size() == N) {
            list.remove(0);
        }
        list.add(hb);
    }

    @Override
    public void releaseMonitored(String id) {
        monitoreds.remove(id);
    }

    @Override
    public void setTimeout(String id, long timeout) {
        Monitored monitored = getMonitored(id);
        monitored.staticTimeout = timeout;
        monitored.eta = timeout / 2;
    }

    @Override
    public void registerMonitored(String id, long now, long timeout) {
        Monitored monitored = monitoreds.get(id);
        if (monitored != null) {
            throw new IllegalArgumentException("Object with id " + id
                    + " is already monitored");
        }
        monitored = new Monitored(id);
        monitored.lastSent = now;
        monitored.lastHeard = now;

        monitored.eta = timeout / 2;
        monitored.delay = timeout / 4;
        monitored.staticTimeout = timeout;
        monitored.timeout = timeout;
        
        //initialize with estimated arrival
        monitored.eA = now + timeout;

        monitoreds.put(id, monitored);
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

        List<Ping> pings = new LinkedList<Ping>();

        long lastSent; //any msg sent
        long lastHeard; //any msg received

        long eA; //estimated arrival

        long deltaP; //moderation parameter
        
        double alpha; //calculated safety margin
        double var; //magnitude between errors
        double error; //error of the last estimation

        long delay; //estimate margin
        long eta; //interrogation delay

        long timeout; //dynamic timeout
        long staticTimeout; //static timeout

        public Monitored(String id) {
            this.id = id;
        }
    }

    private static class Ping {
        long interarrival;

        public Ping(long interarrival) {
            this.interarrival = interarrival;
        }
    }

}
