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

package org.apache.zookeeper.server.quorum;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.zookeeper.server.SessionTracker;
import org.apache.zookeeper.server.SessionTrackerImpl;

/**
 * This is really just a shell of a SessionTracker that tracks session activity
 * to be forwarded to the Leader using a PING.
 */
public class LearnerSessionTracker implements SessionTracker {
    
    private static final int N = 1000;
    
    SessionExpirer expirer;

    HashMap<Long, Integer> touchTable = new HashMap<Long, Integer>();
    long serverId = 1;
    long nextSessionId=0;
    
    private ConcurrentHashMap<Long, Session> sessionsWithTimeouts;

    public LearnerSessionTracker(SessionExpirer expirer,
            ConcurrentHashMap<Long, Integer> sessionsWithTimeouts, long id) {
        this.expirer = expirer;
        this.sessionsWithTimeouts = initializeSessions(sessionsWithTimeouts);
        this.serverId = id;
        nextSessionId = SessionTrackerImpl.initializeNextSession(this.serverId);
        
    }

    private static ConcurrentHashMap<Long, Session> initializeSessions(
            ConcurrentHashMap<Long, Integer> sessionsWithTimeouts) {
        
        ConcurrentHashMap<Long, Session> sessions = new ConcurrentHashMap<Long, Session>();
        
        for (Entry<Long, Integer> sessionEntry : sessionsWithTimeouts.entrySet()) {
            Session s = new Session();
            s.timeout = sessionEntry.getValue();
            s.lastHeard = System.currentTimeMillis();
            sessions.put(sessionEntry.getKey(), s);
        }
        
        return sessions;
    }

    synchronized public void removeSession(long sessionId) {
        sessionsWithTimeouts.remove(sessionId);
        touchTable.remove(sessionId);
    }

    public void shutdown() {
    }

    synchronized public void addSession(long sessionId, int sessionTimeout) {
        Session s = new Session();
        s.timeout = sessionTimeout;
        s.lastHeard = System.currentTimeMillis();
        
        sessionsWithTimeouts.put(sessionId, s);
        touchTable.put(sessionId, sessionTimeout);
    }

    synchronized public boolean touchSession(long sessionId, int sessionTimeout) {
        Session s = sessionsWithTimeouts.get(sessionId);
        if (s != null) {
            s.lastHeard = System.currentTimeMillis();
        }
        
        touchTable.put(sessionId, sessionTimeout);
        return true;
    }

    synchronized HashMap<Long, SessionInfo> snapshot() {
        HashMap<Long, SessionInfo> snapshot = new HashMap<Long, SessionInfo>();
        
        for (Entry<Long, Integer> toEntry : touchTable.entrySet()) {
            Session s = sessionsWithTimeouts.get(toEntry.getKey());
            SessionInfo sInfo = null;
            
            if (s != null) {
                s.updatedSample = false;
                sInfo = createSessionInfo(s);
            } else {
                sInfo = createSessionInfo(toEntry.getValue());
            }
            
            snapshot.put(toEntry.getKey(), sInfo);
        }
        touchTable = new HashMap<Long, Integer>();
        
        return snapshot;
    }

    private SessionInfo createSessionInfo(int timeout) {
        SessionInfo sInfo = new SessionInfo();
        sInfo.timeout = timeout;
        return sInfo;
    }
    
    private SessionInfo createSessionInfo(Session s) {
        SessionInfo sInfo = createSessionInfo(s.timeout);
        sInfo.interArrivalStdDev = (long) s.interArrivals
                .getStandardDeviation();
        sInfo.interArrivalMean = (long) s.interArrivals.getMean();
        return sInfo;
    }

    synchronized public long createSession(int sessionTimeout) {
        return (nextSessionId++);
    }

    public void checkSession(long sessionId, Object owner)  {
        // Nothing to do here. Sessions are checked at the Leader
    }
    
    public void setOwner(long sessionId, Object owner) {
        // Nothing to do here. Sessions are checked at the Leader
    }

    public void dumpSessions(PrintWriter pwriter) {
    	// the original class didn't have tostring impl, so just
    	// dup what we had before
    	pwriter.println(toString());
    }

    public boolean pingSession(long sessionId, int sessionTimeout) {
        Session s = sessionsWithTimeouts.get(sessionId);
        s.interArrivals.addValue(System.currentTimeMillis() - s.lastHeard);
        s.updatedSample = true;
        
        return touchSession(sessionId, sessionTimeout);
    }
    
    static class Session {
        DescriptiveStatistics interArrivals = new DescriptiveStatistics(N);
        long lastHeard;
        int timeout;
        boolean updatedSample = false;
    }
    
    static class SessionInfo {
        long interArrivalStdDev;
        long interArrivalMean;
        int timeout;
        boolean updatedSample = false;
    }
}
