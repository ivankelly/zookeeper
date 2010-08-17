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

package org.apache.zookeeper.server;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.common.fd.FailureDetector;
import org.apache.zookeeper.common.fd.SlicedHeartbeatFailureDetector;

/**
 * This is a full featured SessionTracker. It tracks sessions
 * using a specified failure detection method. By default, it uses
 * the SlicedHeartbeatFailureDetector, which expires sessions
 * in batches, grouped by their tick time.
 */
public class SessionTrackerImpl extends Thread implements SessionTracker {
    private static final Logger LOG = Logger.getLogger(SessionTrackerImpl.class);

    HashMap<Long, SessionImpl> sessionsById = new HashMap<Long, SessionImpl>();
    ConcurrentHashMap<Long, Integer> sessionsWithTimeout;
    
    long nextSessionId = 0;
    int expirationInterval;
    
    FailureDetector fd;

    public static class SessionImpl implements Session {
        SessionImpl(long sessionId, int timeout, long expireTime) {
            this.sessionId = sessionId;
            this.timeout = timeout;
            this.tickTime = expireTime;
        }

        final long sessionId;
        final int timeout;
        long tickTime;

        Object owner;

        public long getSessionId() { return sessionId; }
        public int getTimeout() { return timeout; }
    }

    public static long initializeNextSession(long id) {
        long nextSid = 0;
        nextSid = (System.currentTimeMillis() << 24) >> 8;
        nextSid =  nextSid | (id <<56);
        return nextSid;
    }

    static class SessionSet {
        HashSet<SessionImpl> sessions = new HashSet<SessionImpl>();
    }

    SessionExpirer expirer;


    public SessionTrackerImpl(SessionExpirer expirer,
            ConcurrentHashMap<Long, Integer> sessionsWithTimeout, int tickTime,
            long sid, FailureDetector fd)
    {
        super("SessionTracker");
        this.expirer = expirer;
        this.sessionsWithTimeout = sessionsWithTimeout;
        this.expirationInterval = tickTime;
        this.fd = (fd == null) ? new SlicedHeartbeatFailureDetector(tickTime)
                : fd;
        this.nextSessionId = initializeNextSession(sid);
        for (Entry<Long, Integer> e : sessionsWithTimeout.entrySet()) {
            addSession(e.getKey(), e.getValue());
        }
    }

    volatile boolean running = true;

    volatile long currentTime;

    synchronized public void dumpSessions(PrintWriter pwriter) {
        pwriter.println("Sessions:");
        
        for (SessionImpl s : sessionsById.values()) {
            pwriter.print("\tId: 0x");
            pwriter.print(Long.toHexString(s.sessionId));
            pwriter.print(" Timeout: ");
            pwriter.print(fd.getTimeout(String.valueOf(s.sessionId)));
            pwriter.print(" Idle time: ");
            pwriter.println(fd.getIdleTime(String.valueOf(s.sessionId), 
                    System.currentTimeMillis()));
        }
    }

    @Override
    synchronized public String toString() {
        StringWriter sw = new StringWriter();
        PrintWriter pwriter = new PrintWriter(sw);
        dumpSessions(pwriter);
        pwriter.flush();
        pwriter.close();
        return sw.toString();
    }

    @Override
    synchronized public void run() {
        try {
            while (running) {
                currentTime = System.currentTimeMillis();
                List<String> sessionIds = fd.getFailedObjects(currentTime);
                for (String id : sessionIds) {
                    fd.release(id);
                    SessionImpl s = sessionsById.remove(Long.valueOf(id));
                    expirer.expire(s);
                }
                this.wait(expirationInterval);
            }
        } catch (InterruptedException e) {
            LOG.error("Unexpected interruption", e);
        }
        LOG.info("SessionTrackerImpl exited loop!");
    }

    synchronized public void removeSession(long sessionId) {
        sessionsById.remove(sessionId);
        sessionsWithTimeout.remove(sessionId);
        fd.release(String.valueOf(sessionId));
        
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                    "SessionTrackerImpl --- Removing session 0x"
                    + Long.toHexString(sessionId));
        }
    }

    public void shutdown() {
        running = false;
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.getTextTraceLevel(),
                                     "Shutdown SessionTrackerImpl!");
        }
    }


    synchronized public long createSession(int sessionTimeout) {
        addSession(nextSessionId, sessionTimeout);
        return nextSessionId++;
    }

    synchronized public void addSession(long id, int sessionTimeout) {
        sessionsWithTimeout.put(id, sessionTimeout);
        if (sessionsById.get(id) == null) {
            SessionImpl s = new SessionImpl(id, sessionTimeout, 0);
            sessionsById.put(id, s);
            fd.registerMonitored(String.valueOf(id), 
                    System.currentTimeMillis(), sessionTimeout);
            
            if (LOG.isTraceEnabled()) {
                ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                        "SessionTrackerImpl --- Adding session 0x"
                        + Long.toHexString(id) + " " + sessionTimeout);
            }
        } else {
            if (LOG.isTraceEnabled()) {
                ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                        "SessionTrackerImpl --- Existing session 0x"
                        + Long.toHexString(id) + " " + sessionTimeout);
            }
        }
        touchSession(id, sessionTimeout);
    }

    synchronized public void checkSession(long sessionId, Object owner) throws KeeperException.SessionExpiredException, KeeperException.SessionMovedException {
        SessionImpl session = sessionsById.get(sessionId);
        if (session == null) {
            throw new KeeperException.SessionExpiredException();
        }
        if (session.owner == null) {
            session.owner = owner;
        } else if (session.owner != owner) {
            throw new KeeperException.SessionMovedException();
        }
    }

    synchronized public void setOwner(long id, Object owner) throws SessionExpiredException {
        SessionImpl session = sessionsById.get(id);
        if (session == null) {
            throw new KeeperException.SessionExpiredException();
        }
        session.owner = owner;
    }

    @Override
    synchronized public boolean pingSession(long sessionId, int sessionTimeout) {
        logSession(sessionId, sessionTimeout);
        if (!isSessionValid(sessionId)) {
            return false;
        }
        
        fd.heartbeatReceived(String.valueOf(sessionId), 
                System.currentTimeMillis());
        return true;
    }

    @Override
    synchronized public boolean touchSession(long sessionId, int sessionTimeout) {
        logSession(sessionId, sessionTimeout);
        if (!isSessionValid(sessionId)) {
            return false;
        }
        
        fd.appMessageReceived(String.valueOf(sessionId), 
                System.currentTimeMillis());
        return true;
    }
    
    synchronized public boolean updateHeartbeatSample(long sessionId, int sessionTimeout, 
            long interArrivalMean, long interArrivalStdDev) {
        
        logSession(sessionId, sessionTimeout);
        if (!isSessionValid(sessionId)) {
            return false;
        }
        
        fd.updateHeartbeatSample(String.valueOf(sessionId), 
                System.currentTimeMillis(), interArrivalMean, 
                interArrivalStdDev);
        return true;
    }

    private boolean isSessionValid(long sessionId) {
        SessionImpl s = sessionsById.get(sessionId);
        if (s == null) {
            return false;
        }
        return true;
    }

    private static void logSession(long sessionId, int sessionTimeout) {
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG,
                                     ZooTrace.CLIENT_PING_TRACE_MASK,
                                     "SessionTrackerImpl --- Touch session: 0x"
                    + Long.toHexString(sessionId) + " with timeout " + sessionTimeout);
        }
    }
}
