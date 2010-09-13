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

/**
 * Interface for failure detectors that run in the same thread of the
 * application. A failure detector must be able to determine which monitored
 * objects have failed and the ones that must be pinged.
 * 
 */
public interface FailureDetector {

    /**
     * Signal the failure detector of an message reception.
     * @param id the monitored object identifier
     * @param now the timestamp in which the message was received
     * @param type the type of the received message
     */
    void messageReceived(String id, long now, MessageType type);

    /**
     * Signal the failure detector of a message dispatch
     * @param id the monitored object identifier
     * @param now the timestamp in which the message was sent
     * @param type the type of the received message
     */
    void messageSent(String id, long now, MessageType type);

    /**
     * Sets the timeout of a monitored object
     * @param id the monitored object identifier
     * @param timeout the timeout for the monitored object
     */
    void setTimeout(String id, long timeout);

    /**
     * Appends a ping sample data to this failure detector. 
     * In ZooKeeper, this is used when Learners report client pings
     * to the Leader.
     * @param id
     * @param lastPingTimestamp
     * @param interArrivalMean
     * @param interArrivalStdDev
     */
    void updatePingSample(String id, long lastPingTimestamp, long interArrivalMean,
            long interArrivalStdDev);
    
    /**
     * Registers an object to be monitored by this failure detector.
     * @param id the monitored object identifier, must be unique
     * @param now the timestamp in which the object 
     *          starts being monitored
     * @param timeout the timeout for the monitored object
     */
    void registerMonitored(String id, long now, long timeout);

    /**
     * Removes the interest on a monitored object
     * @param id the monitored object identifier
     */
    void releaseMonitored(String id);

    /**
     * Checks whether a monitored object is failed
     * @param now
     * @param id the monitored object identifier
     * @return true if the monitored object is failed, false otherwise
     */
    boolean isFailed(String id, long now);

    /**
     * Checks whether a monitored object must be pinged.
     * @param now
     * @return true if the monitored object should be pinged, false otherwise
     */
    boolean shouldPing(String id, long now);

    /**
     * Retrieves the interval between now and the last time a ping was
     * received for this monitored object.
     * @param id the monitored object identifier
     * @param now
     * @return the idle time for the monitored object
     */
    long getIdleTime(String id, long now);

    /**
     * Retrieves the remaining time to the sending time of next ping.
     * @param id the monitored object identifier
     * @param now
     * @return the time remaining to the next ping
     */
    long getTimeToNextPing(String id, long now);

    /**
     * Retrieves the timeout of a monitored object
     * @param id the monitored object identifier
     * @return the timeout of a monitored object
     */
    long getTimeout(String id);
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

/**
 * Interface for failure detectors that run in the same thread of the
 * application. A failure detector must be able to determine which monitored
 * objects have failed and the ones that must be pinged.
 * 
 */
public interface FailureDetector {

    /**
     * Signal the failure detector of an message reception.
     * @param id the monitored object identifier
     * @param now the timestamp in which the message was received
     * @param type the type of the received message
     */
    void messageReceived(String id, long now, MessageType type);

    /**
     * Signal the failure detector of a message dispatch
     * @param id the monitored object identifier
     * @param now the timestamp in which the message was sent
     * @param type the type of the received message
     */
    void messageSent(String id, long now, MessageType type);

    /**
     * Sets the timeout of a monitored object
     * @param id the monitored object identifier
     * @param timeout the timeout for the monitored object
     */
    void setTimeout(String id, long timeout);

    /**
     * Appends a ping sample data to this failure detector. 
     * In ZooKeeper, this is used when Learners report client pings
     * to the Leader.
     * @param id
     * @param lastPingTimestamp
     * @param interArrivalMean
     * @param interArrivalStdDev
     */
    void updatePingSample(String id, long lastPingTimestamp, long interArrivalMean,
            long interArrivalStdDev);
    
    /**
     * Registers an object to be monitored by this failure detector.
     * @param id the monitored object identifier, must be unique
     * @param now the timestamp in which the object 
     *          starts being monitored
     * @param timeout the timeout for the monitored object
     */
    void registerMonitored(String id, long now, long timeout);

    /**
     * Removes the interest on a monitored object
     * @param id the monitored object identifier
     */
    void releaseMonitored(String id);

    /**
     * Checks whether a monitored object is failed
     * @param now
     * @param id the monitored object identifier
     * @return true if the monitored object is failed, false otherwise
     */
    boolean isFailed(String id, long now);

    /**
     * Checks whether a monitored object must be pinged.
     * @param now
     * @return true if the monitored object should be pinged, false otherwise
     */
    boolean shouldPing(String id, long now);

    /**
     * Retrieves the interval between now and the last time a ping was
     * received for this monitored object.
     * @param id the monitored object identifier
     * @param now
     * @return the idle time for the monitored object
     */
    long getIdleTime(String id, long now);

    /**
     * Retrieves the remaining time to the sending time of next ping.
     * @param id the monitored object identifier
     * @param now
     * @return the time remaining to the next ping
     */
    long getTimeToNextPing(String id, long now);

    /**
     * Retrieves the timeout of a monitored object
     * @param id the monitored object identifier
     * @return the timeout of a monitored object
     */
    long getTimeout(String id);
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

/**
 * Interface for failure detectors that run in the same thread of the
 * application. A failure detector must be able to determine which monitored
 * objects have failed and the ones that must be pinged.
 * 
 */
public interface FailureDetector {

    /**
     * Signal the failure detector of an message reception.
     * @param id the monitored object identifier
     * @param now the timestamp in which the message was received
     * @param type the type of the received message
     */
    void messageReceived(String id, long now, MessageType type);

    /**
     * Signal the failure detector of a message dispatch
     * @param id the monitored object identifier
     * @param now the timestamp in which the message was sent
     * @param type the type of the received message
     */
    void messageSent(String id, long now, MessageType type);

    /**
     * Sets the timeout of a monitored object
     * @param id the monitored object identifier
     * @param timeout the timeout for the monitored object
     */
    void setTimeout(String id, long timeout);

    /**
     * Appends a ping sample data to this failure detector. 
     * In ZooKeeper, this is used when Learners report client pings
     * to the Leader.
     * @param id
     * @param lastPingTimestamp
     * @param interArrivalMean
     * @param interArrivalStdDev
     */
    void updatePingSample(String id, long lastPingTimestamp, long interArrivalMean,
            long interArrivalStdDev);
    
    /**
     * Registers an object to be monitored by this failure detector.
     * @param id the monitored object identifier, must be unique
     * @param now the timestamp in which the object 
     *          starts being monitored
     * @param timeout the timeout for the monitored object
     */
    void registerMonitored(String id, long now, long timeout);

    /**
     * Removes the interest on a monitored object
     * @param id the monitored object identifier
     */
    void releaseMonitored(String id);

    /**
     * Checks whether a monitored object is failed
     * @param now
     * @param id the monitored object identifier
     * @return true if the monitored object is failed, false otherwise
     */
    boolean isFailed(String id, long now);

    /**
     * Checks whether a monitored object must be pinged.
     * @param now
     * @return true if the monitored object should be pinged, false otherwise
     */
    boolean shouldPing(String id, long now);

    /**
     * Retrieves the interval between now and the last time a ping was
     * received for this monitored object.
     * @param id the monitored object identifier
     * @param now
     * @return the idle time for the monitored object
     */
    long getIdleTime(String id, long now);

    /**
     * Retrieves the remaining time to the sending time of next ping.
     * @param id the monitored object identifier
     * @param now
     * @return the time remaining to the next ping
     */
    long getTimeToNextPing(String id, long now);

    /**
     * Retrieves the timeout of a monitored object
     * @param id the monitored object identifier
     * @return the timeout of a monitored object
     */
    long getTimeout(String id);
}
