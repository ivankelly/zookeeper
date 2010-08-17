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

import java.util.Map;

/**
 * This class represents a factory for FailureDetector instances.
 */
public class FailureDetectorFactory {

    /**
     * Create a FailureDetector instance based on its name and its properties.
     * 
     * @param fdName
     *            the name of the failure detector
     * @param properties
     *            the options of the failure detector to be created.
     * @return the created FailureDetector instance
     */
    public FailureDetector createFd(String fdName,
            Map<String, String> properties) {
        if (fdName.equals("fixedhb")) {
            return new FixedHeartbeatFailureDetector();
        } else if (fdName.equals("chen")) {
            return createChenFD(properties);
        } else if (fdName.equals("bertier")) {
            return createBertierFD(properties);
        } else if (fdName.equals("phiaccrual")) {
            return createPhiAccrualFD(properties);
        } else if (fdName.equals("slicedhb")) {
            return createSlicedHeartbeatFD(properties);
        }

        return null;
    }

    private FailureDetector createChenFD(Map<String, String> properties) {
        Long alpha = parseLong(ChenFailureDetector.DEFAULT_ALPHA, 
                properties.get("alpha"));
        
        return new ChenFailureDetector(alpha);
    }

    private FailureDetector createSlicedHeartbeatFD(Map<String, String> properties) {
        Long slice = parseLong(
                SlicedHeartbeatFailureDetector.DEFAULT_SLICE_SIZE, 
                properties.get("slice"));
        
        return new SlicedHeartbeatFailureDetector(slice);
    }

    private FailureDetector createBertierFD(Map<String, String> properties) {
        Double gamma = parseDouble(BertierFailureDetector.DEFAULT_GAMMA,
                properties.get("gamma"));
        Double beta = parseDouble(BertierFailureDetector.DEFAULT_BETA,
                properties.get("beta"));
        Double phi = parseDouble(BertierFailureDetector.DEFAULT_PHI, properties
                .get("phi"));
        Long moderationstep = parseLong(
                BertierFailureDetector.DEFAULT_MODERATIONSTEP, 
                properties.get("moderationstep"));

        return new BertierFailureDetector(gamma, beta, phi, moderationstep);
    }

    private FailureDetector createPhiAccrualFD(Map<String, String> properties) {
        Double threshold = parseDouble(
                PhiAccrualFailureDetector.DEFAULT_THRESHOLD, 
                properties.get("threshold"));
        
        Integer minWindowSize = parseInt(
                PhiAccrualFailureDetector.DEFAULT_MINWINDOWSIZE, 
                properties.get("minwindowsize"));
        
        return new PhiAccrualFailureDetector(threshold, minWindowSize);
    }

    private static int parseInt(int defaultValue, String prop) {
        if (prop == null) {
            return defaultValue;
        }
        return Integer.valueOf(prop);
    }
    
    private static double parseDouble(double defaultValue, String prop) {
        if (prop == null) {
            return defaultValue;
        }
        return Double.valueOf(prop);
    }
    
    private static long parseLong(long defaultValue, String prop) {
        if (prop == null) {
            return defaultValue;
        }
        return Long.valueOf(prop);
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

import java.util.Map;

/**
 * This class represents a factory for FailureDetector instances.
 */
public class FailureDetectorFactory {

    /**
     * Create a FailureDetector instance based on its name and its properties.
     * 
     * @param fdName
     *            the name of the failure detector
     * @param properties
     *            the options of the failure detector to be created.
     * @return the created FailureDetector instance
     */
    public FailureDetector createFd(String fdName,
            Map<String, String> properties) {
        if (fdName.equals("fixedhb")) {
            return new FixedHeartbeatFailureDetector();
        } else if (fdName.equals("chen")) {
            return createChenFD(properties);
        } else if (fdName.equals("bertier")) {
            return createBertierFD(properties);
        } else if (fdName.equals("phiaccrual")) {
            return createPhiAccrualFD(properties);
        } else if (fdName.equals("slicedhb")) {
            return createSlicedHeartbeatFD(properties);
        }

        return null;
    }

    private FailureDetector createChenFD(Map<String, String> properties) {
        Long alpha = parseLong(ChenFailureDetector.DEFAULT_ALPHA, 
                properties.get("alpha"));
        
        return new ChenFailureDetector(alpha);
    }

    private FailureDetector createSlicedHeartbeatFD(Map<String, String> properties) {
        Long slice = parseLong(
                SlicedHeartbeatFailureDetector.DEFAULT_SLICE_SIZE, 
                properties.get("slice"));
        
        return new SlicedHeartbeatFailureDetector(slice);
    }

    private FailureDetector createBertierFD(Map<String, String> properties) {
        Double gamma = parseDouble(BertierFailureDetector.DEFAULT_GAMMA,
                properties.get("gamma"));
        Double beta = parseDouble(BertierFailureDetector.DEFAULT_BETA,
                properties.get("beta"));
        Double phi = parseDouble(BertierFailureDetector.DEFAULT_PHI, properties
                .get("phi"));
        Long moderationstep = parseLong(
                BertierFailureDetector.DEFAULT_MODERATIONSTEP, 
                properties.get("moderationstep"));

        return new BertierFailureDetector(gamma, beta, phi, moderationstep);
    }

    private FailureDetector createPhiAccrualFD(Map<String, String> properties) {
        Double threshold = parseDouble(
                PhiAccrualFailureDetector.DEFAULT_THRESHOLD, 
                properties.get("threshold"));
        
        Integer minWindowSize = parseInt(
                PhiAccrualFailureDetector.DEFAULT_MINWINDOWSIZE, 
                properties.get("minwindowsize"));
        
        return new PhiAccrualFailureDetector(threshold, minWindowSize);
    }

    private static int parseInt(int defaultValue, String prop) {
        if (prop == null) {
            return defaultValue;
        }
        return Integer.valueOf(prop);
    }
    
    private static double parseDouble(double defaultValue, String prop) {
        if (prop == null) {
            return defaultValue;
        }
        return Double.valueOf(prop);
    }
    
    private static long parseLong(long defaultValue, String prop) {
        if (prop == null) {
            return defaultValue;
        }
        return Long.valueOf(prop);
    }
}
