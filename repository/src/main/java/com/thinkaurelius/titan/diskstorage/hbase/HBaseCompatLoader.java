/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.thinkaurelius.titan.diskstorage.hbase;

import org.apache.hadoop.hbase.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class HBaseCompatLoader {

    private static final Logger log = LoggerFactory
            .getLogger(HBaseCompatLoader.class);

//    public static final String TITAN_HBASE_COMPAT_CLASS_KEY = "TITAN_HBASE_COMPAT_CLASS";
//    private static final String TITAN_HBASE_COMPAT_CLASS;
//
//    static {
//
//        String s;
//
//        if (null != (s = System.getProperty(TITAN_HBASE_COMPAT_CLASS_KEY))) {
//            log.info("Read {} from system properties: {}", TITAN_HBASE_COMPAT_CLASS_KEY, s);
//        } else if (null != (s = System.getenv(TITAN_HBASE_COMPAT_CLASS_KEY))) {
//            log.info("Read {} from process environment: {}", TITAN_HBASE_COMPAT_CLASS_KEY, s);
//        } else {
//            log.debug("Could not read {} from system properties or process environment; using HBase VersionInfo to resolve compat layer", TITAN_HBASE_COMPAT_CLASS_KEY);
//        }
//
//        TITAN_HBASE_COMPAT_CLASS = s;
//    }

    private static HBaseCompat cachedCompat;

    public synchronized static HBaseCompat getCompat(String classOverride) {

        if (null != cachedCompat) {
            log.debug("Returning cached HBase compatibility layer: {}", cachedCompat);
            return cachedCompat;
        }

        HBaseCompat compat = null;
        String className = null;
        String classNameSource = null;

        if (null != classOverride) {
            className = classOverride;
            classNameSource = "from explicit configuration";
        } else {
            String hbaseVersion = VersionInfo.getVersion();
            for (String supportedVersion : Arrays.asList("0.94", "0.96", "0.98", "1.1")) {
                if (hbaseVersion.startsWith(supportedVersion + ".")) {
                    className = "com.thinkaurelius.titan.diskstorage.hbase.HBaseCompat" + supportedVersion.replaceAll("\\.", "_");
                    classNameSource = "supporting runtime HBase version " + hbaseVersion;
                    break;
                }
            }
            if (null == className) {
                throw new RuntimeException("Unrecognized or unsupported HBase version " + hbaseVersion);
            }
        }

        final String errTemplate = " when instantiating HBase compatibility class " + className;

        try {
            compat = (HBaseCompat)Class.forName(className).newInstance();
            log.info("Instantiated HBase compatibility layer {}: {}", classNameSource, compat.getClass().getCanonicalName());
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e.getClass().getSimpleName() + errTemplate, e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e.getClass().getSimpleName() + errTemplate, e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e.getClass().getSimpleName() + errTemplate, e);
        }

        return cachedCompat = compat;
    }
}
