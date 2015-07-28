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

package org.apache.atlas.repository.graph.titan;

import com.google.inject.Provides;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.repository.graph.titan.solr.EmbeddedSolr;
import org.apache.atlas.repository.graph.titan.solr.EmbeddedSolrCloud;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;

/**
 * Default implementation for Graph Provider that doles out Titan Graph.
 */
public class TitanGraphProvider implements GraphProvider<TitanGraph> {

    private static final Logger LOG = LoggerFactory.getLogger(TitanGraphProvider.class);

    /**
     * Constant for the configuration property that indicates the prefix.
     */
    private static final String GRAPH_PREFIX = "atlas.graph";
    private static final String INDEX_BACKEND_PREFIX = "index.search.";
    private static final String INDEX_BACKEND_KEY = INDEX_BACKEND_PREFIX + "backend";
    private static final String INDEX_BACKEND_SOLR = "solr";
    private static final String SOLR_MODE_KEY = INDEX_BACKEND_PREFIX + INDEX_BACKEND_SOLR + ".mode";
    private static final String SOLR_ZK_URL_KEY = INDEX_BACKEND_PREFIX + INDEX_BACKEND_SOLR + ".zookeeper-url";
    private static final String SOLR_MODE_EMBEDDED = "embedded";
    private static final String SOLR_MODE_CLOUD = "cloud";

    private static TitanGraph graphInstance;

    private static Configuration getConfiguration() throws AtlasException {
        Configuration configProperties = ApplicationProperties.get();
        return ApplicationProperties.getSubsetConfiguration(configProperties, GRAPH_PREFIX);
    }

    @Override
    @Singleton
    @Provides
    public TitanGraph get() {
        if(graphInstance == null) {
            synchronized (TitanGraphProvider.class) {
                if(graphInstance == null) {
                    Configuration config;
                    try {
                        config = getConfiguration();
                    } catch (AtlasException e) {
                        throw new RuntimeException(e);
                    }

                    //Initialize solr
                    //Index backend is solr
                    if(INDEX_BACKEND_SOLR.equalsIgnoreCase(config.getString(INDEX_BACKEND_KEY))) {
                        //solr deployment mode is embedded
                        final String solrZKAddress = config.getString(SOLR_ZK_URL_KEY);
                        if(SOLR_MODE_EMBEDDED.equalsIgnoreCase(config.getString(SOLR_MODE_KEY))) {
                            LOG.info("Starting embedded solr server");
                            EmbeddedSolrCloud embeddedSolr = EmbeddedSolrCloud.get();
                            try {
                                embeddedSolr.start();
                            } catch (Exception e) {
                                throw new IllegalStateException("Could not initialize Embedded Solr. Aborting ", e);
                            }
                            //Reset the mode to cloud mode since EmbeddedSolr brings up an embedded ZK Server and comes up in pseudo-cloud mode.
                            config.setProperty(SOLR_MODE_KEY, SOLR_MODE_CLOUD);
                            //Set ZK Address to the embedded ZK address
                            config.setProperty(SOLR_ZK_URL_KEY, embeddedSolr.getZkAddress());
                            LOG.debug("Solr mode : " + config.getString(SOLR_MODE_KEY));
                        }
//                      else if(SOLR_MODE_CLOUD.equalsIgnoreCase(config.getString(SOLR_MODE_KEY))) {
//                      }
                    }
                    graphInstance = TitanFactory.open(config);
                }
            }
        }
        return graphInstance;
    }
}
