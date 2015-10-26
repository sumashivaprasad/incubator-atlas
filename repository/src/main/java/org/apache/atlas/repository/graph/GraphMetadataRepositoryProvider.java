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
package org.apache.atlas.repository.graph;

import com.google.inject.Provides;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.MetadataRepository;

import javax.inject.Inject;
import javax.inject.Provider;


public class GraphMetadataRepositoryProvider implements Provider<MetadataRepository> {

    private final GraphProvider<TitanGraph> graphProvider;

    @Inject
    public GraphMetadataRepositoryProvider(GraphProvider<TitanGraph> graphProvider) throws AtlasException {
        this.graphProvider = graphProvider;
    }

    @Override
    @Provides
    public MetadataRepository get() {
        return new GraphBackedMetadataRepository(graphProvider);
    }
}
