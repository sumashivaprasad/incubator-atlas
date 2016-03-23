/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.fs.model;

import backtype.storm.ILocalCluster;
import backtype.storm.generated.StormTopology;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.fs.model.FSDataModel;
import org.apache.atlas.fs.model.FSDataTypes;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.commons.configuration.Configuration;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test
@Guice(modules = RepositoryMetadataModule.class)
public class HDFSModelTest {

    public static final Logger LOG = LoggerFactory.getLogger(HDFSModelTest.class);
    private static final String ATLAS_URL = "http://localhost:21000/";

    @Inject
    private MetadataService metadataService;

    @Inject
    private GraphProvider<TitanGraph> graphProvider;

    @BeforeClass
    public void setUp() throws Exception {
    }

    @AfterClass
    public void tearDown() throws Exception {
    }

    @Test
    public void testCreateDataModel() throws Exception {
        FSDataModel.main(new String[]{});
        TypesDef fsTypesDef = FSDataModel.typesDef();

        String fsTypesAsJSON = TypesSerialization.toJson(fsTypesDef);
        LOG.info("fsTypesAsJSON = {}", fsTypesAsJSON);

        metadataService.createType(fsTypesAsJSON);

        // verify types are registered
        for (FSDataTypes fsDataType : FSDataTypes.values()) {
            Assert.assertNotNull(atlasClient.getType(fsDataType.getName()));
        }
    }
}