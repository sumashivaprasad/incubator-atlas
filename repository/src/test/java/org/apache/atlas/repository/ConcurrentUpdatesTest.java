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
package org.apache.atlas.repository;

import com.google.inject.Inject;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.TestUtils;
import org.apache.atlas.repository.graph.GraphBackedMetadataRepository;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Test
@Guice(modules = RepositoryMetadataModule.class)
public class ConcurrentUpdatesTest {

    @javax.inject.Inject
    private GraphProvider<TitanGraph> graphProvider;

    private static final Logger log = LoggerFactory.getLogger(ConcurrentUpdatesTest.class);

    private static final String DATABASE_NAME = "foo";

    private TypeSystem typeSystem = TypeSystem.getInstance();

    private String dbGUID;

    @Inject
    private GraphBackedMetadataRepository repositoryService;

    @BeforeTest
    public void setUp() throws Exception {
        TestUtils.createHiveTypes(TypeSystem.getInstance());
        Referenceable databaseInstance = new Referenceable(TestUtils.DATABASE_TYPE);
        databaseInstance.set("name", DATABASE_NAME);
        databaseInstance.set("description", "foo database");

        ClassType dbType = typeSystem.getDataType(ClassType.class, TestUtils.DATABASE_TYPE);
        ITypedReferenceableInstance db = dbType.convert(databaseInstance, Multiplicity.REQUIRED);

        dbGUID = repositoryService.createEntities(db)[0];
    }

    @Test
    public void testUpdateEntity() throws Exception {
        final int threadPoolSize = 10;
        final ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);

        final AtomicInteger exceptionCount = new AtomicInteger(0);


        for (int i = 0; i < threadPoolSize; i++) {
            Runnable thread = new Runnable() {
                public void run() {
                    try {
                        final String value = String.valueOf(Thread.currentThread().getId()) + "-" + RandomStringUtils.randomAlphanumeric(10);
                        log.debug("Updating entity property description with {} ", value);
                        repositoryService.updateEntity(dbGUID, "description", value);
                        Thread.sleep(1000);
                    } catch (RepositoryException e) {
                        exceptionCount.incrementAndGet();
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            };


            log.debug("Exception count : " + exceptionCount.get());
            executorService.submit(thread);
        }
    }

    @AfterTest
    public void tearDown() throws Exception {
        TypeSystem.getInstance().reset();
        try {
            //TODO - Fix failure during shutdown while using BDB
            graphProvider.get().shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            TitanCleanup.clear(graphProvider.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
