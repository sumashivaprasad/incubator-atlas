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

package org.apache.atlas.examples;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageRelation;
import org.apache.atlas.web.resources.BaseResourceIT;
import org.codehaus.jettison.json.JSONException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

public class QuickStartV2IT extends BaseResourceIT {

    @BeforeClass
    public void runQuickStart() throws Exception {
        super.setUp();
        QuickStartV2.runQuickstart(new String[]{}, new String[]{"admin", "admin"});
    }

    @Test
    public void testDBIsAdded() throws Exception {
        AtlasEntity db = getDB(QuickStartV2.SALES_DB);
        Map<String, Object> dbAttributes = db.getAttributes();
        assertEquals(QuickStartV2.SALES_DB, dbAttributes.get("name"));
        assertEquals("sales database", dbAttributes.get("description"));
    }

    private AtlasEntity getDB(String dbName) throws AtlasServiceException, JSONException {
        AtlasEntity dbEntity = entitiesClientV2.getEntityByAttribute(QuickStartV2.DATABASE_TYPE, "name", dbName).get(0);
        return dbEntity;
    }

    @Test
    public void testTablesAreAdded() throws AtlasServiceException, JSONException {
        AtlasEntity table = getTable(QuickStart.SALES_FACT_TABLE);
        verifySimpleTableAttributes(table);

        verifyDBIsLinkedToTable(table);

        verifyColumnsAreAddedToTable(table);

        verifyTrait(table);
    }

    private AtlasEntity getTable(String tableName) throws AtlasServiceException {
        AtlasEntity tableEntity = entitiesClientV2.getEntityByAttribute(QuickStartV2.TABLE_TYPE, AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, tableName).get(0);
        return tableEntity;
    }

    private AtlasEntity getProcess(String processName) throws AtlasServiceException {
        AtlasEntity processEntity = entitiesClientV2.getEntityByAttribute(QuickStartV2.LOAD_PROCESS_TYPE, AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, processName).get(0);
        return processEntity;
    }
     

    private void verifyTrait(AtlasEntity table) throws AtlasServiceException {
        AtlasClassification.AtlasClassifications classfications = entitiesClientV2.getClassifications(table.getGuid());
        List<AtlasClassification> traits = classfications.getList();
        assertNotNull(traits.get(0).getTypeName());
    }

    private void verifyColumnsAreAddedToTable(AtlasEntity table) throws JSONException {
        Map<String, Object> tableAttributes = table.getAttributes();
        List<AtlasEntity> columns = (List<AtlasEntity>) tableAttributes.get("columns");
        assertEquals(4, columns.size());

        Map<String, Object> column = (Map) columns.get(0);
        Map<String, Object> columnAttributes = (Map) column.get("attributes");
        assertEquals(QuickStartV2.TIME_ID_COLUMN, columnAttributes.get("name"));
        assertEquals("int", columnAttributes.get("dataType"));
    }

    private void verifyDBIsLinkedToTable(AtlasEntity table) throws AtlasServiceException, JSONException {
        AtlasEntity db = getDB(QuickStartV2.SALES_DB);
        Map<String, Object> tableAttributes = table.getAttributes();
        Map dbFromTable = (Map) tableAttributes.get("db");
        assertEquals(db.getGuid(), dbFromTable.get("guid"));
    }

    private void verifySimpleTableAttributes(AtlasEntity table) throws JSONException {
        Map<String, Object> tableAttributes = table.getAttributes();
        assertEquals(QuickStartV2.SALES_FACT_TABLE, tableAttributes.get("name"));
        assertEquals("sales fact table", tableAttributes.get("description"));
    }

    @Test
    public void testProcessIsAdded() throws AtlasServiceException, JSONException {
        AtlasEntity loadProcess = entitiesClientV2.getEntityByAttribute(QuickStartV2.LOAD_PROCESS_TYPE, AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                QuickStartV2.LOAD_SALES_DAILY_PROCESS).get(0);

        Map loadProcessAttribs = loadProcess.getAttributes();
        assertEquals(QuickStartV2.LOAD_SALES_DAILY_PROCESS, loadProcessAttribs.get(AtlasClient.NAME));
        assertEquals("hive query for daily summary", loadProcessAttribs.get("description"));

        List inputs = (List) loadProcessAttribs.get("inputs");
        List outputs = (List) loadProcessAttribs.get("outputs");
        assertEquals(2, inputs.size());

        String salesFactTableId = getTableId(QuickStartV2.SALES_FACT_TABLE);
        String timeDimTableId = getTableId(QuickStartV2.TIME_DIM_TABLE);
        String salesFactDailyMVId = getTableId(QuickStartV2.SALES_FACT_DAILY_MV_TABLE);

        assertEquals(salesFactTableId, ((Map) inputs.get(0)).get("guid"));
        assertEquals(timeDimTableId, ((Map) inputs.get(1)).get("guid"));
        assertEquals(salesFactDailyMVId, ((Map) outputs.get(0)).get("guid"));
    }

    private String getTableId(String tableName) throws AtlasServiceException {
        return getTable(tableName).getGuid();
    }

    private String getProcessId(String processName) throws AtlasServiceException {
        return getProcess(processName).getGuid();
    }

    @Test
    public void testLineageIsMaintained() throws AtlasServiceException, JSONException {
        String salesFactTableId      = getTableId(QuickStartV2.SALES_FACT_TABLE);
        String timeDimTableId        = getTableId(QuickStartV2.TIME_DIM_TABLE);
        String salesFactDailyMVId    = getTableId(QuickStartV2.SALES_FACT_DAILY_MV_TABLE);
        String salesFactMonthlyMvId  = getTableId(QuickStartV2.SALES_FACT_MONTHLY_MV_TABLE);
        String salesDailyProcessId   = getProcessId(QuickStartV2.LOAD_SALES_DAILY_PROCESS);
        String salesMonthlyProcessId = getProcessId(QuickStartV2.LOAD_SALES_MONTHLY_PROCESS);

        AtlasLineageInfo inputLineage = lineageClientV2.getLineageInfo(salesFactDailyMVId, LineageDirection.BOTH, 0);
        List<LineageRelation> relations = new ArrayList<>(inputLineage.getRelations());
        Map<String, AtlasEntityHeader> entityMap = inputLineage.getGuidEntityMap();

        assertEquals(relations.size(), 5);
        assertEquals(entityMap.size(), 6);

        assertTrue(entityMap.containsKey(salesFactTableId));
        assertTrue(entityMap.containsKey(timeDimTableId));
        assertTrue(entityMap.containsKey(salesFactDailyMVId));
        assertTrue(entityMap.containsKey(salesDailyProcessId));
        assertTrue(entityMap.containsKey(salesFactMonthlyMvId));
        assertTrue(entityMap.containsKey(salesMonthlyProcessId));
    }

    @Test
    public void testViewIsAdded() throws AtlasServiceException, JSONException {
        AtlasEntity view = entitiesClientV2.getEntityByAttribute(QuickStartV2.VIEW_TYPE, AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, QuickStartV2.PRODUCT_DIM_VIEW).get(0);
        Map<String, Object> viewAttributes = view.getAttributes();
        assertEquals(QuickStartV2.PRODUCT_DIM_VIEW, viewAttributes.get(AtlasClient.NAME));

        String productDimId = getTable(QuickStartV2.PRODUCT_DIM_TABLE).getGuid();
        List inputTables = (List) viewAttributes.get("inputTables");
        Map inputTablesMap = (Map) inputTables.get(0);
        assertEquals(productDimId, inputTablesMap.get("guid"));
    }
}
