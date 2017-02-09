package org.apache.atlas.repository.store.graph.v1;


import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.persistence.Id;

import javax.inject.Inject;
import java.util.List;

import static org.apache.atlas.TestUtils.COLUMNS_ATTR_NAME;
import static org.apache.atlas.TestUtils.NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class SoftDeleteHandlerV1Test extends AtlasDeleteHandlerV1Test {

    @Inject
    MetadataService metadataService;

    @Override
    DeleteHandlerV1 getDeleteHandler(final AtlasTypeRegistry typeRegistry) {
        return new SoftDeleteHandlerV1(typeRegistry);
    }

    @Override
    protected void assertDeletedColumn(final ITypedReferenceableInstance tableInstance) throws AtlasException {

    }

    @Override
    protected void assertTestDeleteEntities(final ITypedReferenceableInstance tableInstance) throws Exception {

    }

    @Override
    protected void assertTableForTestDeleteReference(final String tableId) throws Exception {

        //TODO - Fix after GET is ready
        ITypedReferenceableInstance table = metadataService.getEntityDefinition(tableId);
        assertNotNull(table.get(NAME));
        assertNotNull(table.get("description"));
        assertNotNull(table.get("type"));
        assertNotNull(table.get("tableType"));
        assertNotNull(table.get("created"));

        Id dbId = (Id) table.get("database");
        assertNotNull(dbId);

        ITypedReferenceableInstance db = metadataService.getEntityDefinition(dbId.getId()._getId());
        assertNotNull(db);
        assertEquals(db.getId().getState(), Id.EntityState.ACTIVE);

    }

    @Override
    protected void assertColumnForTestDeleteReference(final AtlasEntity tableInstance) throws AtlasException {

        List<AtlasObjectId> columns = (List<AtlasObjectId>) tableInstance.getAttribute(COLUMNS_ATTR_NAME);
        assertEquals(columns.size(), 1);

        //TODO - Enable after GET is ready
        ITypedReferenceableInstance colInst = metadataService.getEntityDefinition(columns.get(0).getGuid());
        assertEquals(colInst.getId().getState(), Id.EntityState.DELETED);
    }

    @Override
    protected void assertProcessForTestDeleteReference(final AtlasEntityHeader processInstance) throws Exception {
        //
        ITypedReferenceableInstance process = metadataService.getEntityDefinition(processInstance.getGuid());
        List<ITypedReferenceableInstance> outputs =
            (List<ITypedReferenceableInstance>) process.get(AtlasClient.PROCESS_ATTRIBUTE_OUTPUTS);
        List<ITypedReferenceableInstance> expectedOutputs =
            (List<ITypedReferenceableInstance>) process.get(AtlasClient.PROCESS_ATTRIBUTE_OUTPUTS);
        assertEquals(outputs.size(), expectedOutputs.size());

    }

    @Override
    protected void assertEntityDeleted(final String id) throws Exception {
        ITypedReferenceableInstance entity = metadataService.getEntityDefinition(id);
        assertEquals(entity.getId().getState(), Id.EntityState.DELETED);
    }
}
