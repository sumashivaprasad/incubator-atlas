package org.apache.atlas.repository.store.graph;


import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;

import java.util.List;

public interface EntityGraphDiscovery {


    void init() throws AtlasBaseException;

    /*
     * Return list of resolved and unresolved references.
     * Resolved references already exist in the ATLAS repository and have an assigned unique GUID
     * Unresolved attribute references result in an error if they are not composite (managed by a parent entity)
     */
    EntityGraphDiscoveryContext discoverEntities(List<AtlasEntity> entities) throws AtlasBaseException;

    void cleanUp() throws AtlasBaseException;
}
