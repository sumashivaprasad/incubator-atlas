package org.apache.atlas.repository.store.graph;

import org.apache.atlas.exception.AtlasBaseException;


public interface EntityResolver {

    void init(EntityGraphDiscoveryContext entities) throws AtlasBaseException;

    EntityGraphDiscoveryContext resolveEntityReferences() throws AtlasBaseException;

    void cleanUp() throws AtlasBaseException;
}
