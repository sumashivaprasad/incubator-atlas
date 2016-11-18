package org.apache.atlas.repository.store.graph;

import org.apache.atlas.exception.AtlasBaseException;


public interface EntityResolver {

    EntityGraphDiscoveryContext resolveEntityReferences(EntityGraphDiscoveryContext entities) throws AtlasBaseException;
}
