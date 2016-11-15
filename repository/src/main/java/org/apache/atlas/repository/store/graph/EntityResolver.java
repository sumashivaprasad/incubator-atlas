package org.apache.atlas.repository.store.graph;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;


public interface EntityResolver {

    DiscoveredEntities resolveEntityReferences(DiscoveredEntities entities) throws AtlasBaseException;
}
