package org.apache.atlas.repository.store.graph;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;

/**
 * Created by sshivaprasad on 10/20/16.
 */
public interface EntityResolver {

    DiscoveredEntities resolveEntityReferences(DiscoveredEntities entities) throws EntityNotFoundException, AtlasBaseException;
}
