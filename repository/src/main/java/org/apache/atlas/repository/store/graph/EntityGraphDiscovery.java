package org.apache.atlas.repository.store.graph;


import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

public interface EntityGraphDiscovery {

    /*
     * Return list of resolved and unresolved references.
     * Resolved references already exist in the ATLAS repository and have an assigned unique GUID
     * Unresolved attribute references result in an error if they are not composite (managed by a parent entity)
     */
    DiscoveredEntities discoverEntities(List<AtlasEntity> entities) throws AtlasBaseException;

}
