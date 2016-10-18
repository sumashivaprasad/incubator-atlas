package org.apache.atlas.repository.store.graph;


import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public interface EntityReferenceResolver {

    /*
     * Return list of resolved and unresolved references.
     * Resolved references already exist in the ATLAS repository and have a unique GUID
     * Unresolved references have a temporary GUID
     */
    Pair<List<AtlasEntity>, List<AtlasEntity>> resolveReferences(AtlasEntity entity);

}
