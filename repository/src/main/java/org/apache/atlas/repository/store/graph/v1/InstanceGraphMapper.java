package org.apache.atlas.repository.store.graph.v1;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;

public interface InstanceGraphMapper<T> {

    /**
     * Map the given type instance to the graph
     *
     * @param ctx
     * @return the value that was mapped to the vertex
     * @throws AtlasBaseException
     */
    T toGraph(GraphMutationContext ctx) throws AtlasBaseException;

}
