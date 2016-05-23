package org.apache.atlas.repository.graph;

import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.types.ClassType;

public interface EntityDedupHandler {

    /**
     * Checks existence the specified instance identified by eithre primary key or a unique key
     * @param classType
     * @param instance
     * @return
     * @throws AtlasException
     */
    boolean exists(ClassType classType, IReferenceableInstance instance) throws AtlasException;

    /**
     * Returns the vertex associated with the specified instance and classType
     * @param classType
     * @param instance
     * @return null if nothing found else the vertex associated with the instance in the repository
     * @throws AtlasException
     */
    Vertex vertex(ClassType classType, IReferenceableInstance instance) throws AtlasException;

}
