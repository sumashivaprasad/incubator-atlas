package org.apache.atlas.repository.graph;

import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.IInstance;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.IDataType;

public interface DedupHandler<T extends IDataType, V extends IInstance> {

    /**
     * Checks existence the specified instance identified by eithre primary key or a unique key
     * @param instance
     * @return
     * @throws AtlasException
     */
    boolean exists(T type, V instance) throws AtlasException;

    /**
     * Returns the vertex associated with the specified instance and classType
     * @param instance
     * @return null if nothing found else the vertex associated with the instance in the repository
     * @throws AtlasException
     */
    Vertex vertex(T Type, V instance) throws AtlasException;

}
