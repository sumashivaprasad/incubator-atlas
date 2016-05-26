/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.graph;

import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.IInstance;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.IDataType;

/**
 * Provides methids to search for entities based on some unique constraint
 * @param <T>
 * @param <V>
 */
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
