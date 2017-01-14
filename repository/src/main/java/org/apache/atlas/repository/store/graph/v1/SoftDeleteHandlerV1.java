/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.store.graph.v1;

import com.google.inject.Inject;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.RequestContext;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.persistence.Id;

import static org.apache.atlas.repository.Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.MODIFIED_BY_KEY;
import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;

public class SoftDeleteHandlerV1 extends DeleteHandlerV1 {

    @Inject
    public SoftDeleteHandlerV1(AtlasTypeRegistry typeRegistry) {
        super(typeRegistry, false, true);
    }

    @Override
    protected void _deleteVertex(AtlasVertex instanceVertex, boolean force) {
        if (force) {
            graphHelper.removeVertex(instanceVertex);
        } else {
            AtlasEntity.Status state = AtlasGraphUtilsV1.getState(instanceVertex);
            if (state != AtlasEntity.Status.DELETED) {
                GraphHelper.setProperty(instanceVertex, STATE_PROPERTY_KEY, Id.EntityState.DELETED.name());
                GraphHelper.setProperty(instanceVertex, MODIFICATION_TIMESTAMP_PROPERTY_KEY,
                    RequestContext.get().getRequestTime());
                GraphHelper.setProperty(instanceVertex, MODIFIED_BY_KEY, RequestContext.get().getUser());
            }
        }
    }

    @Override
    protected void deleteEdge(AtlasEdge edge, boolean force) throws AtlasBaseException {
        if (force) {
            graphHelper.removeEdge(edge);
        } else {
            Id.EntityState state = GraphHelper.getState(edge);
            if (state != Id.EntityState.DELETED) {
                GraphHelper.setProperty(edge, STATE_PROPERTY_KEY, Id.EntityState.DELETED.name());
                GraphHelper
                    .setProperty(edge, MODIFICATION_TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
                GraphHelper.setProperty(edge, MODIFIED_BY_KEY, RequestContext.get().getUser());
            }
        }
    }
}

