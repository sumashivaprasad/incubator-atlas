/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.rest;

import com.google.inject.Inject;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef.AtlasClassificationDefs;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEntityDef.AtlasEntityDefs;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasEnumDef.AtlasEnumDefs;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasStructDefs;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.store.graph.AtlasEntityDefStore;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.store.AtlasentitiesStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.web.util.Servlets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Set;


@Path("v2/entities")
@Singleton
public class EntitiesREST {
    private static final Logger LOG = LoggerFactory.getLogger(EntitiesREST.class);

    private AtlasEntityStore entitiesStore;

    @Context
    private HttpServletRequest httpServletRequest;

    @Inject
    public EntitiesREST(AtlasEntityStore entitiesStore, AtlasTypeRegistry atlasTypeRegistry) {
        LOG.info("EntitiesRest Init");
        this.entitiesStore = entitiesStore;
    }

    /*******
     * Entity Creation REST calls
     *******/

    @POST
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasEntity.AtlasEntities createEntities(List<AtlasEntity> entities) throws Exception {

    }

}
