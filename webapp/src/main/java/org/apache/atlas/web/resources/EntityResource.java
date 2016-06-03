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

package org.apache.atlas.web.resources;

import com.google.common.base.Preconditions;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.exception.EntityExistsException;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.exception.TraitNotFoundException;
import org.apache.atlas.typesystem.exception.TypeNotFoundException;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.types.ValueConversionException;
import org.apache.atlas.utils.ParamChecker;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * Entity management operations as REST API.
 *
 * An entity is an "instance" of a Type.  Entities conform to the definition
 * of the Type they correspond with.
 */
@Path("entities")
@Singleton
public class EntityResource {

    private static final Logger LOG = LoggerFactory.getLogger(EntityResource.class);
    private static final String TRAIT_NAME = "traitName";

    private final MetadataService metadataService;

    @Context
    UriInfo uriInfo;

    /**
     * Created by the Guice ServletModule and injected with the
     * configured MetadataService.
     *
     * @param metadataService metadata service handle
     */
    @Inject
    public EntityResource(MetadataService metadataService) {
        this.metadataService = metadataService;
    }

    /**
     * Submits the entity definitions (instances).
     * The body contains the JSONArray of entity json. The service takes care of de-duping the entities based on any
     * unique attribute for the give type.
     */
    @POST
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response submit(@Context HttpServletRequest request) {
        try {
            String entities = Servlets.getRequestPayload(request);

            //Handle backward compatibility - if entities is not JSONArray, convert to JSONArray
            try {
                new JSONArray(entities);
            } catch (JSONException e) {
                final String finalEntities = entities;
                entities = new JSONArray() {{
                    put(finalEntities);
                }}.toString();
            }

            LOG.debug("submitting entities {} ", AtlasClient.toString(new JSONArray(entities)));

            final List<String> guids = metadataService.createEntities(entities);
            JSONObject response = getResponse(new AtlasClient.EntityResult(guids, null, null));

            UriBuilder ub = uriInfo.getAbsolutePathBuilder();
            URI locationURI = guids.isEmpty() ? null : ub.path(guids.get(0)).build();

            return Response.created(locationURI).entity(response).build();

        } catch(EntityExistsException e) {
            LOG.error("Unique constraint violation", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.CONFLICT));
        } catch (ValueConversionException ve) {
            LOG.error("Unable to persist entity instance due to a desrialization error ", ve);
            throw new WebApplicationException(Servlets.getErrorResponse(ve.getCause(), Response.Status.BAD_REQUEST));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to persist entity instance", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to persist entity instance", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    private JSONObject getResponse(AtlasClient.EntityResult entityResult) throws AtlasException, JSONException {
        JSONObject response = new JSONObject();
        response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
        response.put(AtlasClient.ENTITIES, new JSONObject(entityResult.toString()).get(AtlasClient.ENTITIES));
        String sampleEntityId = getSample(entityResult);
        if (sampleEntityId != null) {
            String entityDefinition = metadataService.getEntityDefinition(sampleEntityId);
            response.put(AtlasClient.DEFINITION, new JSONObject(entityDefinition));
        }
        return response;
    }

    /**
     * Complete update of a set of entities - the values not specified will be replaced with null/removed
     * Adds/Updates given entities identified by its GUID or unique attribute
     * @return response payload as json
     */
    @PUT
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response updateEntities(@Context HttpServletRequest request) {
        try {
            final String entities = Servlets.getRequestPayload(request);
            LOG.debug("updating entities {} ", AtlasClient.toString(new JSONArray(entities)));

            AtlasClient.EntityResult entityResult = metadataService.updateEntities(entities);
            JSONObject response = getResponse(entityResult);
            return Response.ok(response).build();
        } catch(EntityExistsException e) {
            LOG.error("Unique constraint violation", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.CONFLICT));
        } catch (ValueConversionException ve) {
            LOG.error("Unable to persist entity instance due to a desrialization error ", ve);
            throw new WebApplicationException(Servlets.getErrorResponse(ve.getCause(), Response.Status.BAD_REQUEST));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to persist entity instance", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to persist entity instance", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    private String getSample(AtlasClient.EntityResult entityResult) {
        String sample = getSample(entityResult.getCreatedEntities());
        if (sample == null) {
            sample = getSample(entityResult.getUpdateEntities());
        }
        if (sample == null) {
            sample = getSample(entityResult.getDeletedEntities());
        }
        return sample;
    }


    private String getSample(List<String> list) {
        if (list != null && list.size() > 0) {
            return list.get(0);
        }
        return null;
    }

    /**
     * Adds/Updates given entity identified by its unique attribute( entityType, attributeName and value)
     * Updates support only partial update of an entity - Adds/updates any new values specified
     * Updates do not support removal of attribute values
     *
     * @param entityType the entity type
     * @param attributes the primary key attributes used to identify the entity
     * @param values the primary key attribute values
     * @param request The updated entity json
     * @return response payload as json
     * The body contains the JSONArray of entity json. The service takes care of de-duping the entities based on any
     * unique attribute for the give type.
     */
    @POST
    @Path("qualifiedName")
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response updateByPrimaryKeyOrUniqueAttribute(@QueryParam("type") String entityType,
        @QueryParam("property") List<String> attributes,
        @QueryParam("value") List<String> values, @Context HttpServletRequest request) {
        try {
            String entities = Servlets.getRequestPayload(request);

            LOG.debug("Partially updating entity by primary key {} {} {} {} ", entityType, attributes, values, entities);

            Referenceable updatedEntity =
                InstanceSerialization.fromJsonReferenceable(entities, true);
            AtlasClient.EntityResult entityResult =
                metadataService.updateEntityPartialByPrimaryKeyOrUniqueAttribute(entityType, getUniqueKeys(attributes, values), updatedEntity);

            JSONObject response = getResponse(entityResult);
            return Response.ok(response).build();
        } catch (ValueConversionException ve) {
            LOG.error("Unable to persist entity instance due to a desrialization error ", ve);
            throw new WebApplicationException(Servlets.getErrorResponse(ve.getCause(), Response.Status.BAD_REQUEST));
        } catch(EntityExistsException e) {
            LOG.error("Unique constraint violation", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.CONFLICT));
        } catch (EntityNotFoundException e) {
            LOG.error("An entity with type={} and qualifiedName={} does not exist", entityType, values, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to create/update entity {}" + entityType + ":" + attributes + "." + values, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to update entity {}" + entityType + ":" + attributes + "." + values, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Updates entity identified by its GUID
     * Support Partial update of an entity - Adds/updates any new values specified
     * Does not support removal of attribute values
     *
     * @param guid
     * @param request The updated entity json
     * @return
     */
    @POST
    @Path("{guid}")
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response updateEntityByGuid(@PathParam("guid") String guid, @QueryParam("property") String attribute,
                                       @Context HttpServletRequest request) {
        if (StringUtils.isEmpty(attribute)) {
            return updateEntityPartialByGuid(guid, request);
        } else {
            return updateEntityAttributeByGuid(guid, attribute, request);
        }
    }
    
    private Response updateEntityPartialByGuid(String guid, HttpServletRequest request) {
        try {
            ParamChecker.notEmpty(guid, "Guid property cannot be null");
            final String entityJson = Servlets.getRequestPayload(request);
            LOG.debug("partially updating entity for guid {} : {} ", guid, entityJson);

            Referenceable updatedEntity =
                    InstanceSerialization.fromJsonReferenceable(entityJson, true);
            AtlasClient.EntityResult entityResult = metadataService.updateEntityPartialByGuid(guid, updatedEntity);
            JSONObject response = getResponse(entityResult);
            return Response.ok(response).build();
        } catch (EntityNotFoundException e) {
            LOG.error("An entity with GUID={} does not exist", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to update entity {}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to update entity {}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Supports Partial updates
     * Adds/Updates given entity specified by its GUID
     * Supports updation of only simple primitive attributes like strings, ints, floats, enums, class references and
     * does not support updation of complex types like arrays, maps
     * @param guid entity id
     * @param property property to add
     * @postbody property's value
     * @return response payload as json
     */
    private Response updateEntityAttributeByGuid(String guid, String property, HttpServletRequest request) {
        try {
            Preconditions.checkNotNull(property, "Entity property cannot be null");
            String value = Servlets.getRequestPayload(request);
            Preconditions.checkNotNull(value, "Entity value cannot be null");

            AtlasClient.EntityResult entityResult = metadataService.updateEntityAttributeByGuid(guid, property, value);
            JSONObject response = getResponse(entityResult);
            return Response.ok(response).build();
        } catch (EntityNotFoundException e) {
            LOG.error("An entity with GUID={} does not exist", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to add property {} to entity id {}", property, guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to add property {} to entity id {}", property, guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Delete entities from the repository identified by their guids (including their composite references)
     * or
     * Deletes a single entity identified by its type and unique attribute value from the repository (including their composite references)
     * 
     * @param guids list of deletion candidate guids
     *              or
     * @param entityType the entity type
     * @param attributes the primary keys  used to identify the entity
     * @param values the primary key values value used to identify the entity
     * @return response payload as json - including guids of entities(including composite references from that entity) that were deleted
     */
    @DELETE
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response deleteEntities(@QueryParam("guid") List<String> guids,
        @QueryParam("type") String entityType,
        @QueryParam("property") final List<String> attributes,
        @QueryParam("value") final List<String> values) {
        
        try {
            AtlasClient.EntityResult entityResult;
            if (guids != null && !guids.isEmpty()) {
                entityResult = metadataService.deleteEntities(guids);
            } else {
                ParamChecker.notEmptyElements(attributes, "Primary keys");
                ParamChecker.notEmptyElements(values, "Primary key values");

                entityResult = metadataService.deleteEntityByPrimaryKeyOrUniqueAttribute(entityType, getUniqueKeys(attributes, values));
            }
            JSONObject response = getResponse(entityResult);
            return Response.ok(response).build();
        } catch (EntityNotFoundException e) {
            if(guids != null || !guids.isEmpty()) {
                LOG.error("An entity with GUID={} does not exist", guids, e);
            } else {
                LOG.error("An entity with qualifiedName {}-{}-{} does not exist", entityType, attributes, values, e);
            }
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        }  catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to delete entities {}", guids, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to delete entities {}", guids, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    private Map<String, String> getUniqueKeys(List<String> attributes, List<String> values) {
        Map<String, String> primaryKeys = new LinkedHashMap<String, String>();
        for(int i = 0; i < attributes.size() ; i++) {
            primaryKeys.put(attributes.get(i), values.get(i));
        }
        return primaryKeys;
    }

    /**
     * Fetch the complete definition of an entity given its GUID.
     *
     * @param guid GUID for the entity
     */
    @GET
    @Path("{guid}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getEntityDefinition(@PathParam("guid") String guid) {
        try {
            LOG.debug("Fetching entity definition for guid={} ", guid);
            ParamChecker.notEmpty(guid, "guid cannot be null");
            final String entityDefinition = metadataService.getEntityDefinition(guid);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());

            Response.Status status = Response.Status.NOT_FOUND;
            if (entityDefinition != null) {
                response.put(AtlasClient.DEFINITION, new JSONObject(entityDefinition));
                status = Response.Status.OK;
            } else {
                response.put(AtlasClient.ERROR,
                        Servlets.escapeJsonString(String.format("An entity with GUID={%s} does not exist", guid)));
            }

            return Response.status(status).entity(response).build();

        } catch (EntityNotFoundException e) {
            LOG.error("An entity with GUID={} does not exist", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Bad GUID={}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to get instance definition for GUID {}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Gets the list of entities for a given entity type.
     *
     * @param entityType name of a type which is unique
     */
    public Response getEntityListByType(String entityType) {
        try {
            Preconditions.checkNotNull(entityType, "Entity type cannot be null");

            LOG.debug("Fetching entity list for type={} ", entityType);
            final List<String> entityList = metadataService.getEntityList(entityType);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.TYPENAME, entityType);
            response.put(AtlasClient.RESULTS, new JSONArray(entityList));
            response.put(AtlasClient.COUNT, entityList.size());

            return Response.ok(response).build();
        } catch (NullPointerException e) {
            LOG.error("Entity type cannot be null", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to get entity list for type {}", entityType, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to get entity list for type {}", entityType, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    @GET
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getEntity(@QueryParam("type") String entityType,
        @QueryParam("property") final List<String> attributes,
        @QueryParam("value") final List<String> values) {
        if (attributes != null && attributes.isEmpty()) {
            return getEntityListByType(entityType);
        } else {
            try {

                String entityDefinition = metadataService.getEntityByPrimaryKeyOrUniqueAttribute(entityType, getUniqueKeys(attributes, values));

                JSONObject response = new JSONObject();
                response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());

                Response.Status status = Response.Status.NOT_FOUND;
                if (entityDefinition != null) {
                    response.put(AtlasClient.DEFINITION, new JSONObject(entityDefinition));
                    status = Response.Status.OK;
                } else {
                    response.put(AtlasClient.ERROR,
                        Servlets.escapeJsonString(String.format("An entity with type=%s and primary key{%s}={%s} does not exist", entityType, attributes, values)));
                }

                return Response.status(status).entity(response).build();

            } catch (EntityNotFoundException e) {
                LOG.error("An entity with type and primary key {}-{}-{} does not exist", entityType, attributes, values, e);
                throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
            } catch (AtlasException | IllegalArgumentException e) {
                LOG.error("Unable to delete entities with primary key(s) {} {} ", attributes, values, e);
                throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
            } catch (Throwable e) {
                LOG.error("Unable to delete entities with primary key(s) {} {} ", attributes, values, e);
                throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
            }
        }
    }


    // Trait management functions

    /**
     * Gets the list of trait names for a given entity represented by a guid.
     *
     * @param guid globally unique identifier for the entity
     * @return a list of trait names for the given entity guid
     */
    @GET
    @Path("{guid}/traits")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getTraitNames(@PathParam("guid") String guid) {
        try {
            LOG.debug("Fetching trait names for entity={}", guid);
            final List<String> traitNames = metadataService.getTraitNames(guid);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.RESULTS, new JSONArray(traitNames));
            response.put(AtlasClient.COUNT, traitNames.size());

            return Response.ok(response).build();
        } catch (EntityNotFoundException e) {
            LOG.error("An entity with GUID={} does not exist", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to get trait names for entity {}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to get trait names for entity {}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Adds a new trait to an existing entity represented by a guid.
     *
     * @param guid globally unique identifier for the entity
     */
    @POST
    @Path("{guid}/traits")
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response addTrait(@Context HttpServletRequest request, @PathParam("guid") String guid) {
        try {
            final String traitDefinition = Servlets.getRequestPayload(request);
            LOG.debug("Adding trait={} for entity={} ", traitDefinition, guid);
            metadataService.addTrait(guid, traitDefinition);

            UriBuilder ub = uriInfo.getAbsolutePathBuilder();
            URI locationURI = ub.path(guid).build();

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());

            return Response.created(locationURI).entity(response).build();
        } catch (EntityNotFoundException | TypeNotFoundException e) {
            LOG.error("An entity with GUID={} does not exist", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to add trait for entity={}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to add trait for entity={}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Deletes a given trait from an existing entity represented by a guid.
     *
     * @param guid      globally unique identifier for the entity
     * @param traitName name of the trait
     */
    @DELETE
    @Path("{guid}/traits/{traitName}")
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response deleteTrait(@Context HttpServletRequest request, @PathParam("guid") String guid,
            @PathParam(TRAIT_NAME) String traitName) {
        LOG.debug("Deleting trait={} from entity={} ", traitName, guid);
        try {
            metadataService.deleteTrait(guid, traitName);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(TRAIT_NAME, traitName);

            return Response.ok(response).build();
        } catch (EntityNotFoundException | TypeNotFoundException e) {
            LOG.error("An entity with GUID={} does not exist", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (TraitNotFoundException e) {
            LOG.error("The trait name={} for entity={} does not exist.", traitName, guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to delete trait name={} for entity={}", traitName, guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to delete trait name={} for entity={}", traitName, guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Returns the entity audit events for a given entity id. The events are returned in the decreasing order of timestamp.
     * @param guid entity id
     * @param startKey used for pagination. Startkey is inclusive, the returned results contain the event with the given startkey.
     *                  First time getAuditEvents() is called for an entity, startKey should be null,
     *                  with count = (number of events required + 1). Next time getAuditEvents() is called for the same entity,
     *                  startKey should be equal to the entityKey of the last event returned in the previous call.
     * @param count number of events required
     * @return
     */
    @GET
    @Path("{guid}/audit")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getAuditEvents(@PathParam("guid") String guid, @QueryParam("startKey") String startKey,
                                   @QueryParam("count") @DefaultValue("100") short count) {
        LOG.debug("Audit events request for entity {}, start key {}, number of results required {}", guid, startKey,
                count);
        try {
            List<EntityAuditEvent> events = metadataService.getAuditEvents(guid, startKey, count);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.EVENTS, getJSONArray(events));
            return Response.ok(response).build();
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to get audit events for entity {}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to get audit events for entity {}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    private <T> JSONArray getJSONArray(Collection<T> elements) throws JSONException {
        JSONArray jsonArray = new JSONArray();
        for(T element : elements) {
            jsonArray.put(new JSONObject(element.toString()));
        }
        return jsonArray;
    }
}
