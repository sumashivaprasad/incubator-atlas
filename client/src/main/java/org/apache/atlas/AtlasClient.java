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

package org.apache.atlas;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import org.apache.atlas.security.SecureClientUtils;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.atlas.security.SecurityProperties.TLS_ENABLED;

/**
 * Client for metadata.
 */
public class AtlasClient {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasClient.class);
    public static final String NAME = "name";
    public static final String GUID = "GUID";
    public static final String TYPE = "type";
    public static final String TYPENAME = "typeName";

    public static final String DEFINITION = "definition";
    public static final String ERROR = "error";
    public static final String STACKTRACE = "stackTrace";
    public static final String REQUEST_ID = "requestId";
    public static final String RESULTS = "results";
    public static final String COUNT = "count";
    public static final String ROWS = "rows";
    public static final String DATATYPE = "dataType";

    public static final String BASE_URI = "api/atlas/";
    public static final String TYPES = "types";
    public static final String URI_ENTITY = "entities";
    public static final String URI_SEARCH = "discovery/search";
    public static final String URI_LINEAGE = "lineage/hive/table";

    public static final String QUERY = "query";
    public static final String QUERY_TYPE = "queryType";
    public static final String ATTRIBUTE_NAME = "property";
    public static final String ATTRIBUTE_VALUE = "value";


    public static final String INFRASTRUCTURE_SUPER_TYPE = "Infrastructure";
    public static final String DATA_SET_SUPER_TYPE = "DataSet";
    public static final String PROCESS_SUPER_TYPE = "Process";
    public static final String REFERENCEABLE_SUPER_TYPE = "Referenceable";
    public static final String REFERENCEABLE_ATTRIBUTE_NAME = "qualifiedName";

    public static final String JSON_MEDIA_TYPE = MediaType.APPLICATION_JSON + "; charset=UTF-8";

    private WebResource service;

    protected AtlasClient() {
        //do nothing. For LocalAtlasClient
    }

    public AtlasClient(String baseUrl) {
        this(baseUrl, null, null);
    }

    public AtlasClient(String baseUrl, UserGroupInformation ugi, String doAsUser) {
        DefaultClientConfig config = new DefaultClientConfig();
        Configuration clientConfig = null;
        int readTimeout = 60000;
        int connectTimeout = 60000;
        try {
            clientConfig = getClientProperties();
            if (clientConfig.getBoolean(TLS_ENABLED, false)) {
                // create an SSL properties configuration if one doesn't exist.  SSLFactory expects a file, so forced
                // to create a
                // configuration object, persist it, then subsequently pass in an empty configuration to SSLFactory
                SecureClientUtils.persistSSLClientConfiguration(clientConfig);
            }
            readTimeout = clientConfig.getInt("atlas.client.readTimeoutMSecs", readTimeout);
            connectTimeout = clientConfig.getInt("atlas.client.connectTimeoutMSecs", connectTimeout);
        } catch (Exception e) {
            LOG.info("Error processing client configuration.", e);
        }

        URLConnectionClientHandler handler =
            SecureClientUtils.getClientConnectionHandler(config, clientConfig, doAsUser, ugi);

        Client client = new Client(handler, config);
        client.resource(UriBuilder.fromUri(baseUrl).build());
        client.setReadTimeout(readTimeout);
        client.setConnectTimeout(connectTimeout);

        service = client.resource(UriBuilder.fromUri(baseUrl).build());
    }

    protected Configuration getClientProperties() throws AtlasException {
        return ApplicationProperties.get(ApplicationProperties.CLIENT_PROPERTIES);
    }

    enum API {

        //Type operations
        CREATE_TYPE(BASE_URI + TYPES, HttpMethod.POST),
        GET_TYPE(BASE_URI + TYPES, HttpMethod.GET),
        LIST_TYPES(BASE_URI + TYPES, HttpMethod.GET),
        LIST_TRAIT_TYPES(BASE_URI + TYPES + "?type=trait", HttpMethod.GET),

        //Entity operations
        CREATE_ENTITY(BASE_URI + URI_ENTITY, HttpMethod.POST),
        GET_ENTITY(BASE_URI + URI_ENTITY, HttpMethod.GET),
        UPDATE_ENTITY(BASE_URI + URI_ENTITY, HttpMethod.PUT),
        UPDATE_ENTITY_PARTIAL(BASE_URI + URI_ENTITY, HttpMethod.POST),
        LIST_ENTITIES(BASE_URI + URI_ENTITY, HttpMethod.GET),

        //Trait operations
        ADD_TRAITS(BASE_URI + URI_ENTITY, HttpMethod.POST),
        DELETE_TRAITS(BASE_URI + URI_ENTITY, HttpMethod.DELETE),
        LIST_TRAITS(BASE_URI + URI_ENTITY, HttpMethod.GET),

        //Search operations
        SEARCH(BASE_URI + URI_SEARCH, HttpMethod.GET),
        SEARCH_DSL(BASE_URI + URI_SEARCH + "/dsl", HttpMethod.GET),
        SEARCH_GREMLIN(BASE_URI + URI_SEARCH + "/gremlin", HttpMethod.GET),
        SEARCH_FULL_TEXT(BASE_URI + URI_SEARCH + "/fulltext", HttpMethod.GET),

        //Lineage operations
        LINEAGE_INPUTS_GRAPH(BASE_URI + URI_LINEAGE, HttpMethod.GET),
        LINEAGE_OUTPUTS_GRAPH(BASE_URI + URI_LINEAGE, HttpMethod.GET),
        LINEAGE_SCHEMA(BASE_URI + URI_LINEAGE, HttpMethod.GET);

        private final String method;
        private final String path;

        API(String path, String method) {
            this.path = path;
            this.method = method;
        }

        public String getMethod() {
            return method;
        }

        public String getPath() {
            return path;
        }
    }

    /**
     * Register the given type(meta model)
     * @param typeAsJson type definition a jaon
     * @return result json object
     * @throws AtlasServiceException
     */
    public JSONObject createType(String typeAsJson) throws AtlasServiceException {
        return callAPI(API.CREATE_TYPE, typeAsJson, Response.Status.CREATED);
    }

    public List<String> listTypes() throws AtlasServiceException {
        final JSONObject jsonObject = callAPI(API.LIST_TYPES, null, Response.Status.OK);
        return extractResults(jsonObject);
    }

    public String getType(String typeName) throws AtlasServiceException {
        WebResource resource = getResource(API.GET_TYPE, typeName);
        try {
            JSONObject response = callAPIWithResource(API.GET_TYPE, resource, Response.Status.OK);
            return response.getString(DEFINITION);
        } catch (AtlasServiceException e) {
            if (e.getStatus() == ClientResponse.Status.NOT_FOUND) {
                return null;
            }
            throw e;
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
        }
    }

    /**
     * Create the given entity
     * @param entities entity(type instance) as json
     * @return json array of guids
     * @throws AtlasServiceException
     */
    public JSONArray createEntity(JSONArray entities) throws AtlasServiceException {
        JSONObject response = callAPI(API.CREATE_ENTITY, entities.toString(), Response.Status.CREATED);
        try {
            return response.getJSONArray(GUID);
        } catch (JSONException e) {
            throw new AtlasServiceException(API.GET_ENTITY, e);
        }
    }

    /**
     * Updates the given entities
     * @param entitiesAsJson entity(type instance) as json
     * @return json array of guids which were updated/created
     * @throws AtlasServiceException
     */
    public JSONArray updateEntities(boolean partialUpdate, String... entitiesAsJson) throws AtlasServiceException {
        return updateEntities(partialUpdate, new JSONArray(Arrays.asList(entitiesAsJson)));
    }

    /**
     * Create the given entity
     * @param entities entity(type instance) as json
     * @return json array of guids which were updated/created
     * @throws AtlasServiceException
     */
    public JSONArray updateEntities(boolean partialUpdate, JSONArray entities) throws AtlasServiceException {
        API api = partialUpdate ? API.UPDATE_ENTITY_PARTIAL : API.UPDATE_ENTITY;

        JSONObject response = callAPI(api, entities.toString(), Response.Status.OK);
        try {
            return response.getJSONArray(GUID);
        } catch (JSONException e) {
            throw new AtlasServiceException(API.GET_ENTITY, e);
        }
    }

    /**
     * Create the given entity
     * @param entitiesAsJson entity(type instance) as json
     * @return json array of guids
     * @throws AtlasServiceException
     */
    public JSONArray createEntity(String... entitiesAsJson) throws AtlasServiceException {
        return createEntity(new JSONArray(Arrays.asList(entitiesAsJson)));
    }

    /**
     * Get an entity given the entity id
     * @param guid entity id
     * @return result object
     * @throws AtlasServiceException
     */
    public Referenceable getEntity(String guid) throws AtlasServiceException {
        JSONObject jsonResponse = callAPI(API.GET_ENTITY, null, Response.Status.OK, guid);
        try {
            String entityInstanceDefinition = jsonResponse.getString(AtlasClient.DEFINITION);
            return InstanceSerialization.fromJsonReferenceable(entityInstanceDefinition, true);
        } catch (JSONException e) {
            throw new AtlasServiceException(API.GET_ENTITY, e);
        }
    }

    public static String toString(JSONArray jsonArray) throws JSONException {
        ArrayList<String> resultsList = new ArrayList<>();
        for (int index = 0; index < jsonArray.length(); index++) {
            resultsList.add(jsonArray.getString(index));
        }
        return StringUtils.join(resultsList, ",");
    }

    /**
     * Get an entity given the entity id
     * @param entityType entity type name
     * @param attribute qualified name of the entity
     * @param value
     * @return result object
     * @throws AtlasServiceException
     */
    public Referenceable getEntity(String entityType, String attribute, String value) throws AtlasServiceException {
        WebResource resource = getResource(API.GET_ENTITY);
        resource = resource.queryParam(TYPE, entityType);
        resource = resource.queryParam(ATTRIBUTE_NAME, attribute);
        resource = resource.queryParam(ATTRIBUTE_VALUE, value);
        JSONObject jsonResponse = callAPIWithResource(API.GET_ENTITY, resource, Response.Status.OK);
        try {
            String entityInstanceDefinition = jsonResponse.getString(AtlasClient.DEFINITION);
            return InstanceSerialization.fromJsonReferenceable(entityInstanceDefinition, true);
        } catch (JSONException e) {
            throw new AtlasServiceException(API.GET_ENTITY, e);
        }
    }

    /**
     * List entities for a given entity type
     * @param entityType
     * @return
     * @throws AtlasServiceException
     */
    public List<String> listEntities(String entityType) throws AtlasServiceException {
        WebResource resource = getResource(API.LIST_ENTITIES);
        resource = resource.queryParam(TYPE, entityType);
        JSONObject jsonResponse = callAPIWithResource(API.LIST_ENTITIES, resource, Response.Status.OK);
        return extractResults(jsonResponse);
    }

    private List<String> extractResults(JSONObject jsonResponse) throws AtlasServiceException {
        try {
            JSONArray results = jsonResponse.getJSONArray(AtlasClient.RESULTS);
            ArrayList<String> resultsList = new ArrayList<>();
            for (int index = 0; index < results.length(); index++) {
                resultsList.add(results.getString(index));
            }
            return resultsList;
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
        }
    }

    /**
     * Supports Partial updates
     * Updates property for the entity corresponding to guid
     * @param guid      guid
     * @param property  property key
     * @param value     property value
     */
    public JSONObject updateEntityAttribute(String guid, String property, String value) throws AtlasServiceException {
        return callAPI(API.UPDATE_ENTITY_PARTIAL, value, Response.Status.OK, property);
    }

    /**
    * Supports Partial updates
    * Updates properties set in the definition for the entity corresponding to guid
    * @param guid      guid
    * @param entityJson json of the entity definition
    */
    public JSONObject updateEntity(String guid, String entityJson) throws AtlasServiceException {
        return callAPI(API.UPDATE_ENTITY_PARTIAL, entityJson, Response.Status.OK, guid);
    }

    /**
     * Supports Partial updates
     * Updates properties set in the definition for the entity corresponding to guid
     * @param entityType Type of the entity being updated
     * @param uniqueAttributeName Attribute Name that uniquely identifies the entity
     * @param uniqueAttributeValue Attribute Value that uniquely identifies the entity
     * @param entityJson json of the entity definition
     */
    public JSONObject updateEntity(String entityType, String uniqueAttributeName, String uniqueAttributeValue, String entityJson) throws AtlasServiceException {
        WebResource resource = getResource(API.UPDATE_ENTITY_PARTIAL);
        resource = resource.queryParam(TYPE, entityType);
        resource = resource.queryParam(ATTRIBUTE_NAME, uniqueAttributeName);
        resource = resource.queryParam(ATTRIBUTE_VALUE, uniqueAttributeValue);
        return callAPIWithResource(API.UPDATE_ENTITY_PARTIAL, resource, entityJson, Response.Status.OK);
    }

    /**
     * Replaces entity definitions identified by their guid or unique attribute
     * Updates properties set in the definition for the entity corresponding to guid
     * @param entitiesJSON The json for entity definitions
     */
    public JSONObject updateEntities(String entitiesJSON) throws AtlasServiceException {
        return updateEntities(new JSONArray(Arrays.asList(entitiesJSON)));
    }

    /**
    * Replaces entity definitions identified by their guid or unique attribute
    * Updates properties set in the definition for the entity corresponding to guid
    * @param entitiesJSONArray The json array of entity definitions
    */
    public JSONObject updateEntities(JSONArray entitiesJSONArray) throws AtlasServiceException {
        return callAPI(API.UPDATE_ENTITY, entitiesJSONArray.toString(), Response.Status.OK);
    }



    /**
     * Search using gremlin/dsl/full text
     * @param searchQuery
     * @return
     * @throws AtlasServiceException
     */
    public JSONArray search(String searchQuery) throws AtlasServiceException {
        WebResource resource = getResource(API.SEARCH);
        resource = resource.queryParam(QUERY, searchQuery);
        JSONObject result = callAPIWithResource(API.SEARCH, resource, Response.Status.OK);
        try {
            return result.getJSONArray(RESULTS);
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
        }

    }

    /**
     * Search given type name, an attribute and its value. Uses search dsl
     * @param typeName name of the entity type
     * @param attributeName attribute name
     * @param attributeValue attribute value
     * @return result json object
     * @throws AtlasServiceException
     */
    public JSONArray rawSearch(String typeName, String attributeName, Object attributeValue)
            throws AtlasServiceException {
        //        String gremlinQuery = String.format(
        //                "g.V.has(\"typeName\",\"%s\").and(_().has(\"%s.%s\", T.eq, \"%s\")).toList()",
        //                typeName, typeName, attributeName, attributeValue);
        //        return searchByGremlin(gremlinQuery);
        String dslQuery = String.format("%s where %s = \"%s\"", typeName, attributeName, attributeValue);
        return searchByDSL(dslQuery);
    }

    /**
     * Search given query DSL
     * @param query DSL query
     * @return result json object
     * @throws AtlasServiceException
     */
    public JSONArray searchByDSL(String query) throws AtlasServiceException {
        LOG.debug("DSL query: {}", query);
        WebResource resource = getResource(API.SEARCH_DSL);
        resource = resource.queryParam(QUERY, query);
        JSONObject result = callAPIWithResource(API.SEARCH_DSL, resource, Response.Status.OK);
        try {
            return result.getJSONArray(RESULTS);
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
        }
    }

    /**
     * Search given gremlin query
     * @param gremlinQuery Gremlin query
     * @return result json object
     * @throws AtlasServiceException
     */
    public JSONArray searchByGremlin(String gremlinQuery) throws AtlasServiceException {
        LOG.debug("Gremlin query: " + gremlinQuery);
        WebResource resource = getResource(API.SEARCH_GREMLIN);
        resource = resource.queryParam(QUERY, gremlinQuery);
        JSONObject result = callAPIWithResource(API.SEARCH_GREMLIN, resource, Response.Status.OK);
        try {
            return result.getJSONArray(RESULTS);
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
        }
    }

    /**
     * Search given full text search
     * @param query Query
     * @return result json object
     * @throws AtlasServiceException
     */
    public JSONObject searchByFullText(String query) throws AtlasServiceException {
        WebResource resource = getResource(API.SEARCH_FULL_TEXT);
        resource = resource.queryParam(QUERY, query);
        return callAPIWithResource(API.SEARCH_FULL_TEXT, resource, Response.Status.OK);
    }

    public JSONObject getInputGraph(String datasetName) throws AtlasServiceException {
        JSONObject response = callAPI(API.LINEAGE_INPUTS_GRAPH, null, Response.Status.OK, datasetName, "/inputs/graph");
        try {
            return response.getJSONObject(AtlasClient.RESULTS);
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
        }
    }

    public JSONObject getOutputGraph(String datasetName) throws AtlasServiceException {
        JSONObject response = callAPI(API.LINEAGE_OUTPUTS_GRAPH, null, Response.Status.OK, datasetName, "/outputs/graph");
        try {
            return response.getJSONObject(AtlasClient.RESULTS);
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
        }
    }

    public String getRequestId(JSONObject json) throws AtlasServiceException {
        try {
            return json.getString(REQUEST_ID);
        } catch (JSONException e) {
            throw new AtlasServiceException(e);
        }
    }

    private WebResource getResource(API api, String... pathParams) {
        WebResource resource = service.path(api.getPath());
        if (pathParams != null) {
            for (String pathParam : pathParams) {
                resource = resource.path(pathParam);
            }
        }
        return resource;
    }

    private JSONObject callAPIWithResource(API api, WebResource resource, Response.Status expectedStatus) throws AtlasServiceException {
        return callAPIWithResource(api, resource, null, expectedStatus);
    }

    private JSONObject callAPIWithResource(API api, WebResource resource, Object requestObject, Response.Status expectedStatus)
    throws AtlasServiceException {
        ClientResponse clientResponse = resource.accept(JSON_MEDIA_TYPE).type(JSON_MEDIA_TYPE)
                .method(api.getMethod(), ClientResponse.class, requestObject);

        if (clientResponse.getStatus() == expectedStatus.getStatusCode()) {
            String responseAsString = clientResponse.getEntity(String.class);
            try {
                return new JSONObject(responseAsString);
            } catch (JSONException e) {
                throw new AtlasServiceException(api, e);
            }
        }

        throw new AtlasServiceException(api, clientResponse);
    }

    private JSONObject callAPI(API api, Object requestObject, Response.Status expectedStatus, String... pathParams) throws AtlasServiceException {
        WebResource resource = getResource(api, pathParams);
        return callAPIWithResource(api, resource, requestObject, expectedStatus);
    }
}
