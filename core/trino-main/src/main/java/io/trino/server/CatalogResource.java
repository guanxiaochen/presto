/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.io.ByteStreams;
import com.google.common.net.HttpHeaders;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.ResponseHandler;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.connector.CatalogName;
import io.trino.connector.ConnectorManager;
import io.trino.metadata.AllNodes;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.ForNodeManager;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.NodeState;
import io.trino.server.security.ResourceSecurity;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.ResponseHandlerUtils.propagate;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * todo guan 自己添加的catalog接口
 */
@SuppressWarnings("UnstableApiUsage")
@Path("/v1/catalog")
public class CatalogResource
{
    private static final Logger log = Logger.get(CatalogResource.class);
    public static final JsonCodec<CatalogInfo> CATALOG_INFO_JSON_CODEC = JsonCodec.jsonCodec(CatalogInfo.class);
    /**
     * JsonResponseHandler.createJsonResponseHandler(JsonCodec.jsonCodec(Resp.class))
     */
    public static final ResponseHandler<Resp, RuntimeException> JSON_RESPONSE_HANDLER = new ResponseHandler<>()
    {

        private final com.google.common.net.MediaType MEDIA_TYPE_JSON = com.google.common.net.MediaType.create("application", "json");
        private final JsonCodec<Resp> jsonCodec = JsonCodec.jsonCodec(Resp.class);
        @Override
        public Resp handleException(Request request, Exception exception) throws RuntimeException
        {
            throw propagate(request, exception);
        }

        @Override
        public Resp handle(Request request, io.airlift.http.client.Response response) throws RuntimeException
        {
            String contentType = response.getHeader(CONTENT_TYPE);
            if (contentType == null) {
                return Resp.status(response.getStatusCode(), "Content-Type is not set for response");
            }
            if (!com.google.common.net.MediaType.parse(contentType).is(MEDIA_TYPE_JSON)) {
                return Resp.status(response.getStatusCode(), "Expected application/json response from server but got " + contentType);
            }
            byte[] bytes;
            try {
                bytes = ByteStreams.toByteArray(response.getInputStream());
            }
            catch (Exception e) {
                return Resp.status(response.getStatusCode(), "Error reading response from server");
            }
            try {
                return jsonCodec.fromJson(bytes);
            }
            catch (IllegalArgumentException e) {
                return Resp.status(response.getStatusCode(), new String(bytes, UTF_8));
            }
        }
    };
    private final ConnectorManager connectorManager;
    private final CatalogManager catalogManager;
    private final InternalNodeManager nodeManager;
    private final HttpClient httpClient;
    private final Announcer announcer;

    @Inject
    public CatalogResource(
            ConnectorManager connectorManager,
            CatalogManager catalogManager,
            InternalNodeManager nodeManager,
            @ForNodeManager HttpClient httpClient,
            Announcer announcer)
    {
        this.connectorManager = requireNonNull(connectorManager, "connectorManager is null");
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.announcer = requireNonNull(announcer, "announcer is null");
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Path("nodes")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getNodes()
    {
        AllNodes allNodes = nodeManager.getAllNodes();

        List<NodeInfo> nodeInfos = new ArrayList<>();
        for (InternalNode node : allNodes.getActiveNodes()) {
            nodeInfos.add(new NodeInfo(node.getNodeIdentifier(), node.getInternalUri().toString(), node.getVersion(), node.isCoordinator(), NodeState.ACTIVE));
        }
        for (InternalNode node : allNodes.getInactiveNodes()) {
            nodeInfos.add(new NodeInfo(node.getNodeIdentifier(), node.getInternalUri().toString(), node.getVersion(), node.isCoordinator(), NodeState.INACTIVE));
        }
        for (InternalNode node : allNodes.getShuttingDownNodes()) {
            nodeInfos.add(new NodeInfo(node.getNodeIdentifier(), node.getInternalUri().toString(), node.getVersion(), node.isCoordinator(), NodeState.SHUTTING_DOWN));
        }
        return Response.ok(nodeInfos).build();
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Path("list")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getCatalogs()
    {
        return Response.ok(catalogManager.getCatalogs().stream().map(Catalog::getCatalogName)).build();
    }

    @ResourceSecurity(PUBLIC)
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public synchronized Response putCreate(CatalogInfo catalogInfo)
    {
        return create(catalogInfo);
    }
    @ResourceSecurity(PUBLIC)
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public synchronized Response create(CatalogInfo catalogInfo)
    {
        if (!nodeManager.getCurrentNode().isCoordinator()) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        requireNonNull(catalogInfo, "catalogInfo is null");
        for (InternalNode node : getNodes(false)) {
            Resp nodeResponse = doNodeCreate(node, catalogInfo);
            if (isFail(nodeResponse.getStatus())) {
                return nodeResponse.toResponse();
            }
        }
        for (InternalNode node : getNodes(true)) {
            Resp nodeResponse = doNodeCreate(node, catalogInfo);
            if (isFail(nodeResponse.getStatus())) {
                return nodeResponse.toResponse();
            }
        }
        return Response.status(Response.Status.OK).build();
    }

    @ResourceSecurity(PUBLIC)
    @PUT
    @Path("save")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public synchronized Response putSave(CatalogInfo catalogInfo)
    {
        return save(catalogInfo);
    }


    @ResourceSecurity(PUBLIC)
    @POST
    @Path("save")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public synchronized Response save(CatalogInfo catalogInfo)
    {
        if (!nodeManager.getCurrentNode().isCoordinator()) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        requireNonNull(catalogInfo, "catalogInfo is null");
        Response delete = delete(catalogInfo.getCatalogName());
        if (isFail(delete.getStatus())) {
            return delete;
        }
        return create(catalogInfo);
    }

    @ResourceSecurity(PUBLIC)
    @POST
    @Path("node")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response nodeCreate(CatalogInfo catalogInfo)
    {
        requireNonNull(catalogInfo, "catalogInfo is null");
        if (catalogManager.getCatalog(catalogInfo.getCatalogName()).isPresent()) {
            return Response.status(Response.Status.OK).build();
        }

        try {
            CatalogName connectorId = connectorManager.createCatalog(
                    catalogInfo.getCatalogName(),
                    catalogInfo.getConnectorName(),
                    catalogInfo.getProperties());

            updateConnectorIdAnnouncement(announcer, connectorId);
        }
        catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage()).build();
        }
        return Response.status(Response.Status.OK).build();
    }

    @ResourceSecurity(PUBLIC)
    @DELETE
    @Path("{catalog}")
    @Produces(MediaType.APPLICATION_JSON)
    public synchronized Response delete(@PathParam("catalog") String catalog)
    {
        if (!nodeManager.getCurrentNode().isCoordinator()) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        requireNonNull(catalog, "catalogInfo is null");
        for (InternalNode node : getNodes(false)) {
            Resp nodeResponse = doNodeDelete(node, catalog);
            if (isFail(nodeResponse.getStatus())) {
                return nodeResponse.toResponse();
            }
        }
        for (InternalNode node : getNodes(true)) {
            Resp nodeResponse = doNodeDelete(node, catalog);
            if (isFail(nodeResponse.getStatus())) {
                return nodeResponse.toResponse();
            }
        }
        return Response.status(Response.Status.OK).build();
    }

    @ResourceSecurity(PUBLIC)
    @DELETE
    @Path("node/{catalog}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response nodeDelete(@PathParam("catalog") String catalog)
    {
        requireNonNull(catalog, "catalog is null");
        if (catalogManager.getCatalog(catalog).isEmpty()) {
            return Response.status(Response.Status.OK).build();
        }
        try {
            connectorManager.dropConnection(catalog);
        }
        catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage()).build();
        }
        return Response.status(Response.Status.OK).build();
    }

    private static void updateConnectorIdAnnouncement(Announcer announcer, CatalogName connectorId)
    {
        //
        // This code was copied from TrinoServer, and is a hack that should be removed when the connectorId property is removed
        //

        // get existing announcement
        ServiceAnnouncement announcement = getTrinoAnnouncement(announcer.getServiceAnnouncements());

        // update connectorIds property
        Map<String, String> properties = new LinkedHashMap<>(announcement.getProperties());
        String property = nullToEmpty(properties.get("connectorIds"));
        Set<String> connectorIds = new LinkedHashSet<>(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property));
        connectorIds.add(connectorId.toString());
        properties.put("connectorIds", Joiner.on(',').join(connectorIds));

        // update announcement
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(serviceAnnouncement(announcement.getType()).addProperties(properties).build());
        announcer.forceAnnounce();
    }

    private static ServiceAnnouncement getTrinoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("trino")) {
                return announcement;
            }
        }
        throw new RuntimeException("Trino announcement not found: " + announcements);
    }

    private Resp doNodeCreate(InternalNode node, CatalogInfo catalogInfo)
    {
        try {
            String path = "/v1/catalog/node";
            HttpUriBuilder httpUriBuilder = HttpUriBuilder.uriBuilderFrom(node.getInternalUri());
            Request request = preparePost()
                    .setUri(httpUriBuilder.appendPath(path).build())
                    .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                    .setHeader(HttpHeaders.ACCEPT, "application/json")
                    .setBodyGenerator(createStaticBodyGenerator(CATALOG_INFO_JSON_CODEC.toJsonBytes(catalogInfo)))
                    .build();
            return httpClient.execute(request, JSON_RESPONSE_HANDLER);
        }
        catch (Exception e) {
            log.error(e, "create catalog failed: %s", e.getMessage());
            return Resp.status(Response.Status.GATEWAY_TIMEOUT.getStatusCode(), e.getMessage());
        }
    }

    private Resp doNodeDelete(InternalNode node, String catalog)
    {
        try {
            String path = "/v1/catalog/node/" + catalog;
            HttpUriBuilder httpUriBuilder = HttpUriBuilder.uriBuilderFrom(node.getInternalUri());
            Request request = prepareDelete()
                    .setUri(httpUriBuilder.appendPath(path).build())
                    .build();
            return httpClient.execute(request, JSON_RESPONSE_HANDLER);
        }
        catch (Exception e) {
            log.error(e, "create catalog failed: %s", e.getMessage());
            return Resp.status(Response.Status.GATEWAY_TIMEOUT.getStatusCode(), e.getMessage());
        }
    }

    private List<InternalNode> getNodes(boolean coordinator)
    {
        AllNodes allNodes = nodeManager.getAllNodes();

        List<InternalNode> result = new ArrayList<>();
        for (InternalNode node : allNodes.getActiveNodes()) {
            if (node.isCoordinator() == coordinator) {
                result.add(node);
            }
        }
        for (InternalNode node : allNodes.getInactiveNodes()) {
            if (node.isCoordinator() == coordinator) {
                result.add(node);
            }
        }
        for (InternalNode node : allNodes.getShuttingDownNodes()) {
            if (node.isCoordinator() == coordinator) {
                result.add(node);
            }
        }
        return result;
    }

    private boolean isFail(int status)
    {
        return status / 10 != 20;
    }

    public static class Resp
    {
        private String message;
        private int status;

        @JsonCreator
        public Resp(
                @JsonProperty("status") int status,
                @JsonProperty("message") String message)
        {
            this.message = message;
            this.status = status;
        }

        public static Resp status(int status, String message)
        {
            return new Resp(status, message);
        }

        @JsonProperty
        public String getMessage() {
            return message;
        }

        @JsonProperty
        public int getStatus() {
            return status;
        }

        public Response toResponse() {
            return Response.status(status, message).build();
        }
    }
}
