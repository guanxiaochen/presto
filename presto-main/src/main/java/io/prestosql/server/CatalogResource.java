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
package io.prestosql.server;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.net.HttpHeaders;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.ResponseHandler;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.prestosql.connector.CatalogName;
import io.prestosql.connector.ConnectorManager;
import io.prestosql.metadata.AllNodes;
import io.prestosql.metadata.Catalog;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.ForNodeManager;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.metadata.NodeState;
import io.prestosql.server.security.ResourceSecurity;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
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
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static io.airlift.http.client.HttpStatus.familyForStatusCode;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.prestosql.server.security.ResourceSecurity.AccessType.PUBLIC;
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
    private final ConnectorManager connectorManager;
    private final CatalogManager catalogManager;
    private final InternalNodeManager nodeManager;
    private final HttpClient httpClient;

    @Inject
    public CatalogResource(
            ConnectorManager connectorManager,
            CatalogManager catalogManager,
            InternalNodeManager nodeManager,
            @ForNodeManager HttpClient httpClient)
    {
        this.connectorManager = requireNonNull(connectorManager, "connectorManager is null");
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
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
    public Response create(CatalogInfo catalogInfo)
    {
        if (!nodeManager.getCurrentNode().isCoordinator()) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        requireNonNull(catalogInfo, "catalogInfo is null");
        for (InternalNode node : getNodes(false)) {
            doNodeCreate(node, catalogInfo);
        }
        for (InternalNode node : getNodes(true)) {
            doNodeCreate(node, catalogInfo);
        }
        return Response.status(Response.Status.OK).build();
    }

    @ResourceSecurity(PUBLIC)
    @PUT
    @Path("node")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response nodeCreate(CatalogInfo catalogInfo)
    {
        requireNonNull(catalogInfo, "catalogInfo is null");
        if (catalogManager.getCatalog(catalogInfo.getCatalogName()).isPresent()) {
            return Response.status(Response.Status.OK).build();
        }

        CatalogName connectorId = connectorManager.createCatalog(
                catalogInfo.getCatalogName(),
                catalogInfo.getConnectorName(),
                catalogInfo.getProperties());

        //updateConnectorIdAnnouncement(announcer, connectorId);
        return Response.status(Response.Status.OK).build();
    }

    @ResourceSecurity(PUBLIC)
    @DELETE
    @Path("{catalog}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response delete(@PathParam("catalog") String catalog)
    {
        if (!nodeManager.getCurrentNode().isCoordinator()) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        requireNonNull(catalog, "catalogInfo is null");
        for (InternalNode node : getNodes(false)) {
            doNodeDelete(node, catalog);
        }
        for (InternalNode node : getNodes(true)) {
            doNodeDelete(node, catalog);
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
        connectorManager.dropConnection(catalog);
        return Response.status(Response.Status.OK).build();
    }

    private static void updateConnectorIdAnnouncement(Announcer announcer, CatalogName connectorId)
    {
        //
        // This code was copied from PrestoServer, and is a hack that should be removed when the connectorId property is removed
        //

        // get existing announcement
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());

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

    private static ServiceAnnouncement getPrestoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("presto")) {
                return announcement;
            }
        }
        throw new RuntimeException("Presto announcement not found: " + announcements);
    }

    private void doNodeCreate(InternalNode node, CatalogInfo catalogInfo)
    {
        try {
            String path = "/v1/catalog/node";
            HttpUriBuilder httpUriBuilder = HttpUriBuilder.uriBuilderFrom(node.getInternalUri());
            Request request = preparePut()
                    .setUri(httpUriBuilder.appendPath(path).build())
                    .setHeader(HttpHeaders.CONTENT_TYPE, com.google.common.net.MediaType.JSON_UTF_8.toString())
                    .setBodyGenerator(createStaticBodyGenerator(CATALOG_INFO_JSON_CODEC.toJsonBytes(catalogInfo)))
                    .build();
            httpClient.execute(request, new ResponseHandler<Void, RuntimeException>() {
                @Override
                public Void handleException(Request request, Exception exception)
                {
                    log.debug(exception, "request failed: %s", request.getUri());
                    return null;
                }

                @Override
                public Void handle(Request request, io.airlift.http.client.Response response)
                {
                    if (familyForStatusCode(response.getStatusCode()) != HttpStatus.Family.SUCCESSFUL) {
                        log.debug("Unexpected response code: %s", response.getStatusCode());
                    }
                    return null;
                }
            });
        }
        catch (Exception e) {
            log.error(e, "create catalog failed: %s", e.getMessage());
        }
    }

    private void doNodeDelete(InternalNode node, String catalog)
    {
        try {
            String path = "/v1/catalog/node/" + catalog;
            HttpUriBuilder httpUriBuilder = HttpUriBuilder.uriBuilderFrom(node.getInternalUri());
            Request request = prepareDelete()
                    .setUri(httpUriBuilder.appendPath(path).build())
                    .build();
            httpClient.execute(request, new ResponseHandler<Void, RuntimeException>()
            {
                @Override
                public Void handleException(Request request, Exception exception)
                {
                    log.debug(exception, "request failed: %s", request.getUri());
                    return null;
                }

                @Override
                public Void handle(Request request, io.airlift.http.client.Response response)
                {
                    if (familyForStatusCode(response.getStatusCode()) != HttpStatus.Family.SUCCESSFUL) {
                        log.debug("Unexpected response code: %s", response.getStatusCode());
                    }
                    return null;
                }
            });
        }
        catch (Exception e) {
            log.error(e, "create catalog failed: %s", e.getMessage());
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
}
