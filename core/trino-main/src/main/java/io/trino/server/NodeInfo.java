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
import io.trino.metadata.NodeState;

import javax.annotation.concurrent.Immutable;

import static java.util.Objects.requireNonNull;

@Immutable
public class NodeInfo
{
    private final String identifier;
    private final String uri;
    private final String version;
    private final boolean coordinator;
    private final NodeState nodeState;

    @JsonCreator
    public NodeInfo(
            @JsonProperty("identifier") String identifier,
            @JsonProperty("uri") String uri,
            @JsonProperty("version") String version,
            @JsonProperty("coordinator") boolean coordinator,
            @JsonProperty("nodeState") NodeState nodeState)
    {
        this.identifier = requireNonNull(identifier, "identifier is null");
        this.uri = requireNonNull(uri, "uri is null");
        this.version = requireNonNull(version, "version is null");
        this.coordinator = coordinator;
        this.nodeState = requireNonNull(nodeState, "nodeState is null");
    }

    @JsonProperty
    public String getIdentifier()
    {
        return identifier;
    }

    @JsonProperty
    public String getUri()
    {
        return uri;
    }

    @JsonProperty
    public String getVersion()
    {
        return version;
    }

    @JsonProperty
    public boolean isCoordinator()
    {
        return coordinator;
    }

    @JsonProperty
    public NodeState getNodeState()
    {
        return nodeState;
    }
}
