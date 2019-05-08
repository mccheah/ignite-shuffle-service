/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
 *
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

package com.palantir.ignite;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.net.URI;
import java.nio.file.Path;
import org.immutables.value.Value;

@Value.Immutable
@ImmutablesStyle
@JsonDeserialize(as = ImmutableIgniteRuntimeConfig.class)
public interface IgniteRuntimeConfig {

    @JsonProperty("data-persistence-path")
    Path dataPersistencePath();

    @JsonProperty("work-path")
    Path workPath();

    @JsonProperty("kubernetes-master-url")
    URI kubernetesMasterUrl();

    @JsonProperty("kubernetes-service-name")
    String kubernetesServiceName();

    @JsonProperty("kubernetes-namespace")
    String kubernetesNamespace();
}
