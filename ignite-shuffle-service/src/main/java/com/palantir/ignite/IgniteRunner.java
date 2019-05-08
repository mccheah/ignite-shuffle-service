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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Paths;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.kubernetes.TcpDiscoveryKubernetesIpFinder;

public final class IgniteRunner {

    private IgniteRunner() {}

    public static void main(String[] args) throws IOException  {
        try {
            IgniteRuntimeConfig current = new ObjectMapper(new YAMLFactory()).readValue(
                    Paths.get("var", "conf", "runtime.yml").toFile(),
                    IgniteRuntimeConfig.class);
            TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
            TcpDiscoveryKubernetesIpFinder ipFinder = new TcpDiscoveryKubernetesIpFinder();
            ipFinder.setMasterUrl(current.kubernetesMasterUrl().toString());
            ipFinder.setNamespace(current.kubernetesNamespace());
            ipFinder.setServiceName(current.kubernetesServiceName());
            discoverySpi.setIpFinder(ipFinder);
            IgniteConfiguration igniteConfig = new IgniteConfiguration()
                    .setWorkDirectory(current.workPath().toFile().getAbsolutePath())
                    .setDiscoverySpi(discoverySpi)
                    .setDataStorageConfiguration(new DataStorageConfiguration()
                            .setStoragePath(current.dataPersistencePath().toFile().getAbsolutePath()));
            Ignition.start(igniteConfig);
            boolean interrupted = false;
            while (!interrupted) {
                try {
                    Thread.sleep(Integer.MAX_VALUE);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
