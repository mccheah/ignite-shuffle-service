/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.ignite.spark.shuffle.io;

import com.palantir.ignite.SparkShufflePartition;
import com.palantir.ignite.SparkShufflePartitionBlock;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.kubernetes.TcpDiscoveryKubernetesIpFinder;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.api.ShuffleDataIO;
import org.apache.spark.shuffle.api.ShuffleReadSupport;
import org.apache.spark.shuffle.api.ShuffleWriteSupport;
import org.apache.spark.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function0;
import scala.compat.java8.functionConverterImpls.FromJavaSupplier;
import scala.runtime.BoxedUnit;

public final class IgniteShuffleIO implements ShuffleDataIO {

    private static final Logger log = LoggerFactory.getLogger(IgniteShuffleIO.class);

    private final SparkConf sparkConf;
    private Ignite ignite;
    private IgniteCache<SparkShufflePartitionBlock, byte[]> dataCache;
    private IgniteCache<SparkShufflePartition, Long> metadataCache;
    private int blockSize = 256000;

    public IgniteShuffleIO(SparkConf sparkConf) {
        this.sparkConf = sparkConf;
    }

    @Override
    public void initialize() {
        String master = sparkConf.get("spark.master");
        if (!master.startsWith("k8s://")) {
            throw new IllegalArgumentException("Ignite shuffle only supported in Kubernetes.");
        }
        boolean wasSparkSubmittedInClusterMode = sparkConf.getBoolean("spark.kubernetes.submitInDriver", false);
        String k8sMaster;
        if (wasSparkSubmittedInClusterMode) {
            k8sMaster = "https://kubernetes.default.svc";
        } else {
            k8sMaster = master.replaceFirst("k8s://", "");
        }
        if (!k8sMaster.startsWith("http://") && !k8sMaster.startsWith("https://")) {
            k8sMaster = "k8s://" + k8sMaster;
        }

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        TcpDiscoveryKubernetesIpFinder ipFinder = new TcpDiscoveryKubernetesIpFinder();
        ipFinder.setMasterUrl(k8sMaster);
        ipFinder.setNamespace(sparkConf.getOption("spark.shuffle.ignite.kubernetes.namespace").getOrElse(
                asScala(() -> {
                    throw new IllegalArgumentException(
                            "Namespace for the Ignite cluster must be specified.");
                })));
        ipFinder.setServiceName(sparkConf.getOption("spark.shuffle.ignite.kubernetes.service.name").getOrElse(
                asScala(() -> {
                    throw new IllegalArgumentException(
                            "Name of the Ignite cluster's service must be specified.");
                })));
        discoverySpi.setIpFinder(ipFinder);
        String dataCacheName = sparkConf.getOption("spark.shuffle.ignite.data.cache.name").getOrElse(
                asScala(() -> String.format("spark-ignite-data-cache-%s", sparkConf.getAppId())));
        String metaCacheName = sparkConf.getOption("spark.shuffle.ignite.metadata.cache.name").getOrElse(
                asScala(() -> String.format("spark-ignite-metadata-cache-%s", sparkConf.getAppId())));
        File tempWorkDir;
        try {
            tempWorkDir = Files.createTempDirectory("ignite-shuffle-work").toFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        tempWorkDir.deleteOnExit();

        IgniteConfiguration igniteConfig = new IgniteConfiguration()
                .setDiscoverySpi(discoverySpi)
                .setWorkDirectory(tempWorkDir.getAbsolutePath())
                .setClientMode(true);
        ignite = Ignition.start(igniteConfig);
        ShutdownHookManager.addShutdownHook(asScala(() -> {
            if (metadataCache != null) {
                try {
                    metadataCache.destroy();
                } catch (Exception e) {
                    log.warn("Failed to destroy the metadata cache.", e);
                }
            }
            if (dataCache != null) {
                try {
                    dataCache.destroy();
                } catch (Exception e) {
                    log.warn("Failed to destroy the shuffle blocks data cache.", e);
                }
            }
            ignite.close();
            return BoxedUnit.UNIT;
        }));
        dataCache = ignite.getOrCreateCache(
                new CacheConfiguration<SparkShufflePartitionBlock, byte[]>()
                        .setName(dataCacheName)
                        .setCacheMode(CacheMode.PARTITIONED)
                        .setBackups(2));
        metadataCache = ignite.getOrCreateCache(
                new CacheConfiguration<SparkShufflePartition, Long>()
                        .setName(metaCacheName)
                        .setCacheMode(CacheMode.PARTITIONED)
                        .setBackups(2));
        blockSize = sparkConf.getInt("spark.shuffle.ignite.blockSize", 256000);
    }

    @Override
    public ShuffleReadSupport readSupport() {
        return new IgniteReadSupport(dataCache, metadataCache);
    }

    @Override
    public ShuffleWriteSupport writeSupport() {
        return new IgniteWriteSupport(dataCache, metadataCache, blockSize);
    }

    private static <T> Function0<T> asScala(Supplier<T> value) {
        return new FromJavaSupplier<>(value);
    }
}
