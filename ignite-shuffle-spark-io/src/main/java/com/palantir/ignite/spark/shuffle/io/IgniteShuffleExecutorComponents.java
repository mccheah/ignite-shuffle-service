/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.ignite.spark.shuffle.io;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.ignite.SparkShufflePartition;
import com.palantir.ignite.SparkShufflePartitionBlock;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.kubernetes.TcpDiscoveryKubernetesIpFinder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.shuffle.ShuffleExecutorComponents;
import org.apache.spark.api.shuffle.ShuffleReadSupport;
import org.apache.spark.api.shuffle.ShuffleWriteSupport;
import scala.Function0;
import scala.compat.java8.functionConverterImpls.FromJavaSupplier;

public final class IgniteShuffleExecutorComponents implements ShuffleExecutorComponents {

    private final SparkConf sparkConf;
    private Ignite ignite;
    private IgniteCache<SparkShufflePartitionBlock, byte[]> dataCache;
    private IgniteCache<SparkShufflePartition, Long> metadataCache;
    private String dataCacheName;
    private int blockSize = 256000;
    private int numBlocksPerBatch = 100;
    private int numBatchesInFlight = 10;
    private String appId;

    public IgniteShuffleExecutorComponents(SparkConf sparkConf) {
        this.sparkConf = sparkConf;
    }

    @Override
    public void initializeExecutor(String appId, String execId) {
        this.appId = appId;
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
        dataCacheName = sparkConf.getOption("spark.shuffle.ignite.data.cache.name").getOrElse(
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
        dataCache = ignite.getOrCreateCache(
                new CacheConfiguration<SparkShufflePartitionBlock, byte[]>()
                        .setName(dataCacheName)
                        .setCacheMode(CacheMode.PARTITIONED)
                        .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                        .setBackups(2));
        metadataCache = ignite.getOrCreateCache(
                new CacheConfiguration<SparkShufflePartition, Long>()
                        .setName(metaCacheName)
                        .setCacheMode(CacheMode.PARTITIONED)
                        .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                        .setBackups(2));
        blockSize = sparkConf.getInt("spark.shuffle.ignite.blockSize", 256000);
    }

    @Override
    public ShuffleWriteSupport writes() {
        return new IgniteWriteSupport(
                dataCache,
                metadataCache,
                () -> ignite.dataStreamer(dataCache.getName()),
                () -> ignite.dataStreamer(metadataCache.getName()),
                blockSize,
                appId);
    }

    @Override
    public ShuffleReadSupport reads() {
        return new IgniteReadSupport(
                dataCache,
                metadataCache,
                Executors.newCachedThreadPool(
                        new ThreadFactoryBuilder().setNameFormat("ignite-block-fetcher").setDaemon(true).build()),
                appId,
                numBlocksPerBatch,
                numBatchesInFlight);
    }

    private static <T> Function0<T> asScala(Supplier<T> value) {
        return new FromJavaSupplier<>(value);
    }
}
