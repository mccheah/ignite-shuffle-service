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

package com.palantir.ignite;

import java.io.Serializable;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;

public final class SparkShufflePartitionBlock implements Serializable {

    private static final long serialVersionUID = 1L;

    @AffinityKeyMapped
    private final SparkShufflePartition partition;

    private final long blockNumber;

    private SparkShufflePartitionBlock(SparkShufflePartition partition, long blockNumber) {
        this.partition = partition;
        this.blockNumber = blockNumber;
    }

    public SparkShufflePartition partitionId() {
        return partition;
    }

    public long blockNumber() {
        return blockNumber;
    }

    public static SparkShufflePartitionBlock of(SparkShufflePartition partition, long blockNumber) {
        return new SparkShufflePartitionBlock(partition, blockNumber);
    }
}
