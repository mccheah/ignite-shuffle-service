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

import com.google.common.collect.ImmutableList;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public final class IgniteClientTest {

    private static final Logger log = LoggerFactory.getLogger(IgniteClientTest.class);

    public static void main(String[] args) {
        try {
            JavaSparkContext context = new JavaSparkContext(SparkContext.getOrCreate());
            log.info("Got the following result: {}",
                    context.parallelizePairs(
                    ImmutableList.of(
                            new Tuple2<>(5, 10),
                            new Tuple2<>(5, 15),
                            new Tuple2<>(1, 23),
                            new Tuple2<>(4, 13),
                            new Tuple2<>(4, 12)))
                    .groupByKey()
                    .collectAsMap());
        } catch (Exception e) {
            log.error("Got exception in main thread. Keeping pod alive to investigate.", e);
        }
    }
}
