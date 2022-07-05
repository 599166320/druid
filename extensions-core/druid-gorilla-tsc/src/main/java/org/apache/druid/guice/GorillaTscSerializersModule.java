/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.druid.guice;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.query.aggregation.*;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.tsg.TSG;

public class GorillaTscSerializersModule extends SimpleModule
{
    public GorillaTscSerializersModule()
    {
        registerSubtypes(
                new NamedType(GorillaTscAggregatorFactory.class, GorillaTscAggregatorFactory.TYPE_NAME),
                new NamedType(GorillaTscPostAggregator.class, GorillaTscPostAggregator.TYPE_NAME),
                new NamedType(GorillaTscQueryAggregatorFactory.class, GorillaTscQueryAggregatorFactory.TYPE_NAME)
        );
        addSerializer(TSG.class, new GorillaTscJsonSerializer());
        addDeserializer(TSG.class, new TSGTscJsonDeserializer());
        registerSerde();
    }

    @VisibleForTesting
    public static void registerSerde()
    {
        ComplexMetrics.registerSerde(GorillaTscAggregatorFactory.TYPE_NAME, new GorillaTscComplexMetricSerde());
        ComplexMetrics.registerSerde(GorillaTscQueryAggregatorFactory.TYPE_NAME, new GorillaTscQueryComplexMetricSerde());
    }


}
