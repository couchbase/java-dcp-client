/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.dcp.transport.netty;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelInitializer;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpClientCodec;
import rx.subjects.Subject;

public class ConfigPipeline extends ChannelInitializer<Channel> {

    private final String hostname;
    private final String bucket;
    private final String password;
    private final Subject<CouchbaseBucketConfig, CouchbaseBucketConfig> configStream;


    public ConfigPipeline(String hostname, String bucket, String password,
        Subject<CouchbaseBucketConfig, CouchbaseBucketConfig> configStream) {
        this.hostname = hostname;
        this.bucket = bucket;
        this.password = password;
        this.configStream = configStream;
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
        ch.pipeline()
            .addLast(new HttpClientCodec())
            .addLast(new ConfigHandler(hostname, bucket, password, configStream));
    }

}
