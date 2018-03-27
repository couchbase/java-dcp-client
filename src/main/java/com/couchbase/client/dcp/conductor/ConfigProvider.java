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
package com.couchbase.client.dcp.conductor;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.core.state.Stateful;
import rx.Completable;
import rx.Observable;

/**
 * Describes the contract for a class that provides Couchbase Server configurations.
 *
 * Note that it is assumed by contract that the configuration provider is "cluster aware" and
 * tries until stopped to find a new node to grab a configuration from if the current source
 * is not available anymore.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public interface ConfigProvider extends Stateful<LifecycleState> {

    /**
     * Asynchronously starts the configuration provider.
     */
    Completable start();

    /**
     * Asynchronously stops the configuration provider.
     */
    Completable stop();

    /**
     * Returns an {@link Observable} which emits a new config every time it arrives from
     * the server side. The revision number of each emitted config is guaranteed to be greater
     * than the revision number of the previously emitted config.
     */
    Observable<CouchbaseBucketConfig> configs();

}
