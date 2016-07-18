package com.couchbase.client.dcp.conductor;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import rx.Completable;
import rx.Observable;

public interface ConfigProvider {

    Completable start();

    Completable stop();

    Observable<CouchbaseBucketConfig> configs();

}
