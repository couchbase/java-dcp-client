/*
 * Copyright 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.dcp.test.agent;

import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.manager.bucket.BucketManager;
import com.couchbase.client.java.manager.bucket.BucketSettings;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.java.diagnostics.WaitUntilReadyOptions.waitUntilReadyOptions;
import static java.util.Objects.requireNonNull;

public class BucketServiceImpl implements BucketService {

  private final ClusterSupplier clusterSupplier;

  public BucketServiceImpl(ClusterSupplier clusterSupplier) {
    this.clusterSupplier = requireNonNull(clusterSupplier);
  }

  private BucketManager bucketManager() {
    return clusterSupplier.get().buckets();
  }

  @Override
  public Set<String> list() {
    return bucketManager()
        .getAllBuckets()
        .keySet();
  }

  @Override
  public void create(String bucket, int quotaMb, int replicas, boolean enableFlush) {
    if (bucketManager().getAllBuckets().containsKey(bucket)) {
      return;
    }

    while (true) {
      try {
        BucketSettings settings = BucketSettings.create(bucket)
            .flushEnabled(true)
            .ramQuotaMB(quotaMb)
            .numReplicas(replicas);
        bucketManager().createBucket(settings);
        TimeUnit.SECONDS.sleep(3);

        // instead of sleeping?
        clusterSupplier.get().bucket(bucket)
            .waitUntilReady(
                Duration.ofSeconds(10),
                waitUntilReadyOptions()
                    .serviceTypes(ServiceType.KV)
            );
        break;

      } catch (Exception e) {
        if (e.getMessage().contains("Cannot create buckets during rebalance")) {
          try {
            TimeUnit.SECONDS.sleep(2);
            continue;
          } catch (InterruptedException interrupted) {
            throw new RuntimeException(interrupted);
          }
        }
        throw new RuntimeException("could not create bucket " + bucket + "; " + e.getMessage(), e);
      }
    }
  }

  @Override
  public void delete(String bucket) {
    if (!bucketManager().getAllBuckets().containsKey(bucket)) {
      return;
    }

    try {
      bucketManager().dropBucket(bucket);
    } catch (BucketNotFoundException ignore) {
      // that's fine
    }

    /*
     * TODO(amoudi): per DCP team this hasBucket call is insufficient to determine complete removal
     * For now, we will always wait 3 seconds. later, we can do something more involved.
     * Basically, we need to attempt to open the bucket and get an authentication failure
     */
    try {
      TimeUnit.SECONDS.sleep(3);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
