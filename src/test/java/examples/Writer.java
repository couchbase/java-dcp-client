package examples;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

import java.util.UUID;

public class Writer {

    public static void main(String... args) throws Exception {
        CouchbaseCluster cluster = CouchbaseCluster.create("10.142.150.101");
        Bucket bucket = cluster.openBucket("default");

        while(true) {
            for (int i = 0; i < 1024; i++) {
                System.err.println("IN");
                bucket.upsert(JsonDocument.create("doc:"+i, JsonObject.create().put("uuid", UUID.randomUUID().toString())));
                Thread.sleep(1000);
            }
        }
    }
}
