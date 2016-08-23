package com.couchbase.client.dcp.util;

import com.couchbase.client.core.message.config.RestApiResponse;
import com.couchbase.client.core.utils.Base64;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.api.Form;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.HttpWaitStrategy;
import org.testcontainers.containers.wait.WaitStrategy;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

public class CouchbaseContainer<SELF extends CouchbaseContainer<SELF>> extends GenericContainer<SELF> {

    public static final String DEFAULT_CB_VERSION = "4.5.0";

    public CouchbaseContainer() {
        super("couchbase:" + DEFAULT_CB_VERSION);
    }

    public CouchbaseContainer(String imageName) {
        super(imageName);
    }

    @Override
    protected Integer getLivenessCheckPort() {
        return getMappedPort(8091);
    }

    @Override
    protected void configure() {
        addExposedPorts(8091, 8092, 8093, 8094, 11210, 11211, 11207, 18091, 18092, 18093);
    }

    @Override
    protected void waitUntilContainerStarted() {
        WaitStrategy initWait = new HttpWaitStrategy().forPath("/ui/index.html#/");
        initWait.waitUntilReady(this);
/*
        try {
            String urlBase = String.format("http://%s:%s", getContainerIpAddress(), getMappedPort(8091));

            String poolURL = urlBase + "/pools/default";
            String poolPayload = "memoryQuota=" + URLEncoder.encode(memoryQuota, "UTF-8") + "&indexMemoryQuota=" + URLEncoder.encode(indexMemoryQuota, "UTF-8");

            String setupServicesURL = urlBase + "/node/controller/setupServices";
            StringBuilder servicePayloadBuilder= new StringBuilder();
            if (keyValue) {
                servicePayloadBuilder.append("kv,");
            }
            if (query) {
                servicePayloadBuilder.append("n1ql,");
            }
            if (index) {
                servicePayloadBuilder.append("index,");
            }
            if (fts) {
                servicePayloadBuilder.append("fts,");
            }
            String setupServiceContent = "services=" + URLEncoder.encode(servicePayloadBuilder.toString(), "UTF-8");

            String webSettingsURL = urlBase + "/settings/web";
            String webSettingsContent = "username=" + URLEncoder.encode(clusterUsername, "UTF-8") + "&password=" + URLEncoder.encode(clusterPassword, "UTF-8") + "&port=8091";

            String bucketURL = urlBase + "/sampleBuckets/install";

            StringBuilder sampleBucketPayloadBuilder= new StringBuilder();
            sampleBucketPayloadBuilder.append('[');
            if (travelSample) {
                sampleBucketPayloadBuilder.append("\"travel-sample\",");
            }
            if (beerSample) {
                sampleBucketPayloadBuilder.append("\"beer-sample\",");
            }
            if (gamesIMSample) {
                sampleBucketPayloadBuilder.append("\"gamesim-sample\",");
            }
            sampleBucketPayloadBuilder.append(']');

            callCouchbaseRestAPI(poolURL, poolPayload);
            callCouchbaseRestAPI(setupServicesURL, setupServiceContent);
            callCouchbaseRestAPI(webSettingsURL, webSettingsContent);
            callCouchbaseRestAPI(bucketURL, sampleBucketPayloadBuilder.toString(), clusterUsername, clusterPassword);

            CouchbaseWaitStrategy s = new CouchbaseWaitStrategy();
            s.withBasicCredentials("Administrator", "password");
            s.waitUntilReady(this);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        */
    }

    public void callCouchbaseRestAPI(String url, String content) throws IOException {
        callCouchbaseRestAPI(url, content, null, null);
    }

    public void callCouchbaseRestAPI(String url, String payload, String username, String password) throws IOException {
        HttpURLConnection httpConnection = (HttpURLConnection) ((new URL(url).openConnection()));
        httpConnection.setDoOutput(true);
        httpConnection.setDoInput(true);
        httpConnection.setRequestMethod("POST");
        httpConnection.setRequestProperty("Content-Type",
            "application/x-www-form-urlencoded");
        if (username != null) {
            String encoded = Base64.encode((username + ":" + password).getBytes("UTF-8"));
            httpConnection.setRequestProperty("Authorization", "Basic "+encoded);
        }
        DataOutputStream out = new DataOutputStream(httpConnection.getOutputStream());
        out.writeBytes(payload);
        out.flush();
        out.close();
        httpConnection.getResponseCode();
        httpConnection.disconnect();
    }
}
