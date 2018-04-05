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

package com.couchbase.client.dcp.test;

import com.couchbase.client.dcp.test.ExecUtils.ExecResultWithExitCode;
import com.couchbase.client.dcp.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.shaded.com.google.common.base.Stopwatch;

import java.io.IOException;
import java.util.Optional;

import static com.couchbase.client.dcp.test.ExecUtils.exec;
import static com.couchbase.client.dcp.test.ExecUtils.execOrDie;
import static java.util.Objects.requireNonNull;

public class CouchbaseContainer extends GenericContainer<CouchbaseContainer> {
    private static final Logger log = LoggerFactory.getLogger(CouchbaseContainer.class);

    private final String username;
    private final String password;
    private final String hostname;

    public CouchbaseContainer(String dockerImageName, String hostname, String username, String password) {
        super(dockerImageName);
        this.username = requireNonNull(username);
        this.password = requireNonNull(password);
        this.hostname = requireNonNull(hostname);

        withNetworkAliases(hostname);
        withCreateContainerCmdModifier(cmd -> cmd.withHostName(hostname));
    }

    public CouchbaseContainer initCluster(int ramMb) throws IOException, InterruptedException {
        execOrDie(this, "couchbase-cli cluster-init" +
                " --cluster " + getHostname() +
                " --cluster-username=" + username +
                " --cluster-password=" + password +
//                " --services=data,query,index" +
//                " --cluster-index-ramsize=512" +
                " --cluster-ramsize=" + ramMb);
        return this;
    }

    public CouchbaseContainer loadSampleBucket(String bucketName, int bucketQuotaMb) throws IOException, InterruptedException {
        Stopwatch timer = Stopwatch.createStarted();

        ExecResultWithExitCode result = exec(this, "cbdocloader" +
                " --cluster " + getHostname() + // + ":8091" +
                " --username " + username +
                " --password " + password +
                " --bucket " + bucketName +
                " --bucket-quota " + bucketQuotaMb +
                " --dataset ./opt/couchbase/samples/" + bucketName + ".zip");

        // Query and index services must be present to avoid this warning. We don't need those services.
        if (result.getExitCode() != 0 && !result.getStdout().contains("Errors occurred during the index creation phase")) {
            throw new IOException("Failed to load sample bucket: " + result);
        }

        log.info("Importing sample bucket took {}", timer);
        return this;

        // cbimport is faster, but isn't always available, and fails when query & index services are not absent
//        Stopwatch timer = Stopwatch.createStarted();
//        createBucket(bucketName, bucketQuotaMb);
//        exec(this, "cbimport2 json " +
//                " --cluster couchbase://" + getHostname() +
//                " --username " + username +
//                " --password " + password +
//                " --bucket " + bucketName +
//                " --format sample" +
//                " --dataset ./opt/couchbase/samples/beer-sample.zip");
//        log.info("Importing sample bucket with cbimport took " + timer);
//        return this;
    }

    public CouchbaseContainer createBucket(String bucketName, int bucketQuotaMb) throws IOException, InterruptedException {
        Stopwatch timer = Stopwatch.createStarted();

        execOrDie(this, "couchbase-cli bucket-create" +
                " --cluster " + getHostname() +
                " --username " + username +
                " --password " + password +
                " --bucket " + bucketName +
                " --bucket-ramsize " + bucketQuotaMb + "" +
                " --bucket-type couchbase " +
                //" --bucket-replica " + replicas +
                " --wait");
        log.info("Creating bucket took " + timer);
        return this;
    }

    public Optional<Version> getVersion() throws IOException, InterruptedException {
        ExecResultWithExitCode execResult = exec(this, "couchbase-server --version");
        if (execResult.getExitCode() != 0) {
            return getVersionFromDockerImageName();
        }
        Optional<Version> result = tryParseVersion(execResult.getStdout().trim());
        return result.isPresent() ? result : getVersionFromDockerImageName();
    }

    private static int indexOfFirstDigit(final String s) {
        for (int i = 0; i < s.length(); i++) {
            if (Character.isDigit(s.charAt(i))) {
                return i;
            }
        }
        return -1;
    }

    private static Optional<Version> tryParseVersion(final String versionString) {
        try {
            // We get a string like "Couchbase Server 5.5.0-2036 (EE)". The version parser
            // tolerates trailing garbage, but not leading garbage, so...
            final int actualStartIndex = indexOfFirstDigit(versionString);
            if (actualStartIndex == -1) {
                return Optional.empty();
            }
            final String versionWithoutLeadingGarbage = versionString.substring(actualStartIndex);
            final Version version = Version.parseVersion(versionWithoutLeadingGarbage);
            // builds off master branch might have version 0.0.0 :-(
            return version.major() == 0 ? Optional.empty() : Optional.of(version);
        } catch (Exception e) {
            log.warn("Failed to parse version string '{}'", versionString, e);
            return Optional.empty();
        }
    }

    private Optional<Version> getVersionFromDockerImageName() {
        final String imageName = getDockerImageName();
        final int tagDelimiterIndex = imageName.indexOf(':');
        return tagDelimiterIndex == -1 ? Optional.empty() : tryParseVersion(imageName.substring(tagDelimiterIndex + 1));
    }

    private String getHostname() {
        return hostname;
    }
}
