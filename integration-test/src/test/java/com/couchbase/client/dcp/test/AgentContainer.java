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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

/**
 * Container that runs the DCP Test Agent webapp. The integration tests
 * communicate with using JSON-RPC over HTTP.
 *
 * @see RemoteAgent
 */
public class AgentContainer extends GenericContainer<AgentContainer> {
    private static final Logger log = LoggerFactory.getLogger(AgentContainer.class);

    private static final int HTTP_PORT = 8080;
    private static final int DEBUG_PORT = 5005;

    public AgentContainer(File agentJar) throws FileNotFoundException {
        super(createImage(agentJar));

        if (debuggerPresent()) {
            withExposedPorts(HTTP_PORT, DEBUG_PORT);
        } else {
            withExposedPorts(HTTP_PORT);
        }
    }

    public int getHttpPort() {
        return getMappedPort(HTTP_PORT);
    }

    public int getDebuggerPort() {
        return getMappedPort(DEBUG_PORT);
    }

    @Override
    public void start() {
        super.start();

        log.info("DCP Test Agent {} running at http://localhost:{}",
                getContainerName(), getHttpPort());

        if (debuggerPresent()) {
            log.info("DCP Test Agent listening for debugger on port {}", getDebuggerPort());
        }
    }

    private static Future<String> createImage(File appFile) throws FileNotFoundException {
        if (!appFile.exists()) {
            throw new FileNotFoundException(appFile.getAbsolutePath());
        }

        final List<String> command = new ArrayList<>(Arrays.asList("java", "-Djava.security.egd=file:/dev/./urandom", "-jar", "/app.jar"));
        if (debuggerPresent()) {
            // The integration tests are running in debug mode, so launch the agent in debug mode too.
            command.add(1, "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=" + DEBUG_PORT);
        }
        final String[] entryPoint = command.toArray(new String[0]);

        return new ImageFromDockerfile("dcp-test-agent", true)
                .withDockerfileFromBuilder(dockerfileBuilder -> dockerfileBuilder
                        .from("openjdk:8-jdk-alpine")
                        .volume("/tmp")
                        .add("/app.jar", "app.jar")
                        .entryPoint(entryPoint))
                .withFileFromFile("/app.jar", appFile);
    }

    /**
     * Returns true if and only if the JVM was started with debugging enabled.
     */
    private static boolean debuggerPresent() {
        for (String arg : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
            if (arg.contains("-agentlib:jdwp")) {
                return true;
            }
        }
        return false;
    }
}
