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
import org.testcontainers.containers.Container;

import java.io.IOException;

/**
 * Helper methods that check the exit code of exec'd processes.
 * Inspired by https://stackoverflow.com/a/46805652/611819
 */
public class ExecUtils {
    public static class ExecResultWithExitCode extends Container.ExecResult {
        private final int exitCode;

        public ExecResultWithExitCode(String stdout, String stderr, int exitCode) {
            super(stdout, stderr);
            this.exitCode = exitCode;
        }

        public int getExitCode() {
            return exitCode;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (!getStdout().isEmpty()) {
                sb.append("stdout: ").append(getStdout());
            }
            if (!getStderr().isEmpty()) {
                sb.append("stderr: ").append(getStderr());
            }
            if (exitCode != 0) {
                sb.append("exit code: ").append(exitCode);
            }
            return sb.toString().trim();
        }
    }

    private ExecUtils() {
        throw new AssertionError("not instantiable");
    }

    public static ExecResultWithExitCode execOrDie(Container container, String shellCommand) throws IOException, InterruptedException {
        ExecResultWithExitCode result = exec(container, shellCommand);
        if (result.exitCode != 0) {
            throw new IOException(result.toString());
        }
        return result;
    }

    public static ExecResultWithExitCode exec(Container container, String shellCommand) throws IOException, InterruptedException {
        Logger log = LoggerFactory.getLogger("[" + container.getContainerName() + "]");
        log.info("Executing command: {}", shellCommand);

        String exitCodeMarker = "ExitCode=";
        Container.ExecResult result = container.execInContainer(
                "sh", "-c", shellCommand + "; echo \"" + exitCodeMarker + "$?\""
        );
        String stdout = result.getStdout();
        int i = stdout.lastIndexOf(exitCodeMarker);
        if (i < 0) {
            throw new RuntimeException("failed to determine exit code: " + result.getStdout() + "/" + result.getStderr());
        }
        int exitCode = Integer.parseInt(stdout.substring(i + exitCodeMarker.length()).trim());
        ExecResultWithExitCode resultWithExitCode = new ExecResultWithExitCode(stdout.substring(0, i), result.getStderr(), exitCode);
        if (resultWithExitCode.exitCode != 0) {
            log.warn("{}", resultWithExitCode);
        } else {
            log.info("{}", resultWithExitCode);
        }
        return resultWithExitCode;
    }
}
