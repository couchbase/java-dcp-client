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

package com.couchbase.testcontainers.custom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;

/**
 * Helper methods that check the exit code of exec'd processes.
 * Inspired by https://stackoverflow.com/a/46805652/611819
 */
public class ExecUtils {

  private ExecUtils() {
    throw new AssertionError("not instantiable");
  }

  public static Container.ExecResult execOrDie(Container<?> container, String shellCommand) {
    return checkExitCode(exec(container, shellCommand));
  }

  private static Container.ExecResult checkExitCode(Container.ExecResult result) {
    if (result.getExitCode() != 0) {
      throw new UncheckedIOException(new IOException(result.toString()));
    }
    return result;
  }

  public static Container.ExecResult exec(Container<?> container, String shellCommand) {
    final Logger log = LoggerFactory.getLogger("[" + container.getContainerName() + "]");
    log.info("Executing command: {}", shellCommand);

    return execInContainerUnchecked(container, "sh", "-c", shellCommand);
  }

  private static Container.ExecResult execInContainerUnchecked(Container<?> container, String... command) {
    try {
      return container.execInContainer(command);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InterruptedException e) {
      throw new UncheckedIOException(new InterruptedIOException(e.getMessage()));
    }
  }
}
