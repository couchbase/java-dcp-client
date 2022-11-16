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
package com.couchbase.client.dcp.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * Representation of a software version, up to major.minor.patch granularity.
 */
public class Version implements Comparable<Version> {
  private final int major;
  private final int minor;
  private final int patch;

  public Version(int major, int minor, int patch) {
    this.major = major;
    this.minor = minor;
    this.patch = patch;
  }

  public int major() {
    return major;
  }

  public int minor() {
    return minor;
  }

  public int patch() {
    return patch;
  }

  public boolean isAtLeast(Version other) {
    return compareTo(other) >= 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Version version = (Version) o;

    return major == version.major && minor == version.minor && patch == version.patch;
  }

  @Override
  public int hashCode() {
    int result = major;
    result = 31 * result + minor;
    result = 31 * result + patch;
    return result;
  }

  @Override
  public int compareTo(Version o) {
    int result = Integer.compare(major, o.major);
    if (result != 0) {
      return result;
    }
    result = Integer.compare(minor, o.minor);
    if (result != 0) {
      return result;
    }
    return Integer.compare(patch, o.patch);
  }

  @Override
  public String toString() {
    return major + "." + minor + "." + patch;
  }

  private static final Pattern VERSION_PATTERN = Pattern.compile("^(\\d+)(?:\\.(\\d+))?(?:\\.(\\d+))?.*");

  /**
   * Parses a {@link String} into a {@link Version}. This expects a version string in the form of
   * "X[.Y[.Z]][anything]". That is a major number, followed optionally by a minor only or a minor + a patch revision,
   * everything after that being completely ignored.
   * <p>
   * The parsing is extremely lenient, accepting any input string whose first character is a decimal digit.
   * <p>
   * For example, the following version strings are valid:
   * <pre>
   * - "3.a.2" (3.0.0) considered only a major version since there's a char where minor number should be
   * - "2" (2.0.0)
   * - "3.11" (3.11.0)
   * - "3.14.15" (3.14.15)
   * - "1.2.3-SNAPSHOT-12.10.2014" (1.2.3)
   * </pre>
   * <p>
   * Bad version strings cause an {@link IllegalArgumentException}, whereas a null one will cause
   * a {@link NullPointerException}.
   *
   * @param versionString the string to parse into a Version.
   * @return the major.minor.patch Version corresponding to the string.
   * @throws IllegalArgumentException if the string cannot be correctly parsed into a Version.
   * This happens if the input is empty, the first character is not a decimal digit,
   * or if any version component is greater than Integer.MAX_VALUE.
   * @throws NullPointerException if the string is null.
   */
  public static Version parseVersion(String versionString) {
    requireNonNull(versionString, "versionString");

    Matcher matcher = VERSION_PATTERN.matcher(versionString);
    if (matcher.matches() && matcher.groupCount() == 3) {
      int major = Integer.parseInt(matcher.group(1));
      int minor = matcher.group(2) != null ? Integer.parseInt(matcher.group(2)) : 0;
      int patch = matcher.group(3) != null ? Integer.parseInt(matcher.group(3)) : 0;

      // Custom server builds report a version of 0.0.0. Treat these as if they
      // were the most recent version, since they usually are.
      if (major == 0) {
        major = 9999;
      }

      return new Version(major, minor, patch);
    } else {
      throw new IllegalArgumentException(
          "Expected a version string starting with X[.Y[.Z]], was " + versionString);
    }
  }
}
