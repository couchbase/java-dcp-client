/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.client.dcp.metrics;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.dropwizard.DropwizardConfig;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class DefaultDropwizardConfig implements DropwizardConfig {
  private final String prefix;
  private final Function<String, String> configValueAccessor;

  public DefaultDropwizardConfig() {
    this("dropwizard", key -> null);
  }

  public DefaultDropwizardConfig(String prefix, Map<String, String> config) {
    this(prefix, config::get);
  }

  public DefaultDropwizardConfig(String prefix, Properties config) {
    this(prefix, config::getProperty);
  }

  public DefaultDropwizardConfig(String prefix, Function<String, String> configValueAccessor) {
    this.prefix = requireNonNull(prefix);
    this.configValueAccessor = requireNonNull(configValueAccessor);
  }

  @Override
  public String prefix() {
    return prefix;
  }

  @Override
  public String get(String key) {
    return configValueAccessor.apply(key);
  }

  public static final HierarchicalNameMapper PRETTY_TAGS = (id, convention) -> {
    String base = id.getConventionName(convention);

    if (base.startsWith("dcp")) {
      base = "dcp." + Character.toLowerCase(base.charAt(3)) + base.substring(4);
    }

    List<Tag> tags = id.getConventionTags(convention);
    if (tags.isEmpty()) {
      return base;
    }
    return base + "{" + tags.stream()
        .map(t -> t.getKey() + "=" + t.getValue())
        .map(nameSegment -> nameSegment.replace(" ", "_"))
        .collect(Collectors.joining(","))
        + "}";
  };
}
