/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.obiba.presto.opal.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Attribute {
  private final String namespace;
  private final String name;
  private final String locale;
  private final String value;

  public Attribute(@JsonProperty("name") String name,
                   @JsonProperty("namespace") String namespace,
                   @JsonProperty("locale") String locale,
                   @JsonProperty("value") String value) {
    this.name = name;
    this.namespace = namespace;
    this.locale = locale;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getLocale() {
    return locale;
  }

  public String getValue() {
    return value;
  }

  public boolean isFor(String namespace, String name, String locale) {
    if (this.namespace == null) {
      if (namespace != null) return false;
    } else if (!this.namespace.equals(namespace)) return false;
    if (this.locale == null) {
      if (locale != null) return false;
    } else if (!this.locale.equals(locale)) return false;
    return this.name.equals(name);
  }
}
