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

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Category {

  private final String name;
  private final boolean missing;
  private final List<Attribute> attributes;

  public Category(@JsonProperty("name") String name,
                  @JsonProperty("isMissing") boolean missing,
                  @JsonProperty("attributes") List<Attribute> attributes) {
    this.name = name;
    this.missing = missing;
    this.attributes = attributes;
  }

  public String getName() {
    return name;
  }

  public boolean isMissing() {
    return missing;
  }

  public boolean hasAttributes() {
    return attributes != null && !attributes.isEmpty();
  }

  public List<Attribute> getAttributes() {
    return attributes;
  }

  public String getLabelValue(String locale) {
    return getAttributeValue(null, "label", locale);
  }

  public String getAttributeValue(String namespace, String name, String locale) {
    if (!hasAttributes()) return null;
    return attributes.stream().filter(attr -> attr.isFor(namespace, name, locale)).map(Attribute::getValue).findFirst().orElse(null);
  }
}
