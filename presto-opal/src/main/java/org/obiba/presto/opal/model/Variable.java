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
public class Variable {
  private final String name;
  private final String entityType;
  private final String valueType;
  private final boolean repeatable;
  private final String occurrenceGroup;
  private final String mimeType;
  private final String referencedEntityType;
  private final String unit;
  private final int index;
  private final List<Category> categories;
  private final List<Attribute> attributes;

  public Variable(@JsonProperty("name") String name,
                  @JsonProperty("entityType") String entityType,
                  @JsonProperty("valueType") String valueType,
                  @JsonProperty("isRepeatable") boolean repeatable,
                  @JsonProperty("occurrenceGroup") String occurrenceGroup,
                  @JsonProperty("mimeType") String mimeType,
                  @JsonProperty("referencedEntityType") String referencedEntityType,
                  @JsonProperty("unit") String unit,
                  @JsonProperty("index") int index,
                  @JsonProperty("categories") List<Category> categories,
                  @JsonProperty("attributes") List<Attribute> attributes) {
    this.name = name;
    this.entityType = entityType;
    this.valueType = valueType;
    this.repeatable = repeatable;
    this.occurrenceGroup = occurrenceGroup;
    this.mimeType = mimeType;
    this.referencedEntityType = referencedEntityType;
    this.unit = unit;
    this.index = index;
    this.categories = categories;
    this.attributes = attributes;
  }

  public String getName() {
    return name;
  }

  public String getEntityType() {
    return entityType;
  }

  public String getValueType() {
    return valueType;
  }

  public boolean isRepeatable() {
    return repeatable;
  }

  public String getOccurrenceGroup() {
    return occurrenceGroup;
  }

  public String getMimeType() {
    return mimeType;
  }

  public String getReferencedEntityType() {
    return referencedEntityType;
  }

  public String getUnit() {
    return unit;
  }

  public int getIndex() {
    return index;
  }

  public boolean hasCategories() {
    return categories != null && !categories.isEmpty();
  }

  public List<Category> getCategories() {
    return categories;
  }

  public boolean hasAttributes() {
    return attributes != null && !attributes.isEmpty();
  }

  public List<Attribute> getAttributes() {
    return attributes;
  }

  public String getAttributeValue(String namespace, String name, String locale) {
    if (!hasAttributes()) return null;
    return attributes.stream().filter(attr -> attr.isFor(namespace, name, locale)).map(Attribute::getValue).findFirst().orElse(null);
  }
}
