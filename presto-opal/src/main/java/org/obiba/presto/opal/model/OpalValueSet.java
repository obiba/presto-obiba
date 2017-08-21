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
import com.google.common.base.Joiner;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OpalValueSet {
  private final String identifier;
  private final List<Map<String, Object>> values;
  private final Timestamps timestamps;

  public OpalValueSet(@JsonProperty("identifier") String identifier,
                      @JsonProperty("values") List<Map<String, Object>> values,
                      @JsonProperty("timestamps") Timestamps timestamps) {
    this.identifier = identifier;
    this.values = values;
    this.timestamps = timestamps;
  }

  public String getIdentifier() {
    return identifier;
  }

  public List<String> getStringValues() {
    return values.stream().map(v -> v.containsKey("values") ?
        asString(v.getOrDefault("values", "")): asString(v.getOrDefault("value", "")))
        .collect(Collectors.toList());
  }

  public List<String> getStringValues(List<Integer> positions) {
    return positions.stream().map(pos -> {
      if (pos<0 || pos>=values.size()) return "";
      Map<String, Object> valueMap = values.get(pos);
      if (valueMap.containsKey("value")) return asString(valueMap.get("value"));
      else if (valueMap.containsKey("values")) return asString(valueMap.get("values"));
      return "";
    }).collect(Collectors.toList());
  }

  public Timestamps getTimestamps() {
    return timestamps;
  }

  private String asString(Object obj) {
    if (obj == null) return "";
    if (obj instanceof Collection)
      return ((Collection<?>)obj).stream().map(o -> asString(((Map<String, Object>)o).get("value"))).collect(Collectors.joining(","));
    return obj.toString();
  }
}
