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
public class Entry {
  private final String key;
  private final String value;
  private final List<String> values;

  public Entry(@JsonProperty("key") String key,
               @JsonProperty("value") String value,
               @JsonProperty("values") List<String> values) {
    this.key = key;
    this.value = value;
    this.values = values;
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  public boolean hasValues() {
    return values != null && !values.isEmpty();
  }

  public List<String> getValues() {
    return values;
  }
}
