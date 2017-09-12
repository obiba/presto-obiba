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
public class Project {
  private final String name;
  private final String title;
  private final String description;
  private final List<String> tags;
  private final String database;
  private final String vcfStoreService;

  public Project(@JsonProperty("name") String name,
                 @JsonProperty("title") String title,
                 @JsonProperty("description") String description,
                 @JsonProperty("tags") List<String> tags,
                 @JsonProperty("database") String database,
                 @JsonProperty("vcfStoreService") String vcfStoreService) {
    this.name = name;
    this.title = title;
    this.description = description;
    this.tags = tags;
    this.database = database;
    this.vcfStoreService = vcfStoreService;
  }

  public String getName() {
    return name;
  }

  public String getTitle() {
    return title;
  }

  public String getDescription() {
    return description;
  }

  public boolean hasTags() {
    return tags != null && !tags.isEmpty();
  }

  public List<String> getTags() {
    return tags;
  }

  public String getDatabase() {
    return database;
  }

  public String getVcfStoreService() {
    return vcfStoreService;
  }
}
