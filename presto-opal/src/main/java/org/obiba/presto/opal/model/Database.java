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

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Database {
  private final String name;
  private final String usage;
  private final boolean defaultStorage;
  private final boolean hasDatasource;
  private final boolean usedForIdentifiers;
  private final String type;
  private final String url;
  private final String username;

  public Database(@JsonProperty("name") String name,
                  @JsonProperty("usage") String usage,
                  @JsonProperty("defaultStorage") boolean defaultStorage,
                  @JsonProperty("hasDatasource") boolean hasDatasource,
                  @JsonProperty("usedForIdentifiers") boolean usedForIdentifiers,
                  @JsonProperty("mongoDbSettings") Map<String, Object> mongoDbSettings,
                  @JsonProperty("sqlSettings") Map<String, Object> sqlSettings) {
    this.name = name;
    this.usage = usage;
    this.defaultStorage = defaultStorage;
    this.hasDatasource = hasDatasource;
    this.usedForIdentifiers = usedForIdentifiers;
    if (mongoDbSettings != null && !mongoDbSettings.isEmpty()) {
      this.type = "MONGODB";
      this.url = mongoDbSettings.get("url").toString();
      this.username = mongoDbSettings.containsKey("username") ? mongoDbSettings.get("username").toString() : null;
    } else {
      this.type = sqlSettings.get("sqlSchema").toString();
      this.url = sqlSettings.get("url").toString();
      this.username = sqlSettings.containsKey("username") ? sqlSettings.get("username").toString() : null;
    }
  }

  public String getName() {
    return name;
  }

  public String getUsage() {
    return usage;
  }

  public boolean isDefaultStorage() {
    return defaultStorage;
  }

  public boolean hasDatasource() {
    return hasDatasource;
  }

  public boolean isUsedForIdentifiers() {
    return usedForIdentifiers;
  }

  public String getType() {
    return type;
  }

  public String getUrl() {
    return url;
  }

  public String getUsername() {
    return username;
  }
}
