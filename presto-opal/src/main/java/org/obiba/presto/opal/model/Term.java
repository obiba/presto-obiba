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
public class Term {
  private final String name;
  private final List<LocaleText> title;
  private final List<LocaleText> description;
  private final List<LocaleText> keywords;

  public Term(@JsonProperty("name") String name,
              @JsonProperty("title") List<LocaleText> title,
              @JsonProperty("description") List<LocaleText> description,
              @JsonProperty("keywords") List<LocaleText> keywords) {
    this.name = name;
    this.title = title;
    this.description = description;
    this.keywords = keywords;
  }

  public String getName() {
    return name;
  }

  public List<LocaleText> getTitle() {
    return title;
  }

  public List<LocaleText> getDescription() {
    return description;
  }

  public List<LocaleText> getKeywords() {
    return keywords;
  }
}
