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
import com.google.common.collect.Lists;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Taxonomy extends TaxonomyEntity {
  private final String name;
  private final String author;
  private final String license;
  private final List<Vocabulary> vocabularies;

  public Taxonomy(@JsonProperty("name") String name,
                  @JsonProperty("author") String author,
                  @JsonProperty("license") String license,
                  @JsonProperty("title") List<LocaleText> title,
                  @JsonProperty("description") List<LocaleText> description,
                  @JsonProperty("keywords") List<LocaleText> keywords,
                  @JsonProperty("attributes") List<Entry> attributes,
                  @JsonProperty("vocabularies") List<Vocabulary> vocabularies) {
    super(title, description, keywords, attributes);
    this.name = name;
    this.author = author;
    this.license = license;
    this.vocabularies = vocabularies;
  }

  public String getName() {
    return name;
  }

  public String getAuthor() {
    return author;
  }

  public String getLicense() {
    return license;
  }

  public List<Vocabulary> getVocabularies() {
    return vocabularies == null ? Lists.newArrayList() : vocabularies;
  }
}
