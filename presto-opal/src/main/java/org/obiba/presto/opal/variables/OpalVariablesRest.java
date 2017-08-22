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

package org.obiba.presto.opal.variables;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.obiba.presto.RestColumnHandle;
import org.obiba.presto.opal.OpalDatasourcesRest;
import org.obiba.presto.opal.model.Category;
import org.obiba.presto.opal.model.Taxonomy;
import org.obiba.presto.opal.model.Variable;
import retrofit2.Response;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OpalVariablesRest extends OpalDatasourcesRest {

  private List<Taxonomy> taxonomies;

  // schema table name vs. columns
  private Map<SchemaTableName, ConnectorTableMetadata> connectorTableMap = Maps.newHashMap();

  // column name vs. taxonomy-vocabulary tuple
  private Map<String, String[]> vocabularyMap;

  public OpalVariablesRest(String url, String username, String password) {
    super(url, username, password);
  }

  @Override
  public synchronized ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName) {
    initialize();
    if (connectorTableMap.containsKey(schemaTableName)) return connectorTableMap.get(schemaTableName);
    ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.<ColumnMetadata>builder()
        .add(new ColumnMetadata("name", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("entity_type", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("value_type", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("repeatable", BooleanType.BOOLEAN))
        .add(new ColumnMetadata("occurrence_group", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("mime_type", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("referenced_entity_type", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("unit", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("index", IntegerType.INTEGER))
        .add(new ColumnMetadata("categories", VarcharType.createUnboundedVarcharType()));
    builder.add(new ColumnMetadata("script", VarcharType.createUnboundedVarcharType()));
    for (String text : new String[]{"label", "description"}) {
      for (String language : opalConf.getLanguages()) {
        builder.add(new ColumnMetadata(text + ":" + language, VarcharType.createUnboundedVarcharType()));
      }
    }
    getVocabularyColumnNames().forEach(vocAttr -> builder.add(new ColumnMetadata(vocAttr, VarcharType.createUnboundedVarcharType())));
    ConnectorTableMetadata connectorTableMetadata = new ConnectorTableMetadata(schemaTableName, builder.build());
    connectorTableMap.put(schemaTableName, connectorTableMetadata);
    return connectorTableMetadata;
  }

  @Override
  public Collection<? extends List<?>> getRows(SchemaTableName schemaTableName, List<RestColumnHandle> restColumnHandles, TupleDomain<ColumnHandle> tupleDomain) {
    initialize();
    try {
      // TODO use the tuple domain constraints
      Response<List<Variable>> execute = service.listVariables(token, getOpalDatasourceName(schemaTableName), getOpalTableName(schemaTableName)).execute();
      if (!execute.isSuccessful())
        throw new IllegalStateException("Unable to read '" + getOpalTableRef(schemaTableName) + "' variables: " + execute.message());
      List<String> columnNames = restColumnHandles.stream().map(RestColumnHandle::getName).collect(Collectors.toList());
      return execute.body().stream().map(v -> {
        List<Object> builder = Lists.newArrayList();
        for (String colName : columnNames) {
          if ("name".equals(colName)) builder.add(v.getName());
          else if ("entity_type".equals(colName)) builder.add(v.getEntityType());
          else if ("value_type".equals(colName)) builder.add(v.getValueType());
          else if ("repeatable".equals(colName)) builder.add(v.isRepeatable());
          else if ("occurrence_group".equals(colName)) builder.add(v.getOccurrenceGroup());
          else if ("referenced_entity_type".equals(colName)) builder.add(v.getReferencedEntityType());
          else if ("mime_type".equals(colName)) builder.add(v.getMimeType());
          else if ("unit".equals(colName)) builder.add(v.getUnit());
          else if ("index".equals(colName)) builder.add(v.getIndex());
          else if ("categories".equals(colName)) builder.add(v.hasCategories() ?
              v.getCategories().stream().map(Category::getName).collect(Collectors.joining(",")) : null);
          else if ("script".equals(colName))
            builder.add(v.getAttributeValue(null, "script", null));
          else if (colName.startsWith("label:"))
            builder.add(v.getAttributeValue(null, "label", extractLocale(colName)));
          else if (colName.startsWith("description:"))
            builder.add(v.getAttributeValue(null, "description", extractLocale(colName)));
          else if (vocabularyMap.containsKey(colName))
            builder.add(v.getAttributeValue(vocabularyMap.get(colName)[0],vocabularyMap.get(colName)[1], null));
          else builder.add(null); // TODO parse attribute
        }
        return builder;
      }).collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String extractLocale(String columnName) {
    List<String> tokens = Splitter.on(":").splitToList(columnName);
    return tokens.get(tokens.size() - 1);
  }

  @Override
  protected synchronized void initialize() {
    super.initialize();
    initializeTaxonomies();
  }

  private List<String> getVocabularyColumnNames() {
    if (vocabularyMap == null) return Lists.newArrayList();
    return Lists.newArrayList(vocabularyMap.keySet());
  }

  private void initializeTaxonomies() {
    if (taxonomies != null && !taxonomies.isEmpty()) return;
    try {
      Response<List<Taxonomy>> response = service.listTaxonomies(token).execute();
      if (!response.isSuccessful())
        throw new IllegalStateException("Unable to read opal taxonomies: " + response.message());
      taxonomies = response.body();
      // Vocabulary names in the form of attribute header: namespace::name.
      vocabularyMap = Maps.newHashMap();
      taxonomies.forEach(taxo -> taxo.getVocabularies()
          .forEach(voc -> vocabularyMap.put(normalize(taxo.getName() + "::" + voc.getName()), new String[] {taxo.getName(), voc.getName()})));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}