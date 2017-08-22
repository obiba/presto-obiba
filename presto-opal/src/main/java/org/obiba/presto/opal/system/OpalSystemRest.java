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

package org.obiba.presto.opal.system;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.obiba.presto.RestColumnHandle;
import org.obiba.presto.opal.OpalRest;
import org.obiba.presto.opal.model.LocaleText;
import org.obiba.presto.opal.model.Taxonomy;
import retrofit2.Response;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OpalSystemRest extends OpalRest {

  private static final String[] localeTexts = new String[]{"title", "description", "keywords"};

  // schema table name vs. columns
  private Map<SchemaTableName, ConnectorTableMetadata> connectorTableMap = Maps.newConcurrentMap();

  public OpalSystemRest(String url, String username, String password) {
    super(url, username, password);
  }

  @Override
  public synchronized ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName) {
    initialize();
    if (connectorTableMap.containsKey(schemaTableName)) return connectorTableMap.get(schemaTableName);
    ConnectorTableMetadata connectorTableMetadata;
    if ("taxonomy".equals(schemaTableName.getTableName()))
      connectorTableMetadata = new ConnectorTableMetadata(schemaTableName, createTaxonomyColumns());
    else if ("vocabulary".equals(schemaTableName.getTableName()))
      connectorTableMetadata = new ConnectorTableMetadata(schemaTableName, createVocabularyColumns());
    else if ("term".equals(schemaTableName.getTableName()))
      connectorTableMetadata = new ConnectorTableMetadata(schemaTableName, createTermColumns());
    else
      throw new RuntimeException("Unknown opal system schema table: " + schemaTableName);
    connectorTableMap.put(schemaTableName, connectorTableMetadata);
    return connectorTableMetadata;
  }

  @Override
  public List<String> listSchemas() {
    return ImmutableList.of("taxonomies");
  }

  @Override
  public List<SchemaTableName> listTables(String schema) {
    return ImmutableList.of(new SchemaTableName("taxonomies", "taxonomy"),
        new SchemaTableName("taxonomies", "vocabulary"),
        new SchemaTableName("taxonomies", "term"));
  }

  @Override
  public Collection<? extends List<?>> getRows(SchemaTableName schemaTableName, List<RestColumnHandle> restColumnHandles, TupleDomain<ColumnHandle> tupleDomain) {
    initialize();
    try {
      // TODO use the tuple domain constraints
      Response<List<Taxonomy>> execute = service.listTaxonomies(token).execute();
      if (!execute.isSuccessful())
        throw new IllegalStateException("Unable to read taxonomies: " + execute.message());
      List<String> columnNames = restColumnHandles.stream().map(RestColumnHandle::getName).collect(Collectors.toList());
      if ("taxonomy".equals(schemaTableName.getTableName()))
        return getTaxonomyRows(columnNames, tupleDomain, execute.body());
      if ("vocabulary".equals(schemaTableName.getTableName()))
        return getVocabularyRows(columnNames, tupleDomain, execute.body());
      if ("term".equals(schemaTableName.getTableName()))
        return getTermRows(columnNames, tupleDomain, execute.body());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    throw new RuntimeException("Unknown opal system schema table: " + schemaTableName);
  }

  private Collection<? extends List<?>> getTaxonomyRows(List<String> columnNames, TupleDomain<ColumnHandle> tupleDomain, List<Taxonomy> taxonomies) {
    return taxonomies.stream().map(taxo -> {
      List<Object> row = Lists.newArrayList();
      for (String colName : columnNames) {
        if ("name".equals(colName)) row.add(taxo.getName());
        else if ("author".equals(colName)) row.add(taxo.getAuthor());
        else if ("license".equals(colName)) row.add(taxo.getLicense());
        else if (colName.startsWith("title:"))
          row.add(findText(taxo.getTitle(), extractLocale(colName)));
        else if (colName.startsWith("description:"))
          row.add(findText(taxo.getDescription(), extractLocale(colName)));
        else if (colName.startsWith("keywords:"))
          row.add(findText(taxo.getKeywords(), extractLocale(colName)));
        else row.add(null); // TODO parse attribute
      }
      return row;
    }).collect(Collectors.toList());
  }

  private Collection<? extends List<?>> getVocabularyRows(List<String> columnNames, TupleDomain<ColumnHandle> tupleDomain, List<Taxonomy> taxonomies) {
    Collection<List<?>> rows = Lists.newArrayList();
    taxonomies.forEach(taxo -> {
      if (taxo.hasVocabularies())
        taxo.getVocabularies().forEach(voc -> {
          List<Object> row = Lists.newArrayList();
          for (String colName : columnNames) {
            if ("name".equals(colName)) row.add(voc.getName());
            else if ("taxonomy".equals(colName)) row.add(taxo.getName());
            else if (colName.startsWith("title:"))
              row.add(findText(voc.getTitle(), extractLocale(colName)));
            else if (colName.startsWith("description:"))
              row.add(findText(voc.getDescription(), extractLocale(colName)));
            else if (colName.startsWith("keywords:"))
              row.add(findText(voc.getKeywords(), extractLocale(colName)));
            else row.add(null); // TODO parse attribute
          }
          rows.add(row);
        });
    });
    return rows;
  }

  private Collection<? extends List<?>> getTermRows(List<String> columnNames, TupleDomain<ColumnHandle> tupleDomain, List<Taxonomy> taxonomies) {
    Collection<List<?>> rows = Lists.newArrayList();
    taxonomies.forEach(taxo -> {
      if (taxo.hasVocabularies())
        taxo.getVocabularies().forEach(voc -> {
          if (voc.hasTerms())
            voc.getTerms().forEach(term -> {
              List<Object> row = Lists.newArrayList();
              for (String colName : columnNames) {
                if ("name".equals(colName)) row.add(term.getName());
                else if ("taxonomy".equals(colName)) row.add(taxo.getName());
                else if ("vocabulary".equals(colName)) row.add(voc.getName());
                else if (colName.startsWith("title:"))
                  row.add(findText(term.getTitle(), extractLocale(colName)));
                else if (colName.startsWith("description:"))
                  row.add(findText(term.getDescription(), extractLocale(colName)));
                else if (colName.startsWith("keywords:"))
                  row.add(findText(term.getKeywords(), extractLocale(colName)));
                else row.add(null); // TODO parse attribute
              }
              rows.add(row);
            });
        });
    });
    return rows;
  }
  
  private String extractLocale(String columnName) {
    return Splitter.on(":").splitToList(columnName).get(1);
  }

  private String findText(List<LocaleText> texts, String locale) {
    if (texts == null || texts.isEmpty()) return null;
    return texts.stream().filter(lt -> locale.equals(lt.getLocale())).map(LocaleText::getText).findFirst().orElse(null);
  }

  private List<ColumnMetadata> createTaxonomyColumns() {
    ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.<ColumnMetadata>builder()
        .add(new ColumnMetadata("name", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("author", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("license", VarcharType.createUnboundedVarcharType()));
    addLocaleTextColumns(builder);
    return builder.build();
  }

  private List<ColumnMetadata> createVocabularyColumns() {
    ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.<ColumnMetadata>builder()
        .add(new ColumnMetadata("name", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("taxonomy", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("repeatable", BooleanType.BOOLEAN));
    addLocaleTextColumns(builder);
    return builder.build();
  }

  private List<ColumnMetadata> createTermColumns() {
    ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.<ColumnMetadata>builder()
        .add(new ColumnMetadata("name", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("taxonomy", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("vocabulary", VarcharType.createUnboundedVarcharType()));
    addLocaleTextColumns(builder);
    return builder.build();
  }

  private void addLocaleTextColumns(ImmutableList.Builder<ColumnMetadata> builder) {
    for (String text : localeTexts) {
      for (String language : opalConf.getLanguages()) {
        builder.add(new ColumnMetadata(text + ":" + language, VarcharType.createUnboundedVarcharType()));
      }
    }
  }

}
