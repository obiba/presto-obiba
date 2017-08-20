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

package org.obiba.presto.opal;

import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.obiba.presto.Rest;
import org.obiba.presto.RestColumnHandle;
import org.obiba.presto.opal.model.OpalDatasource;
import org.obiba.presto.opal.model.OpalValueSets;
import org.obiba.presto.opal.model.OpalVariable;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class OpalRest implements Rest {
  private final String opalUrl;
  private final String token;
  private final OpalService service;

  private List<OpalDatasource> datasources;

  // schema name vs. opal datasource
  private Map<String, OpalDatasource> opalDatasourceMap = Maps.newHashMap();

  // schema table name vs. opal table name
  private Map<SchemaTableName, String> opalTableNameMap = Maps.newHashMap();

  // schema table name vs. columns
  private Map<SchemaTableName, ConnectorTableMetadata> connectorTableMap = Maps.newHashMap();

  // schema table name vs (column name vs. variable name)
  private Map<SchemaTableName, Map<String, OpalVariable>> columnNameMap = Maps.newHashMap();

  public OpalRest(String url, String username, String password) {
    this.opalUrl = url;
    this.token = "X-Opal-Auth " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
    this.service = new Retrofit.Builder()
        .baseUrl(url)
        .addConverterFactory(JacksonConverterFactory.create())
        .build()
        .create(OpalService.class);
  }

  @Override
  public ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName) {
    initialize();
    if (connectorTableMap.containsKey(schemaTableName)) return connectorTableMap.get(schemaTableName);
    // fetch and cache variables
    try {
      Response<List<OpalVariable>> response = service.listVariables(token, getOpalDatasourceName(schemaTableName), getOpalTableName(schemaTableName)).execute();
      if (!response.isSuccessful())
        throw new IllegalStateException("Unable to read '" + getOpalTableRef(schemaTableName) + "' variables: " + response.message());
      List<OpalVariable> variables = response.body();
      columnNameMap.put(schemaTableName, Maps.newHashMap());
      for (OpalVariable variable : variables) {
        String columnNameOrig = variable.getName().toLowerCase(Locale.ENGLISH);
        String columnName = columnNameOrig;
        int i = 1;
        while (columnNameMap.get(schemaTableName).containsKey(columnName)) {
          columnName = columnNameOrig + "_" + i;
          i++;
        }
        columnNameMap.get(schemaTableName).put(columnName, variable);
      }
      ConnectorTableMetadata connectorTableMetadata = new ConnectorTableMetadata(schemaTableName,
          variables.stream().map(OpalColumnMetadata::new).collect(Collectors.toList()));
      connectorTableMap.put(schemaTableName, connectorTableMetadata);
      return connectorTableMetadata;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<String> listSchemas() {
    initialize();
    return ImmutableList.copyOf(opalDatasourceMap.keySet());
  }

  @Override
  public List<SchemaTableName> listTables(String schema) {
    initialize();
    return ImmutableList.copyOf(opalTableNameMap.keySet());
  }

  @Override
  public Collection<? extends List<?>> getRows(SchemaTableName schemaTableName, List<RestColumnHandle> restColumnHandles) {
    initialize();
    try {
      Response<OpalValueSets> execute = service.listValueSets(token, getOpalDatasourceName(schemaTableName), getOpalTableName(schemaTableName)).execute();
      if (!execute.isSuccessful())
        throw new IllegalStateException("Unable to read '" + getOpalTableRef(schemaTableName) + "' values: " + execute.message());
      OpalValueSets valueSets = execute.body();
      return valueSets.getStringValues(restColumnHandles.stream().map(col -> getOpalVariable(schemaTableName, col)).collect(toList()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Consumer<List> createRowSink(SchemaTableName schemaTableName) {
    throw new IllegalStateException("This connector does not support write");
  }

  /**
   * Fetch opal datasources and associated tables.
   */
  private void initialize() {
    if (datasources != null && !datasources.isEmpty()) return;
    try {
      Response<List<OpalDatasource>> response = service.listDatasources(token).execute();
      if (!response.isSuccessful())
        throw new IllegalStateException("Unable to read opal datasources: " + response.message());
      datasources = response.body();
      // handle possible case conflicts
      opalDatasourceMap.clear();
      for (OpalDatasource datasource : datasources) {
        String schemaNameOrig = datasource.getName().toLowerCase(Locale.ENGLISH);
        String schemaName = schemaNameOrig;
        int i = 1;
        while (opalDatasourceMap.containsKey(schemaName)) {
          schemaName = schemaNameOrig + "_" + i;
          i++;
        }
        opalDatasourceMap.put(schemaName, datasource);
        for (String tableName : datasource.getTableNames()) {
          SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
          i = 1;
          while (opalTableNameMap.containsKey(schemaTableName)) {
            schemaTableName = new SchemaTableName(schemaName, tableName + "_" + i);
            i++;
          }
          opalTableNameMap.put(schemaTableName, tableName);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String getOpalDatasourceName(SchemaTableName schemaTableName) {
    return opalDatasourceMap.get(schemaTableName.getSchemaName()).getName();
  }

  private String getOpalTableName(SchemaTableName schemaTableName) {
    return opalTableNameMap.get(schemaTableName);
  }

  private OpalVariable getOpalVariable(SchemaTableName schemaTableName, RestColumnHandle columnHandle) {
    return columnNameMap.get(schemaTableName).get(columnHandle.getName());
  }

  private String getOpalTableRef(SchemaTableName schemaTableName) {
    return opalDatasourceMap.get(schemaTableName.getSchemaName()).getName() + "." + opalTableNameMap.get(schemaTableName);
  }

}
