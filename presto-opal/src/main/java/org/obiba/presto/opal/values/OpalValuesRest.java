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

package org.obiba.presto.opal.values;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.obiba.presto.RestColumnHandle;
import org.obiba.presto.RestRecordSet;
import org.obiba.presto.opal.OpalDatasourcesRest;
import org.obiba.presto.opal.model.ValueSets;
import org.obiba.presto.opal.model.Variable;
import retrofit2.Response;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class OpalValuesRest extends OpalDatasourcesRest {

  public static final int BATCH_SIZE = 10000;

  // schema table name vs. columns
  private Map<SchemaTableName, ConnectorTableMetadata> connectorTableMap = Maps.newHashMap();

  // schema table name vs (column name vs. variable name)
  private Map<SchemaTableName, Map<String, Variable>> columnNameMap = Maps.newHashMap();

  public OpalValuesRest(String url, String username, String password) {
    super(url, username, password);
  }

  @Override
  public synchronized ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName) {
    initialize();
    if (connectorTableMap.containsKey(schemaTableName)) return connectorTableMap.get(schemaTableName);
    // fetch and cache variables
    try {
      Response<List<Variable>> response = service.listVariables(token, getOpalDatasourceName(schemaTableName), getOpalTableName(schemaTableName)).execute();
      if (!response.isSuccessful())
        throw new IllegalStateException("Unable to read '" + getOpalTableRef(schemaTableName) + "' variables: " + response.message());
      List<Variable> variables = response.body();
      columnNameMap.put(schemaTableName, Maps.newHashMap());
      for (Variable variable : variables) {
        String columnNameOrig = normalize(variable.getName());
        String columnName = columnNameOrig;
        int i = 1;
        while (columnNameMap.get(schemaTableName).containsKey(columnName)) {
          columnName = columnNameOrig + "_" + i;
          i++;
        }
        columnNameMap.get(schemaTableName).put(columnName, variable);
      }
      List<ColumnMetadata> columns = variables.stream().map(OpalColumnMetadata::new).collect(Collectors.toList());
      columns.add(0, new OpalIDColumnMetadata());
      ConnectorTableMetadata connectorTableMetadata = new ConnectorTableMetadata(schemaTableName, columns);
      connectorTableMap.put(schemaTableName, connectorTableMetadata);
      return connectorTableMetadata;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Collection<? extends List<?>> getRows(SchemaTableName schemaTableName, List<RestColumnHandle> restColumnHandles) {
    throw new UnsupportedOperationException();
  }

  Collection<? extends List<?>> getRows(SchemaTableName schemaTableName, List<RestColumnHandle> restColumnHandles, int offset) {
    initialize();
    try {
      // TODO use the tuple domain constraints
      Response<ValueSets> execute = service.listValueSets(token, getOpalDatasourceName(schemaTableName), getOpalTableName(schemaTableName), offset, BATCH_SIZE).execute();
      if (!execute.isSuccessful())
        throw new IllegalStateException("Unable to read '" + getOpalTableRef(schemaTableName) + "' values: " + execute.message());
      ValueSets valueSets = execute.body();
      return valueSets.getStringValues(restColumnHandles.stream().map(col -> getOpalVariable(schemaTableName, col)).collect(toList()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public RecordSet getRecordSet(SchemaTableName schemaTableName, List<RestColumnHandle> restColumnHandles) {
    return new OpalValuesRecordSet(this, schemaTableName, restColumnHandles);
  }

  private Variable getOpalVariable(SchemaTableName schemaTableName, RestColumnHandle columnHandle) {
    return columnNameMap.get(schemaTableName).get(columnHandle.getName());
  }

}
