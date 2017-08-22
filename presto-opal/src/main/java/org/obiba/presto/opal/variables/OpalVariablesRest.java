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
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.obiba.presto.RestColumnHandle;
import org.obiba.presto.opal.OpalDatasourcesRest;
import org.obiba.presto.opal.model.OpalVariable;
import retrofit2.Response;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OpalVariablesRest extends OpalDatasourcesRest {

  // schema table name vs. columns
  private Map<SchemaTableName, ConnectorTableMetadata> connectorTableMap = Maps.newHashMap();

  public OpalVariablesRest(String url, String username, String password) {
    super(url, username, password);
  }

  @Override
  public synchronized ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName) {
    initializeDatasources();
    if (connectorTableMap.containsKey(schemaTableName)) return connectorTableMap.get(schemaTableName);
    List<ColumnMetadata> columns = ImmutableList.<ColumnMetadata>builder()
        .add(new ColumnMetadata("name", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("entity_type", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("value_type", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("repeatable", BooleanType.BOOLEAN))
        .add(new ColumnMetadata("occurrence_group", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("mime_type", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("referenced_entity_type", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("unit", VarcharType.createUnboundedVarcharType()))
        .build();
    ConnectorTableMetadata connectorTableMetadata = new ConnectorTableMetadata(schemaTableName, columns);
    connectorTableMap.put(schemaTableName, connectorTableMetadata);
    return connectorTableMetadata;
  }

  @Override
  public Collection<? extends List<?>> getRows(SchemaTableName schemaTableName, List<RestColumnHandle> restColumnHandles, TupleDomain<ColumnHandle> tupleDomain) {
    initializeDatasources();
    try {
      // TODO use the tuple domain constraints
      Response<List<OpalVariable>> execute = service.listVariables(token, getOpalDatasourceName(schemaTableName), getOpalTableName(schemaTableName)).execute();
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
        }
        return builder;
      }).collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
