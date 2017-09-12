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

package org.obiba.presto.opal.administration;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.obiba.presto.opal.model.Database;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

class DatabaseTable extends ConnectorTableMetadata {

  DatabaseTable(SchemaTableName table) {
    super(table, createColumns());
  }

  static Collection<? extends List<?>> getRows(List<String> columnNames, List<Database> databases) {
    return databases.stream()
        .map(db -> Lists.newArrayList(db.getName(), db.getUsage(), db.isDefaultStorage(), db.hasDatasource(), db.getType(), db.getUrl(), db.getUsername()))
        .collect(Collectors.toList());
  }

  private static List<ColumnMetadata> createColumns() {
    ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.<ColumnMetadata>builder()
        .add(new ColumnMetadata("name", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("usage", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("default_storage", BooleanType.BOOLEAN))
        .add(new ColumnMetadata("has_datasource", BooleanType.BOOLEAN))
        .add(new ColumnMetadata("type", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("url", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("username", VarcharType.createUnboundedVarcharType()));
    return builder.build();
  }

}
