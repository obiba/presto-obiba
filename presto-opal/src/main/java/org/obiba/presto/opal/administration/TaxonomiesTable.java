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
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.obiba.presto.RestCache;
import org.obiba.presto.opal.model.OpalConf;
import org.obiba.presto.opal.model.Taxonomy;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

class TaxonomiesTable extends TaxonomyItemTable {

  static final String NAME = "taxonomies";

  TaxonomiesTable(SchemaTableName table, RestCache<OpalConf> opalConfCache) {
    super(table, createColumns(opalConfCache));
  }

  static Collection<? extends List<?>> getRows(List<String> columnNames, List<Taxonomy> taxonomies) {
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

  private static List<ColumnMetadata> createColumns(RestCache<OpalConf> opalConfCache) {
    ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.<ColumnMetadata>builder()
        .add(new ColumnMetadata("name", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("author", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("license", VarcharType.createUnboundedVarcharType()));
    addLocaleTextColumns(opalConfCache, builder);
    return builder.build();
  }
}
