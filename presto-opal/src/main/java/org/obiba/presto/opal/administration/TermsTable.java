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

class TermsTable extends TaxonomyItemTable {

  static final String NAME = "terms";

  TermsTable(SchemaTableName table, RestCache<OpalConf> opalConfCache) {
    super(table, createColumns(opalConfCache));
  }

  static Collection<? extends List<?>> getRows(List<String> columnNames, List<Taxonomy> taxonomies) {
    Collection<List<?>> rows = Lists.newArrayList();
    taxonomies.forEach(taxo ->
        taxo.getVocabularies().forEach(voc -> {
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
        }));
    return rows;
  }


  private static List<ColumnMetadata> createColumns(RestCache<OpalConf> opalConfCache) {
    ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.<ColumnMetadata>builder()
        .add(new ColumnMetadata("name", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("taxonomy", VarcharType.createUnboundedVarcharType()))
        .add(new ColumnMetadata("vocabulary", VarcharType.createUnboundedVarcharType()));
    addLocaleTextColumns(opalConfCache, builder);
    return builder.build();
  }
}
