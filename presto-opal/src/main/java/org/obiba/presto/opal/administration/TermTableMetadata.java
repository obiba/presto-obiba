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
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import org.obiba.presto.RestCache;
import org.obiba.presto.opal.model.OpalConf;

import java.util.List;

public class TermTableMetadata extends TaxonomyItemTableMetadata {

  public TermTableMetadata(SchemaTableName table, RestCache<OpalConf> opalConfCache) {
    super(table, createColumns(opalConfCache));
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
