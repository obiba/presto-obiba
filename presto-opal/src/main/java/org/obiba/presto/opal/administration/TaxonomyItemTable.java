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
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.obiba.presto.RestCache;
import org.obiba.presto.opal.model.LocaleText;
import org.obiba.presto.opal.model.OpalConf;

import java.util.List;

abstract class TaxonomyItemTable extends ConnectorTableMetadata {

  private static final String[] localeTexts = new String[]{"title", "description", "keywords"};

  TaxonomyItemTable(SchemaTableName table, List<ColumnMetadata> columns) {
    super(table, columns);
  }

  static void addLocaleTextColumns(RestCache<OpalConf> opalConfCache, ImmutableList.Builder<ColumnMetadata> builder) {
    for (String text : localeTexts) {
      for (String language : opalConfCache.getItem().getLanguages()) {
        builder.add(new ColumnMetadata(text + ":" + language, VarcharType.createUnboundedVarcharType()));
      }
    }
  }

  static String extractLocale(String columnName) {
    return Splitter.on(":").splitToList(columnName).get(1);
  }

  static String findText(List<LocaleText> texts, String locale) {
    if (texts == null || texts.isEmpty()) return null;
    return texts.stream().filter(lt -> locale.equals(lt.getLocale())).map(LocaleText::getText).findFirst().orElse(null);
  }
}
