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

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.obiba.presto.RestCache;
import org.obiba.presto.opal.model.Datasource;
import retrofit2.Response;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class OpalDatasourcesRest extends OpalRest {

  private RestCache<List<Datasource>> datasourcesCache;

  // schema name vs. opal datasource
  protected Map<String, Datasource> opalDatasourceMap = Maps.newHashMap();

  // schema table name vs. opal table name
  protected Map<SchemaTableName, String> opalTableNameMap = Maps.newHashMap();

  public OpalDatasourcesRest(String url, String username, String password, int cacheDelay) {
    super(url, username, password, cacheDelay);
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
  protected synchronized void initialize() {
    super.initialize();
    initializeDatasources();
  }

  /**
   * Fetch opal datasources and associated tables.
   */
  private void initializeDatasources() {
    if (datasourcesCache != null && !datasourcesCache.hasExpired() && !datasourcesCache.getItem().isEmpty()) return;
    datasourcesCache = null;
    opalDatasourceMap.clear();
    opalTableNameMap.clear();
    try {
      Response<List<Datasource>> response = service.listDatasources(token).execute();
      if (!response.isSuccessful())
        throw new IllegalStateException("Unable to read opal datasources: " + response.message());
      List<Datasource> datasources = response.body();
      // handle possible case conflicts
      opalDatasourceMap.clear();
      for (Datasource datasource : datasources) {
        String schemaNameOrig = normalize(datasource.getName());
        String schemaName = schemaNameOrig;
        int i = 1;
        while (opalDatasourceMap.containsKey(schemaName)) {
          schemaName = schemaNameOrig + "_" + i;
          i++;
        }
        opalDatasourceMap.put(schemaName, datasource);
        for (String tableName : datasource.getTableNames()) {
          SchemaTableName schemaTableName = new SchemaTableName(schemaName, normalize(tableName));
          i = 1;
          while (opalTableNameMap.containsKey(schemaTableName)) {
            schemaTableName = new SchemaTableName(schemaName, normalize(tableName) + "_" + i);
            i++;
          }
          opalTableNameMap.put(schemaTableName, tableName);
        }
      }
      datasourcesCache = new RestCache<>(datasources, cacheDelay);
      onDatasourcesInitialized();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected void onDatasourcesInitialized() {

  }

  protected String getOpalDatasourceName(SchemaTableName schemaTableName) {
    return opalDatasourceMap.get(schemaTableName.getSchemaName()).getName();
  }

  protected String getOpalTableName(SchemaTableName schemaTableName) {
    return opalTableNameMap.get(schemaTableName);
  }

  protected String getOpalTableRef(SchemaTableName schemaTableName) {
    return opalDatasourceMap.get(schemaTableName.getSchemaName()).getName() + "." + opalTableNameMap.get(schemaTableName);
  }

}
