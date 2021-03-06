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

import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.obiba.presto.RestColumnHandle;
import org.obiba.presto.opal.OpalRest;
import org.obiba.presto.opal.model.Database;
import org.obiba.presto.opal.model.PluginPackages;
import org.obiba.presto.opal.model.Project;
import org.obiba.presto.opal.model.Taxonomy;
import retrofit2.Response;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class OpalAdministrationRest extends OpalRest {

  // schema table name vs. columns
  private Map<SchemaTableName, ConnectorTableMetadata> connectorTableMap = Maps.newConcurrentMap();

  public OpalAdministrationRest(String url, String username, String password, int cacheDelay) {
    super(url, username, password, cacheDelay);
  }

  @Override
  public synchronized ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName) {
    initialize();
    if (connectorTableMap.containsKey(schemaTableName)) return connectorTableMap.get(schemaTableName);
    ConnectorTableMetadata connectorTableMetadata;
    if (TaxonomiesTable.NAME.equals(schemaTableName.getTableName()))
      connectorTableMetadata = new TaxonomiesTable(schemaTableName, opalConfCache);
    else if (VocabulariesTable.NAME.equals(schemaTableName.getTableName()))
      connectorTableMetadata = new VocabulariesTable(schemaTableName, opalConfCache);
    else if (TermsTable.NAME.equals(schemaTableName.getTableName()))
      connectorTableMetadata = new TermsTable(schemaTableName, opalConfCache);
    else if (DatabasesTable.NAME.equals(schemaTableName.getTableName()))
      connectorTableMetadata = new DatabasesTable(schemaTableName);
    else if (PluginsTable.NAME.equals(schemaTableName.getTableName()))
      connectorTableMetadata = new PluginsTable(schemaTableName);
    else if (ProjectsTable.NAME.equals(schemaTableName.getTableName()))
      connectorTableMetadata = new ProjectsTable(schemaTableName);
    else
      throw new RuntimeException("Unknown opal system schema table: " + schemaTableName);
    connectorTableMap.put(schemaTableName, connectorTableMetadata);
    return connectorTableMetadata;
  }

  @Override
  public List<String> listSchemas() {
    return ImmutableList.of("system");
  }

  @Override
  public List<SchemaTableName> listTables(String schema) {
    if ("system".equals(schema))
      return ImmutableList.of(new SchemaTableName(schema, TaxonomiesTable.NAME),
          new SchemaTableName(schema, VocabulariesTable.NAME),
          new SchemaTableName(schema, TermsTable.NAME),
          new SchemaTableName(schema, DatabasesTable.NAME),
          new SchemaTableName(schema, PluginsTable.NAME),
          new SchemaTableName(schema, ProjectsTable.NAME));
    else
      return Lists.newArrayList();
  }

  @Override
  public Collection<? extends List<?>> getRows(SchemaTableName schemaTableName, List<RestColumnHandle> restColumnHandles) {
    initialize();
    if (DatabasesTable.NAME.equals(schemaTableName.getTableName())) {
      try {
        Response<List<Database>> execute = service.listDatabases(token).execute();
        if (!execute.isSuccessful())
          throw new IllegalStateException("Unable to read databases: " + execute.message());
        List<String> columnNames = restColumnHandles.stream().map(RestColumnHandle::getName).collect(Collectors.toList());
        return DatabasesTable.getRows(columnNames, execute.body());
      } catch (IOException e) {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
      }
    }
    else if (PluginsTable.NAME.equals(schemaTableName.getTableName())) {
      try {
        Response<PluginPackages> execute = service.getPluginPackages(token).execute();
        if (!execute.isSuccessful())
          throw new IllegalStateException("Unable to read databases: " + execute.message());
        List<String> columnNames = restColumnHandles.stream().map(RestColumnHandle::getName).collect(Collectors.toList());
        return PluginsTable.getRows(columnNames, execute.body());
      } catch (IOException e) {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
      }
    }
    else if (ProjectsTable.NAME.equals(schemaTableName.getTableName())) {
      try {
        Response<List<Project>> execute = service.listProjects(token).execute();
        if (!execute.isSuccessful())
          throw new IllegalStateException("Unable to read databases: " + execute.message());
        List<String> columnNames = restColumnHandles.stream().map(RestColumnHandle::getName).collect(Collectors.toList());
        return ProjectsTable.getRows(columnNames, execute.body());
      } catch (IOException e) {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
      }
    }
    return getTaxonomiesRows(schemaTableName, restColumnHandles);
  }

  private Collection<? extends List<?>> getTaxonomiesRows(SchemaTableName schemaTableName, List<RestColumnHandle> restColumnHandles) {
    try {
      Response<List<Taxonomy>> execute = service.listTaxonomies(token).execute();
      if (!execute.isSuccessful())
        throw new IllegalStateException("Unable to read taxonomies: " + execute.message());
      List<String> columnNames = restColumnHandles.stream().map(RestColumnHandle::getName).collect(Collectors.toList());
      if (TaxonomiesTable.NAME.equals(schemaTableName.getTableName()))
        return TaxonomiesTable.getRows(columnNames, execute.body());
      if (VocabulariesTable.NAME.equals(schemaTableName.getTableName()))
        return VocabulariesTable.getRows(columnNames, execute.body());
      if (TermsTable.NAME.equals(schemaTableName.getTableName()))
        return TermsTable.getRows(columnNames, execute.body());
    } catch (IOException e) {
      throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
    }
    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unknown opal system schema table: " + schemaTableName);
  }

}
