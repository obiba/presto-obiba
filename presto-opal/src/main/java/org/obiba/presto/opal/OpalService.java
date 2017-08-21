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

import org.obiba.presto.opal.model.OpalDatasource;
import org.obiba.presto.opal.model.OpalTable;
import org.obiba.presto.opal.model.OpalValueSets;
import org.obiba.presto.opal.model.OpalVariable;
import retrofit2.Call;
import retrofit2.http.*;

import java.util.List;

public interface OpalService {

  @Headers({"Accept: application/json"})
  @GET("/ws/datasources")
  Call<List<OpalDatasource>> listDatasources(@Header("Authorization") String opalAuth);

  @Headers({"Accept: application/json"})
  @GET("/ws/datasource/{name}")
  Call<OpalDatasource> getDatasource(@Header("Authorization") String opalAuth, @Path("name") String name);

  @Headers({"Accept: application/json"})
  @GET("/ws/datasource/{name}/tables")
  Call<List<OpalTable>> listTables(@Header("Authorization") String opalAuth, @Path("name") String name);

  @Headers({"Accept: application/json"})
  @GET("/ws/datasource/{ds}/table/{name}")
  Call<OpalTable> getTable(@Header("Authorization") String opalAuth, @Path("ds") String datasource, @Path("name") String name);

  @Headers({"Accept: application/json"})
  @GET("/ws/datasource/{ds}/table/{name}/variables")
  Call<List<OpalVariable>> listVariables(@Header("Authorization") String opalAuth, @Path("ds") String datasource, @Path("name") String name);

  @Headers({"Accept: application/json"})
  @GET("/ws/datasource/{ds}/table/{tbl}/variable/{name}")
  Call<OpalVariable> getVariable(@Header("Authorization") String opalAuth, @Path("ds") String datasource, @Path("tbl") String table, @Path("name") String name);

  @Headers({"Accept: application/json"})
  @GET("/ws/datasource/{ds}/table/{name}/valueSets")
  Call<OpalValueSets> listValueSets(@Header("Authorization") String opalAuth, @Path("ds") String datasource, @Path("name") String name, @Query("offset") int offset, @Query("limit") int limit);

}
