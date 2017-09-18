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

import org.obiba.presto.opal.model.Datasource;
import org.obiba.presto.opal.model.ValueTable;
import org.obiba.presto.opal.model.Variable;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.IOException;
import java.util.Base64;
import java.util.List;

public class OpalServiceTest {

  private OpalService service;

  @BeforeTest
  public void setup() {
    service = new Retrofit.Builder()
        .baseUrl("https://opal-demo.obiba.org/")
        .addConverterFactory(JacksonConverterFactory.create())
        .build()
        .create(OpalService.class);
  }

  @Test
  public void listDatasources() throws IOException {
    Response<List<Datasource>> response = service.listDatasources(getOpalAuth()).execute();
    if (!response.isSuccessful()) Assert.fail();
    List<Datasource> datasources = response.body();
    Assert.assertEquals(datasources.size(), 15);
  }

  @Test
  public void getDatasource() throws IOException {
    Response<Datasource> response = service.getDatasource(getOpalAuth(), "datashield").execute();
    if (!response.isSuccessful()) Assert.fail();
    Datasource datasource = response.body();
    Assert.assertEquals(datasource.getName(), "datashield");
  }

  @Test
  public void listTables() throws IOException {
    Response<List<ValueTable>> response = service.listTables(getOpalAuth(), "datashield", false).execute();
    if (!response.isSuccessful()) Assert.fail();
    List<ValueTable> tables = response.body();
    Assert.assertEquals(tables.size(), 3);
  }

  @Test
  public void getTable() throws IOException {
    Response<ValueTable> response = service.getTable(getOpalAuth(), "datashield", "CNSIM1", false).execute();
    if (!response.isSuccessful()) Assert.fail();
    ValueTable table = response.body();
    Assert.assertEquals(table.getDatasourceName(), "datashield");
    Assert.assertEquals(table.getName(), "CNSIM1");
    Assert.assertEquals(table.getEntityType(), "Participant");
  }

  @Test
  public void listVariables() throws IOException {
    Response<List<Variable>> response = service.listVariables(getOpalAuth(), "datashield", "CNSIM1").execute();
    if (!response.isSuccessful()) Assert.fail();
    List<Variable> variables = response.body();
    Assert.assertEquals(variables.size(), 11);
  }

  @Test
  public void getVariable() throws IOException {
    Response<Variable> response = service.getVariable(getOpalAuth(), "datashield", "CNSIM1", "GENDER").execute();
    if (!response.isSuccessful()) Assert.fail();
    Variable variable = response.body();
    Assert.assertEquals(variable.getName(), "GENDER");
    Assert.assertEquals(variable.getEntityType(), "Participant");
    Assert.assertEquals(variable.getValueType(), "integer");
    Assert.assertEquals(variable.isRepeatable(), false);
  }

  private String getOpalAuth() {
    return "X-Opal-Auth " + Base64.getEncoder().encodeToString("administrator:password".getBytes());
  }
}
