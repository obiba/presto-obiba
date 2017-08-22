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

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class OpalVariablesQueriesTest
    extends AbstractTestQueryFramework {
  protected OpalVariablesQueriesTest()
      throws Exception {
    super(createLocalQueryRunner());
  }

  public static QueryRunnerSupplier createLocalQueryRunner()
      throws Exception {
    Session defaultSession = testSessionBuilder()
        .setCatalog("demo")
        .setSchema("datashield")
        .build();

    QueryRunner queryRunner = new DistributedQueryRunner(defaultSession, 1);
    queryRunner.installPlugin(new OpalPlugin());

    queryRunner.createCatalog(
        "demo",
        "opal",
        ImmutableMap.of("opal.url", "https://opal-demo.obiba.org/",
            "opal.catalog-type", "variables",
            "opal.username", "administrator",
            "opal.password", "password"));

    return new QueryRunnerSupplier() {
      @Override
      public QueryRunner get() throws Exception {
        return queryRunner;
      }
    };
  }

  @Test
  public void showSchemas() {
    assertQuery("SHOW SCHEMAS FROM demo", "VALUES 'cls','clsa','cptp','datashield','fnac','frele','heliad','information_schema','lasa','lbls','nuage','path','ship','ulsam'");
  }

  @Test
  public void showTables() {
    assertQuery("SHOW TABLES FROM demo.datashield", "VALUES 'cnsim1','cnsim2','cnsim3'");
  }

  @Test
  public void showColumns() {
    MaterializedResult result = computeActual("SHOW COLUMNS FROM demo.datashield.cnsim1");
    // variables properties
    Assert.assertEquals(result.getRowCount(), 38);
    // TODO check data types
  }

  @Test
  public void selectAllFromDefault() {
    computeActual("SELECT * FROM cnsim1");
  }

  @Test
  public void selectColumnFromDefault() {
    MaterializedResult result = computeActual("SELECT name FROM cnsim1 LIMIT 10");
    Assert.assertEquals(result.getRowCount(), 10);
  }

}
