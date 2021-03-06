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

public class OpalAdministrationQueriesTest
    extends AbstractTestQueryFramework {
  protected OpalAdministrationQueriesTest()
      throws Exception {
    super(createLocalQueryRunner());
  }

  public static QueryRunnerSupplier createLocalQueryRunner()
      throws Exception {
    Session defaultSession = testSessionBuilder()
        .setCatalog("demo")
        .setSchema("system")
        .build();

    QueryRunner queryRunner = new DistributedQueryRunner(defaultSession, 1);
    queryRunner.installPlugin(new OpalPlugin());

    queryRunner.createCatalog(
        "demo",
        "opal",
        ImmutableMap.of("opal.url", "https://opal-demo.obiba.org/",
            "opal.catalog-type", "administration",
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
    assertQuery("SHOW SCHEMAS FROM demo", "VALUES 'system','information_schema'");
  }

  @Test
  public void showTables() {
    assertQuery("SHOW TABLES FROM demo.system", "VALUES 'databases','plugins','projects','taxonomies','vocabularies','terms'");
  }

  @Test
  public void showColumns() {
    MaterializedResult result = computeActual("SHOW COLUMNS FROM demo.system.taxonomies");
    Assert.assertEquals(result.getRowCount(), 6);
    // TODO check data types
  }

  @Test
  public void selectAllFromDefault() {
    computeActual("SELECT * FROM taxonomies");
  }

  @Test
  public void selectColumnFromDefault() {
    MaterializedResult result = computeActual("SELECT name FROM vocabularies LIMIT 10");
    Assert.assertEquals(result.getRowCount(), 10);
  }

}
