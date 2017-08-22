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

public class OpalSystemQueriesTest
    extends AbstractTestQueryFramework {
  protected OpalSystemQueriesTest()
      throws Exception {
    super(createLocalQueryRunner());
  }

  public static QueryRunnerSupplier createLocalQueryRunner()
      throws Exception {
    Session defaultSession = testSessionBuilder()
        .setCatalog("demo")
        .setSchema("taxonomies")
        .build();

    QueryRunner queryRunner = new DistributedQueryRunner(defaultSession, 1);
    queryRunner.installPlugin(new OpalPlugin());

    queryRunner.createCatalog(
        "demo",
        "opal",
        ImmutableMap.of("opal.url", "https://opal-demo.obiba.org/",
            "opal.catalog-type", "system",
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
    assertQuery("SHOW SCHEMAS FROM demo", "VALUES 'taxonomies','information_schema'");
  }

  @Test
  public void showTables() {
    assertQuery("SHOW TABLES FROM demo.taxonomies", "VALUES 'taxonomy','vocabulary','term'");
  }

  @Test
  public void showColumns() {
    MaterializedResult result = computeActual("SHOW COLUMNS FROM demo.taxonomies.taxonomy");
    Assert.assertEquals(result.getRowCount(), 6);
    // TODO check data types
  }

  @Test
  public void selectAllFromDefault() {
    computeActual("SELECT * FROM taxonomy");
  }

  @Test
  public void selectColumnFromDefault() {
    MaterializedResult result = computeActual("SELECT name FROM vocabulary LIMIT 10");
    Assert.assertEquals(result.getRowCount(), 10);
  }

}
