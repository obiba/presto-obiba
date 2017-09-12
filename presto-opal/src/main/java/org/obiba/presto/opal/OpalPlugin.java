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

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;
import org.obiba.presto.RestConnectorFactory;
import org.obiba.presto.opal.administration.OpalAdministrationRest;
import org.obiba.presto.opal.values.OpalValuesRest;
import org.obiba.presto.opal.variables.OpalVariablesRest;

import java.util.Map;

public class OpalPlugin implements Plugin {

  @Override
  public Iterable<ConnectorFactory> getConnectorFactories() {
    return ImmutableList.of(new RestConnectorFactory("opal", this::createRestFactory));
  }

  private OpalRest createRestFactory(Map<String, String> config) {
    String catalogType = config.getOrDefault("opal.catalog-type", "values");
    String opalUrl = config.get("opal.url");
    String username = config.get("opal.username");
    String password = config.get("opal.password");
    int delay = Integer.parseInt(config.getOrDefault("opal.cache-delay", "300"));
    if ("values".equals(catalogType))
      return new OpalValuesRest(opalUrl, username, password, delay);
    if ("variables".equals(catalogType))
      return new OpalVariablesRest(opalUrl, username, password, delay);
    if ("system".equals(catalogType) || "administration".equals(catalogType))
      return new OpalAdministrationRest(opalUrl, username, password, delay);
    return new OpalValuesRest(opalUrl, username, password, delay);
  }
}
