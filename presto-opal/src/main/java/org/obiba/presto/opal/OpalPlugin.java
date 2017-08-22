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
import org.obiba.presto.opal.system.OpalSystemRest;
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
    if ("values".equals(catalogType))
      return new OpalValuesRest(config.get("opal.url"), config.get("opal.username"), config.get("opal.password"));
    if ("variables".equals(catalogType))
      return new OpalVariablesRest(config.get("opal.url"), config.get("opal.username"), config.get("opal.password"));
    if ("system".equals(catalogType))
      return new OpalSystemRest(config.get("opal.url"), config.get("opal.username"), config.get("opal.password"));
    return new OpalValuesRest(config.get("opal.url"), config.get("opal.username"), config.get("opal.password"));
  }
}
