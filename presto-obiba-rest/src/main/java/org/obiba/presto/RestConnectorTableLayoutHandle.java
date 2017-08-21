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

package org.obiba.presto;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class RestConnectorTableLayoutHandle
    implements ConnectorTableLayoutHandle {
  private final RestTableHandle tableHandle;
  private final TupleDomain<ColumnHandle> tupleDomain;

  @JsonCreator
  public RestConnectorTableLayoutHandle(@JsonProperty("tableHandle") RestTableHandle tableHandle,
                                        @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain) {
    this.tableHandle = tableHandle;
    this.tupleDomain = tupleDomain;
  }

  @JsonProperty("tableHandle")
  public RestTableHandle getTableHandle() {
    return tableHandle;
  }

  @JsonProperty
  public TupleDomain<ColumnHandle> getTupleDomain()
  {
    return tupleDomain;
  }


  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RestConnectorTableLayoutHandle that = (RestConnectorTableLayoutHandle) o;
    return Objects.equals(tableHandle, that.tableHandle) &&
        Objects.equals(tupleDomain, that.tupleDomain);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(tableHandle, tupleDomain);
  }
}
