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
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.util.List;
import java.util.stream.Collectors;

public class RestPageSourceProvider implements ConnectorPageSourceProvider {
  private final Rest rest;

  public RestPageSourceProvider(Rest rest) {
    this.rest = rest;
  }

  @Override
  public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit connectorSplit, List<ColumnHandle> columns) {
    RestConnectorSplit split = Types.checkType(connectorSplit, RestConnectorSplit.class, "split");
    List<RestColumnHandle> restColumnHandles = columns.stream().map(col -> Types.checkType(col, RestColumnHandle.class, "column handle")).collect(Collectors.toList());

    return new RestPageSource(rest, split, restColumnHandles);
  }
}
