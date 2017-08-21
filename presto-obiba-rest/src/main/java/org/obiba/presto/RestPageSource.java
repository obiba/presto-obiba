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

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;

import java.io.IOException;
import java.util.List;

public class RestPageSource implements ConnectorPageSource {

  private final Rest rest;
  private final RestConnectorSplit split;
  private final List<RestColumnHandle> columnHandles;

  private long count;
  private long totalCount;
  private boolean finished;

  public RestPageSource(Rest rest, RestConnectorSplit split, List<RestColumnHandle> columnHandles) {
    this.rest = rest;
    this.split = split;
    this.columnHandles = columnHandles;
  }

  @Override
  public long getTotalBytes() {
    return totalCount;
  }

  @Override
  public long getCompletedBytes() {
    return count;
  }

  @Override
  public long getReadTimeNanos() {
    return 0;
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public Page getNextPage() {
    return null;
  }

  @Override
  public long getSystemMemoryUsage() {
    return 0L;
  }

  @Override
  public void close() throws IOException {

  }
}
