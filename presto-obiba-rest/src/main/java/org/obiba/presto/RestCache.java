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

import java.util.Date;

public class RestCache<T> {
  private final T item;
  private final int delaySec;
  private final long createdAt;

  public RestCache(T item, int delaySec) {
    this.item = item;
    this.delaySec = delaySec;
    this.createdAt = now();
  }

  public T getItem() {
    return item;
  }

  public boolean hasExpired() {
    return now()>(createdAt + delaySec);
  }

  /**
   * Now in seconds since EPOCH.
   *
   * @return
   */
  private long now() {
    return new Date().getTime()/1000;
  }
}
