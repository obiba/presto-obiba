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

import com.facebook.presto.spi.PrestoException;
import org.obiba.presto.Rest;
import org.obiba.presto.RestCache;
import org.obiba.presto.opal.model.OpalConf;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.IOException;
import java.util.Base64;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public abstract class OpalRest implements Rest {

  private final String opalUrl;
  protected final int cacheDelay;
  protected final String token;
  protected final OpalService service;
  protected RestCache<OpalConf> opalConfCache;

  public OpalRest(String url, String username, String password) {
    this(url, username, password, 300);
  }

  public OpalRest(String url, String username, String password, int cacheDelay) {
    this.opalUrl = url;
    this.cacheDelay = cacheDelay;
    // TODO login and use session id instead of authenticating at each request
    this.token = "X-Opal-Auth " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
    this.service = new Retrofit.Builder()
        .baseUrl(url)
        .addConverterFactory(JacksonConverterFactory.create())
        .build()
        .create(OpalService.class);
  }

  protected synchronized void initialize() {
    initializeOpalConf();
  }

  private void initializeOpalConf() {
    if (opalConfCache != null && !opalConfCache.hasExpired()) return;
    opalConfCache = null;
    try {
      Response<OpalConf> response = service.getOpalConf(token).execute();
      if (!response.isSuccessful())
        throw new IllegalStateException("Unable to read opal datasources: " + response.message());
      opalConfCache = new RestCache<>(response.body(), cacheDelay);
    } catch (IOException e) {
      throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
    }
  }

}
