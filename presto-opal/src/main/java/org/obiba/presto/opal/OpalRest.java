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

import org.obiba.presto.Rest;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.util.Base64;

public abstract class OpalRest implements Rest {

  private final String opalUrl;
  protected final String token;
  protected final OpalService service;

  public OpalRest(String url, String username, String password) {
    this.opalUrl = url;
    // TODO login and use session id instead of authenticating at each request
    this.token = "X-Opal-Auth " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
    this.service = new Retrofit.Builder()
        .baseUrl(url)
        .addConverterFactory(JacksonConverterFactory.create())
        .build()
        .create(OpalService.class);
  }


}
