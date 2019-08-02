package es.redmic.elasticsearchlib.config;

/*-
 * #%L
 * elasticsearch-lib
 * %%
 * Copyright (C) 2019 REDMIC Project / Server
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.IOException;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestClientBuilder.RequestConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import es.redmic.exception.common.ExceptionType;
import es.redmic.exception.common.InternalException;

@Configuration
public class EsClientProvider {

	private RestHighLevelClient client;

	@Value("#{'${elastic.addresses}'.split(',')}")
	private List<String> addresses;
	@Value("${elastic.port}")
	private Integer port;
	@Value("${elastic.user}")
	private String user;
	@Value("${elastic.password}")
	private String password;

	int timeout = 60000;

	protected static Logger logger = LogManager.getLogger();

	public EsClientProvider() {
	}

	public RestHighLevelClient getClient() {
		if (client == null)
			connect();
		return client;
	}

	@PostConstruct
	private void connect() {

		HttpHost[] hosts = new HttpHost[addresses.size()];
		int it = 0;
		for (String address : addresses) {
			hosts[it] = new HttpHost(address, port, "http");
			it++;
		}

		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));

		client = new RestHighLevelClient(RestClient.builder(hosts).setRequestConfigCallback(getRequestConfigCallback())
				.setMaxRetryTimeoutMillis(timeout)
				.setHttpClientConfigCallback(getHttpClientConfigCallback(credentialsProvider)));

		checkClusterHealth();
	}

	private RequestConfigCallback getRequestConfigCallback() {

		return new RestClientBuilder.RequestConfigCallback() {
			@Override
			public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
				return requestConfigBuilder.setSocketTimeout(timeout);
			}
		};
	}

	private HttpClientConfigCallback getHttpClientConfigCallback(CredentialsProvider credentialsProvider) {
		return new HttpClientConfigCallback() {

			private static final int KEEP_ALIVE_MS = 20 * 60 * 1000; // 20 minutes

			@Override
			public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
				return httpClientBuilder.setKeepAliveStrategy((response, context) -> KEEP_ALIVE_MS)
						.setDefaultCredentialsProvider(credentialsProvider);
			}
		};
	}

	private void checkClusterHealth() {

		ClusterHealthResponse response;

		try {
			response = client.cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
		} catch (IOException e) {
			e.printStackTrace();
			throw new InternalException(ExceptionType.INTERNAL_EXCEPTION);
		}

		if (response.getStatus().equals(ClusterHealthStatus.RED)) {
			logger.error("Imposible conectar con elastic. Cluster no saludable");
			throw new InternalException(ExceptionType.INTERNAL_EXCEPTION);
		}
	}

	@PreDestroy
	private void disconnect() throws IOException {
		client.close();
	}
}
