package es.redmic.elasticsearchlib.config;

import java.io.IOException;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
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

		client = new RestHighLevelClient(
				RestClient.builder(hosts).setHttpClientConfigCallback(new HttpClientConfigCallback() {
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				}));

		checkClusterHealth();
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
