package es.redmic.elasticsearchlib.config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
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
	@Value("${elastic.secured}")
	private Boolean secured;
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

		String authorization = "";
		if (secured)
			authorization = user + ":" + password + "@";

		List<HttpHost> hosts = new ArrayList<>();
		for (String address : addresses) {

			hosts.add(new HttpHost(authorization + address, port, "http"));
		}

		client = new RestHighLevelClient(RestClient.builder(hosts.toArray(new HttpHost[hosts.size()])));

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
