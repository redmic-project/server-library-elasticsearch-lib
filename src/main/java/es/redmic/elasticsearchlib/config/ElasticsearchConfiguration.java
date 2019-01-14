package es.redmic.elasticsearchlib.config;

import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchConfiguration {

	@Value("#{'${elastic.addresses}'.split(',')}")
	private List<String> addresses;
	@Value("${elastic.port}")
	private Integer port;
	@Value("${elastic.clusterName}")
	private String clusterName;
	@Value("${elastic.user}")
	private String user;
	@Value("${elastic.password}")
	private String password;

	@Bean
	public EsClientProvider esClientProvider() {

		EsConfig elastic = new EsConfig();
		elastic.setAddresses(addresses);
		elastic.setPort(port);
		elastic.setClusterName(clusterName);
		elastic.setUser(user);
		elastic.setPassword(password);
		return new EsClientProvider(elastic);
	}
}
