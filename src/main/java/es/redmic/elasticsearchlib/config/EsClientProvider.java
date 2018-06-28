package es.redmic.elasticsearchlib.config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import es.redmic.exception.common.ExceptionType;
import es.redmic.exception.common.InternalException;

public class EsClientProvider {

	private TransportClient client;

	private List<String> addresses;
	private Integer port;
	private String clusterName;

	protected static Logger logger = LogManager.getLogger();

	public EsClientProvider(EsConfig config) {
		this.addresses = config.getAddresses();
		this.port = config.getPort();
		this.clusterName = config.getClusterName();
	}

	public TransportClient getClient() {
		if (client == null)
			connect();
		return client;
	}

	@PostConstruct
	private void connect() {

		Settings settings = Settings.builder().put("cluster.name", this.clusterName).build();

		client = new PreBuiltTransportClient(settings);

		for (String address : addresses) {
			try {
				client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(address), port));
			} catch (UnknownHostException e) {
				logger.warn(e.getMessage());
			}
		}

		List<DiscoveryNode> nodes = client.connectedNodes();
		if (nodes == null || nodes.isEmpty()) {
			// TODO: Añadir excepción propia
			throw new InternalException(ExceptionType.INTERNAL_EXCEPTION);
		}
	}

	@PreDestroy
	private void disconnect() {
		client.close();
	}

}
