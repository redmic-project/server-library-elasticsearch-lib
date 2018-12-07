package es.redmic.elasticsearchlib.config;

import java.util.List;

public class EsConfig {

	private List<String> addresses;
	private Integer port;
	private String clusterName;

	public EsConfig() {
	}

	public EsConfig(List<String> addresses, Integer port, String clusterName) {
		this.addresses = addresses;
		this.port = port;
		this.clusterName = clusterName;
	}

	public List<String> getAddresses() {
		return addresses;
	}

	public void setAddresses(List<String> addresses) {
		this.addresses = addresses;
	}

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}
}
