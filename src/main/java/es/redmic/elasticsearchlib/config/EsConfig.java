package es.redmic.elasticsearchlib.config;

import java.util.List;

public class EsConfig {

	private List<String> addresses;
	private Integer port;
	private String clusterName;
	private String user;
	private String password;

	public EsConfig() {
	}

	public EsConfig(List<String> addresses, Integer port, String clusterName, String user, String password) {
		this.addresses = addresses;
		this.port = port;
		this.clusterName = clusterName;
		this.user = user;
		this.password = password;
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

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
}
