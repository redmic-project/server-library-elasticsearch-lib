package es.redmic.elasticsearchlib.config;

import java.util.List;

public class EsConfig {

	private List<String> addresses;
	private Integer port;
	private String clusterName;
	private String xpackSecurityUser;

	public EsConfig() {
	}

	public EsConfig(List<String> addresses, Integer port, String clusterName, String xpackSecurityUser) {
		this.addresses = addresses;
		this.port = port;
		this.clusterName = clusterName;
		this.setXpackSecurityUser(xpackSecurityUser);
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

	public String getXpackSecurityUser() {
		return xpackSecurityUser;
	}

	public void setXpackSecurityUser(String xpackSecurityUser) {
		this.xpackSecurityUser = xpackSecurityUser;
	}
}
