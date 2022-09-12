package model;

public class Device {
	int id; // Unique id, temporarily introduced in place of IP address
	String ip; //IP address
	String userId;
	String password;
	public Device(int id, String ip, String userId, String password) {
		super();
		this.id = id;
		this.ip = ip;
		this.userId = userId;
		this.password = password;
	}
	public int getId() {
		System.out.println("getId called for the device " + id);
		return id;
	}
	public String getIp() {
		return ip;
	}
	public String getUserId() {
		return userId;
	}
	public String getPassword() {
		return password;
	}
	@Override
	public String toString() {
		return "Device [id=" + id + ", ip=" + ip + ", userId=" + userId + ", password=" + password + "]";
	}
}