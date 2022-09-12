package model;

public class Event {
	private String description;
	private String deviceIp;
	private int id;
	private String time;
	
	public Event(String description, String deviceIp, int id, String time) {
		super();
		this.description = description;
		this.deviceIp = deviceIp;
		this.id = id;
		this.time = time;
	}
	public String getDescription() {
		return description;
	}
	public String getDeviceIp() {
		return deviceIp;
	}
	public int getId() {
		return id;
	}
	public String getTime() {
		return time;
	}
	@Override
	public String toString() {
		return "Event [description=" + description + ", deviceIp=" + deviceIp + ", id=" + id + ", time=" + time + "]";
	}
}
