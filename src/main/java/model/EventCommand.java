package model;

public class EventCommand {
	private Device device;

	public EventCommand(Device device) {
		super();
		this.device = device;
	}

	public Device getDevice() {
		System.out.println("getDevice called for the device " + device.getId());
		return device;
	}
	
	public String getDeviceIP() {
		System.out.println("getDevice called for the device " + device.getId());
		return device.getIp();
	}
	
	public String getHash(int id) {
		System.out.println("getHash called for the device " + id);
		if (id >= 1 &&  id<= 10) return "1";
		else 	if (id >= 11 &&  id<= 20) return "2";
		else 	if (id >= 21 &&  id<= 30) return "3";
		return null;
	}
}