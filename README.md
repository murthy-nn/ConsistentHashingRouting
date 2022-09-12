# ConsistentHashingRouting
Summary: This appliction use ConsistentHashingRouting for routing events from a specific Network device/source to same Akka worker based on some unique id such as Device IP address, Device Id.

Description:
The EventRouter sends StartEventProcessing message to EventWorker to start processing the events that are received from a specific network device. Inside a infinite loop, the worker will make a blocking call to the Network device to receive a single event.

In order to stop processing the events for a specific network device, the EventRouter sends StopEventProcessing message to EventWorker. The application uses ConsistentHashingRouting to route this message to an appropriate EventWorker which is processing the event from the network device.
