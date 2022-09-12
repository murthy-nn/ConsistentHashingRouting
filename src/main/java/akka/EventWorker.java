package akka;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Receive;
import model.EventCommand;
import model.Device;
import akka.actor.typed.javadsl.Behaviors;

/* One worker per network device.
 */
public class EventWorker extends AbstractBehavior<EventCommand> {

	boolean processEventFlag = true;

	private EventWorker(ActorContext<EventCommand> context) {
		super(context);
		getContext().getLog().debug("constructer");
	}

	public static Behavior<EventCommand> create() {
		return Behaviors.setup(EventWorker::new);
	}

	@Override
	public Receive<EventCommand> createReceive() {
		getContext().getLog().debug("createReceive " + getContext().getSelf());
		return newReceiveBuilder()
				.onMessage(StartEventProcessing.class, this::onStartEventProcessing)
				.onMessage(StopEventProcessing.class, this::onStopEventProcessing)
				.build();
	}

	private Behavior<EventCommand> onStartEventProcessing (StartEventProcessing command) throws InterruptedException {
		getContext().getLog().debug("onStartEventProcessing " + getContext().getSelf());
		int delay = 10; //in seconds
		while (processEventFlag) {
			getContext().getLog().debug("Awaiting Event from the device " + command.getDevice().getId() +
					". " + getContext().getSelf());
            Thread.sleep(delay*1000);

			//TODO: Make a blocking call to receive event one at a time from the network device
		} // EOF infinite loop
		
		if (processEventFlag == false) {
			getContext().getLog().debug("Closing all session with the network device");
			//TODO: Close all the session with the network device
		}
		return this;
	}

	private Behavior<EventCommand> onStopEventProcessing (StopEventProcessing command) {
		getContext().getLog().debug("onStopEventProcessing " + getContext().getSelf());
		processEventFlag = false;
		getContext().getLog().debug("processEventFlag= " + processEventFlag + ",  " + getContext().getSelf());
		return this;
	}

	/******** Message or command definition ********/
	//Message or command definition
	public static class StartEventProcessing extends EventCommand {
		private static final long serialVersionUID = 1L;
		public StartEventProcessing(Device device) {
			super(device);
		}
		public static long getSerialversionuid() {
			return serialVersionUID;
		}
	}

	public static class StopEventProcessing extends EventCommand {
		private static final long serialVersionUID = 1L;

		public StopEventProcessing(Device device) {
			super(device);
		}
		public static long getSerialversionuid() {
			return serialVersionUID;
		}
	}
}