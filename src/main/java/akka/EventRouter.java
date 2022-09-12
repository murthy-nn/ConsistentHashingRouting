package akka;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.GroupRouter;
import akka.actor.typed.javadsl.PoolRouter;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.Routers;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import model.EventCommand;
import model.Device;

public class EventRouter extends AbstractBehavior<EventCommand> {	
	int poolSize = 5;
	private ActorRef<EventCommand> worker;

	private EventRouter(ActorContext<EventCommand> context) {
		super(context);
		getContext().getLog().debug("constructor");
		groupRouter_v1(context);
		//poolRouter(context);
	}
	
	void groupRouter_v1 (ActorContext<EventCommand> context) {
		/*The group router is created with a ServiceKey and uses the receptionist to discover 
		 * available actors for that key and routes messages to one of the currently known 
		 * registered actors for a key.
		 */
		//TODO: Let us create a single ActorSystem per application.
		getContext().getLog().debug("groupRouter_v1");
		getContext().getLog().debug("Event ServiceKey created");
		worker = ActorSystem.create(EventWorker.create(), "EventWorker");
		ServiceKey<EventCommand> serviceKey = ServiceKey.create(EventCommand.class, "Event-sk");
		getContext().getLog().debug("Event router created with poolSize of " + poolSize);
		GroupRouter<EventCommand> group = Routers.group(serviceKey).
				withConsistentHashingRouting(poolSize, msg -> msg.getHash(msg.getDevice().getId()));
		ActorSystem<EventCommand> as = ActorSystem.create(group, "EventActorSystem");
		as.receptionist().tell(Receptionist.register(serviceKey, worker));
		getContext().getLog().debug("Event worker is registered with the Receptionist");
	}
	
	void groupRouter_v1a (ActorContext<EventCommand> context) {
		//v1 vs v1a: ActorSystem.create is replaced with getContext().spawn		
		/*The group router is created with a ServiceKey and uses the receptionist to discover 
		 * available actors for that key and routes messages to one of the currently known 
		 * registered actors for a key.
		 */
		getContext().getLog().debug("groupRouter_v1a");
		getContext().getLog().debug("Event ServiceKey created");
		worker = getContext().spawn(EventWorker.create(), "EventWorker");
		ServiceKey<EventCommand> serviceKey = ServiceKey.create(EventCommand.class, "Event-sk");
		getContext().getLog().debug("Event router created with poolSize of " + poolSize);
		GroupRouter<EventCommand> group = Routers.group(serviceKey).
				withConsistentHashingRouting(poolSize, msg -> msg.getHash(msg.getDevice().getId()));
		
		ActorSystem<EventCommand> as = ActorSystem.create(group, "EventActorSystem");
		as.receptionist().tell(Receptionist.register(serviceKey, worker));
		getContext().getLog().debug("Event worker is registered with the Receptionist");
	}

	void poolRouter(ActorContext<EventCommand> context) {
		//Pool router approach
		getContext().getLog().debug("poolRouter");
		getContext().getLog().debug("Event router created with poolSize of " + poolSize);
		PoolRouter<EventCommand> router = Routers.pool(
				poolSize,
				Behaviors.supervise(EventWorker.create()).onFailure(SupervisorStrategy.restart())
		);
		worker = getContext().spawn(router, "EventWorker");
	}

	public static Behavior<EventCommand> create() {
		return Behaviors.setup(EventRouter::new);
	}

	@Override
	public Receive<EventCommand> createReceive() {
		getContext().getLog().debug("createReceive");
		return newReceiveBuilder()
				.onMessage(StartEventProcessing.class, this::onStartEventProcessing)
				.onMessage(StopEventProcessing.class, this::onStopEventProcessing)
				.build();
	}

	private Behavior<EventCommand> onStartEventProcessing(StartEventProcessing command) {
		getContext().getLog().debug("onStartEventProcessing. worker=" + worker);
		worker.tell(new EventWorker.StartEventProcessing(command.getDevice()));
		return this;
	}

	private Behavior<EventCommand> onStopEventProcessing(StopEventProcessing command) {
		/* Route onStopEventProcessing message to an existing EventWorker based on  DeviceIP
		 */
		getContext().getLog().debug("onStopEventProcessing. worker=" + worker);

		worker.tell(new EventWorker.StopEventProcessing(command.getDevice()));
		return this;
	}
	
	/******** Message or command definition ********/
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