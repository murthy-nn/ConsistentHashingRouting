package akka;

import java.time.Duration;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;
import model.EventCommand;
import model.Device;

public class App {
	public static void main(String[] args) {
		System.out.println(">> MyApp started....");
		ActorSystem<EventCommand> actorSystem = 
				ActorSystem.create(EventRouter.create(), "MyApp");

		Device device1 = new Device (1, "192.168.56.101", "admin", "admin");
		send_messages (actorSystem, device1);
		
		Device device2 = new Device (11, "192.168.56.102", "admin", "admin");
		send_messages (actorSystem, device2);		
		
//		Device device3 = new Device (21, "192.168.56.103", "admin", "admin");
//		send_messages (actorSystem, device3);		
	}
	
	static void send_messages(ActorSystem<EventCommand> actorSystem, Device device) {
		/*In reality, following messages would be sent from different entities
		 * at different point in time
		 */
		AskPattern.ask(actorSystem,
				me -> new EventRouter.StartEventProcessing(device),
				Duration.ofSeconds(30),
				actorSystem.scheduler());
		System.out.println(">> StartEventProcessing message sent");
		
		AskPattern.ask(actorSystem,
				me -> new EventRouter.StopEventProcessing(device),
				Duration.ofSeconds(30),
				actorSystem.scheduler());
		System.out.println(">> StopEventProcessing message sent");				
	}
}