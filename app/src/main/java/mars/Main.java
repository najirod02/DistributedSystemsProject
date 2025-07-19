package mars;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import mars.Client.UpdateMsg;

public class Main {
    
    //user.dir will make sure that all logs are generated inside the projet folder starting from app
    private static final String LOGGER_FILE_BASE_PATH = System.getProperty("user.dir") + "/logs";

    public static void main(String[] args) {
        // testing of logger, not required for main execution
        // TestLogger testLogger = new TestLogger(LOGGER_FILE_BASE_PATH + "/test.txt");
        // testLogger.runTest();

        // testing client class
        // create the actor system and send a message
        Logger logger = new Logger(LOGGER_FILE_BASE_PATH + "/test.txt");
        final ActorSystem system = ActorSystem.create("marsakka");
        
        ActorRef client = system.actorOf(Client.props("C1", logger), "C1");
        client.tell(new UpdateMsg(2, "COPPER"), null);
        client.tell(new UpdateMsg(1, "IRON"), null);
        client.tell(new UpdateMsg(101001, "DIAMOND"), null);


        System.out.println("Running. Press Ctrl+C to terminate.");
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            System.out.println("Interrupted. Shutting down.");
        }
        system.terminate();
    }
}