package mars;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import mars.Client.UpdateMsg;
import mars.Client.GetMsg;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

public class Main {
    
    //user.dir will make sure that all logs are generated inside the projet folder starting from app
    private static final String LOGGER_FILE_BASE_PATH = System.getProperty("user.dir") + "/logs";
    private static final int N_NODES = 2;
    private static final Random rand = new Random();

    public static void main(String[] args) {
        // testing of logger, not required for main execution
        // TestLogger testLogger = new TestLogger(LOGGER_FILE_BASE_PATH + "/test.txt");
        // testLogger.runTest();

        // testing the components
        final ActorSystem system = ActorSystem.create("marsakka");
        
        Logger logger = new Logger(LOGGER_FILE_BASE_PATH + "/test.txt");
        List<ActorRef> nodeList = new ArrayList<ActorRef>();

        // create some nodes and store them in the actor_ref list
        //TODO: not the correct way to update the peer list of all nodes
        //the correct procedure would be to make a join request to the network and after that
        //make another request to update all lists of all nodes
        for(int i=0; i<N_NODES; ++i){
            Integer name = rand.nextInt(80 - 5) + 5;
            ActorRef node = system.actorOf(Node.props(name, logger), name.toString());
            nodeList.add(node);
        }

        //order the list so that we already have a "ring" structure
        //this list should be available to all nodes of the network
        nodeList.sort(Comparator.comparingInt(ref -> {
            String name = ref.path().name();//e.g., "10"
            return Integer.parseInt(name);//parse "10"
        }));

        // create some clients and ask them to make some requests
        ActorRef client_1 = system.actorOf(Client.props("C1", logger, nodeList), "C1");
        ActorRef client_2 = system.actorOf(Client.props("C2", logger, nodeList), "C2");
        client_1.tell(new UpdateMsg(10, "IRON"), null);
        client_1.tell(new GetMsg(10), null);
        client_2.tell(new UpdateMsg(10, "COPPER"), null);
        client_2.tell(new GetMsg(10), null);

        // System.out.println("\nRunning. Press Ctrl+C to terminate.");
        // try {
        //     Thread.sleep(Long.MAX_VALUE);
        // } catch (InterruptedException e) {
        //     System.out.println("Interrupted. Shutting down.");
        // }
        system.terminate();
    }
}