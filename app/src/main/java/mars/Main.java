package mars;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import mars.Node.JoinMsg;

import mars.Client.UpdateMsg;
import mars.Client.GetMsg;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Main {
    
    public static void delay(){
        try{
            Thread.sleep(rand.nextInt(300) + 200);
        }catch(InterruptedException e){
            System.err.println(e);
        }
    }

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

        // create some nodes and make them join the network through the correct message
        //this will be used as the bootstrap for every new node
        System.out.println("Creating Nodes");
        ActorRef bootstrap = system.actorOf(Node.props(40, logger), "40");
        nodeList.add(bootstrap);
        
        // create some clients and ask them to make some requests
        // note that even if nodeList is not ordered like a ring, it doesn't
        // matter as clients can ask anyone
        ActorRef client_1 = system.actorOf(Client.props("C1", logger, nodeList), "C1");
        ActorRef client_2 = system.actorOf(Client.props("C2", logger, nodeList), "C2");

        bootstrap.tell(new JoinMsg(bootstrap), null);
        delay();

        client_1.tell(new UpdateMsg(10, "IRON"), null);
        delay();

        client_1.tell(new GetMsg(10), null);
        delay();

        nodeList.add(system.actorOf(Node.props(20, logger), "20"));
        nodeList.get(1).tell(new JoinMsg(bootstrap), null);
        delay();

        client_2.tell(new UpdateMsg(10, "COPPER"), null);
        delay();

        client_2.tell(new GetMsg(10), null);
        delay();

        nodeList.add(system.actorOf(Node.props(30, logger), "30"));
        nodeList.get(2).tell(new JoinMsg(bootstrap), null);
        delay();

        nodeList.add(system.actorOf(Node.props(10, logger), "10"));
        nodeList.get(3).tell(new JoinMsg(bootstrap), null);
        delay();

        logger.closeStream();
        system.terminate();
    }
}