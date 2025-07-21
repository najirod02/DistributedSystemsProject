package mars;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import mars.Node.JoinMsg;
import mars.Node.LeaveMsg;
import mars.Node.CrashMsg;
import mars.Node.RecoveryMsg;

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

    public static void delay(int millis){
        try{
            Thread.sleep(millis);
        }catch(InterruptedException e){
            System.err.println(e);
        }
    }

    //user.dir will make sure that all logs are generated inside the projet folder starting from app
    private static final String LOGGER_FILE_BASE_PATH = System.getProperty("user.dir") + "/logs";
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
        System.out.println("Creating 40 - bootstrap");
        ActorRef bootstrap = system.actorOf(Node.props(40, logger), "40");
        nodeList.add(bootstrap);
        
        // create some clients and ask them to make some requests
        // note that even if nodeList is not ordered like a ring, it doesn't
        // matter as clients can ask anyone
        System.out.println("Creating clients");
        ActorRef client_1 = system.actorOf(Client.props("C1", logger, nodeList), "C1");
        ActorRef client_2 = system.actorOf(Client.props("C2", logger, nodeList), "C2");

        //crash the bootsrap to check timeout client
        System.out.println("Crash 40 and test timeout client");
        bootstrap.tell(new CrashMsg(), null);
        delay();
        client_2.tell(new GetMsg(10), null);//should abort
        delay(1500);
        bootstrap.tell(new RecoveryMsg(bootstrap), null);
        delay();

        System.out.println("Join 40 and store key 9 - 10");
        bootstrap.tell(new JoinMsg(bootstrap), null);
        delay();

        client_1.tell(new UpdateMsg(10, "IRON"), null);
        delay();

        client_1.tell(new GetMsg(10), null);
        delay();

        client_1.tell(new UpdateMsg(10, "IRON++"), null);
        delay();

        client_1.tell(new UpdateMsg(9, "GOLD"), null);
        delay();

        System.out.println("Join 20 - 30 - 10");
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

        System.out.println("Leave 20");
        nodeList.get(1).tell(new LeaveMsg(), null);
        delay();

        System.out.println("Crash and recover 30");
        nodeList.get(2).tell(new CrashMsg(), null);
        delay();
        nodeList.get(2).tell(new RecoveryMsg(nodeList.get(3)), null);
        delay();

        //make sure that all the last writes are done before closing the stream
        //to be sure, put a delay before closing the stream
        logger.closeStream();
        system.terminate();
    }
}