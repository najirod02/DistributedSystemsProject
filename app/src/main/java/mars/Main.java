package mars;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import mars.Node.JoinMsg;
import mars.Node.LeaveMsg;
import mars.Node.CrashMsg;
import mars.Node.RecoveryMsg;

import mars.Node.LogStorage;
import mars.Node.PeersMsg;
import mars.Client.UpdateMsg;
import mars.Client.UpdateNodeListMsg;
import mars.Client.GetMsg;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;


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

    //user.dir will make sure that all logs are generated inside the project folder starting from app
    private static final String LOGGER_FILE_BASE_PATH = System.getProperty("user.dir") + "/logs";
    private static final Random rand = new Random();
    private static final ActorSystem system = ActorSystem.create("marsakka");
    private static final Logger logger = new Logger(LOGGER_FILE_BASE_PATH + "/test.txt");
    private static final List<ActorRef> nodeList = new ArrayList<ActorRef>();
    private static final ActorRef bootstrap = system.actorOf(Node.props(40, logger), "40");
    private static final ActorRef client_1 = system.actorOf(Client.props("C1", logger, nodeList), "C1");
    private static final ActorRef client_2 = system.actorOf(Client.props("C2", logger, nodeList), "C2");

    public static void main(String[] args) {
        //FIXME: Close all Akka if assumption is violated
        //FIXME: try to make better tests
        //at the moment it is hard to understand what is going on
        //maybe leave uncommented the tests you want
        //even better, wait for user input before starting new test

        //NOTE: The bootstrap node is always up and running

        System.out.println("Creating 40 - bootstrap");
        bootstrap.tell(new Node.JoinMsg(null), null);
        nodeList.add(bootstrap);
        delay(1000);

        System.out.println("Join 20 - 30 - 10");
        nodeList.add(system.actorOf(Node.props(20, logger), "20"));
        nodeList.get(1).tell(new JoinMsg(bootstrap), null);
        delay(1000);

        nodeList.add(system.actorOf(Node.props(30, logger), "30"));
        nodeList.get(2).tell(new JoinMsg(bootstrap), null);
        delay(1000);

        nodeList.add(system.actorOf(Node.props(10, logger), "10"));
        nodeList.get(3).tell(new JoinMsg(bootstrap), null);
        delay(2000);
        
        // Interactive choice for tests
        boolean runTests = true;
        Scanner scanner = new Scanner(System.in);
        while(runTests) {
            System.out.println("Choose a test to run:");
            System.out.println("1. Basic Concurrent Update Test");
            System.out.println("2. Sequential Consistency Test");
            System.out.println("3. Crash & Recovery Test");
            System.out.println("4. Node Leave Test");
            System.out.println("5. Quorum Failure Test");
            System.out.println("6. Quorum Timeout Test");
            System.out.println("7. Additional Edge Tests");
            System.out.println("8. Exit");
            System.out.print("Enter your choice (1-8): ");
            String choice_s;
            int choice = -1;
            try {
                choice_s = scanner.nextLine();
                System.out.println("You chose: " + choice_s);
                choice = Integer.parseInt(choice_s);
            } catch (NumberFormatException e) {
                System.out.println("Invalid input. Please enter a number between 1 and 8.");
                continue;
            }
            switch (choice) {
                case 1:
                    concurrentUpdateTest();
                    break;
                case 2:
                    sequentialConsistencyTest();
                    break;
                case 3:
                    crashRecoveryTest();
                    break;
                case 4:
                    nodeLeaveTest();
                    break;
                case 5:
                    quorumFailureTest();
                    break;
                case 6:
                    quorumTimeoutTest();
                    break;
                case 7:
                    additionalTests();
                    break;
                case 8:
                    System.out.println("Exiting...");
                    runTests = false;
                    break;
                default:
                    System.out.println("Invalid choice. Please enter a number between 1 and 8.");
            }
        }
        scanner.close();
        logger.log("MAIN", "TERMINATE");
        logger.closeStream();
        system.terminate();
    }

    private static void concurrentUpdateTest() {
        // BASIC CONCURRENT UPDATE TEST
        logger.log("MAIN", "BASIC CONCURRENT UPDATE TEST");
        System.out.println("START BASIC CONCURRENT UPDATE TEST");
        client_1.tell(new UpdateMsg(42, "GOLD"), null);
        delay(2000);
        client_2.tell(new UpdateMsg(42, "SILVER"), null);
        delay(10000);
    }

    private static void sequentialConsistencyTest() {
        // SEQUENTIAL CONSISTENCY TEST
        logger.log("MAIN", "SEQUENTIAL CONSISTENCY TEST");
        System.out.println("START SEQUENTIAL CONSISTENCY TEST");

        client_1.tell(new UpdateMsg(42, "GOLD"), null);
        delay();
        client_2.tell(new UpdateMsg(42, "SILVER"), null);
        delay(10000);
        
        client_1.tell(new GetMsg(42), null);
        delay();
        client_2.tell(new GetMsg(42), null);
        delay(10000);
        
        client_1.tell(new UpdateMsg(42, "PLATINUM"), null);
        delay();
        client_2.tell(new GetMsg(42), null);
        delay(10000);
        
        client_1.tell(new GetMsg(42), null);
        delay();
        System.out.println("END SEQUENTIAL CONSISTENCY TEST");
        delay(10000);
    }

    private static void crashRecoveryTest() {
        // CRASH & RECOVERY
        logger.log("MAIN", "CRASH & RECOVERY");
        System.out.println("Crashing node 20");
        nodeList.get(1).tell(new CrashMsg(), null);
        delay(2000);
        
        System.out.println("Client 1 attempts update on key 15 (should fail)");
        client_1.tell(new UpdateMsg(15, "SILVER"), null);
        delay(2000);

        System.out.println("Recovering node 20");
        nodeList.get(1).tell(new RecoveryMsg(bootstrap), null);
        delay(2000);

        System.out.println("Client 1 attempts update on key 15 (should succeed)");
        client_1.tell(new UpdateMsg(15, "PLATINUM"), null);
        delay();
        System.out.println("Client 2 attempts get on key 15 (should succeed)");
        client_2.tell(new GetMsg(15), null);
        delay(10000);
    }

    private static void nodeLeaveTest() {
        // NODE LEAVE
        List<ActorRef> forClient = new ArrayList<>(nodeList);
        forClient.remove(nodeList.get(1)); // remove the node that is leaving

        logger.log("MAIN", "NODE LEAVE");
        System.out.println("Node 20 leaving the network");
        client_1.tell(new UpdateNodeListMsg(forClient), null);
        
        nodeList.get(1).tell(new LeaveMsg(), null);
        delay(2000);

        System.out.println("Client 1 attempts update on key 9 after node 20 leaves (should work)");
        client_1.tell(new UpdateMsg(9, "BRONZE"), null);
        delay(10000);
    }

    private static void quorumFailureTest() {
        // TODO
        // QUORUM FAILURE TEST
        logger.log("MAIN", "QUORUM FAILURE TEST");
        System.out.println("=== START QUORUM FAILURE TEST ===");
        System.out.println("Crashing node 20 and 30");
        nodeList.get(1).tell(new CrashMsg(), null);
        delay();
        nodeList.get(2).tell(new CrashMsg(), null);
        delay();
        System.out.println("Client 1 attempts update with no quorum (should fail)");
        client_1.tell(new UpdateMsg(88, "TUNGSTEN"), null);
        delay();
        System.out.println("Client 2 attempts get with no quorum (should fail)");
        client_2.tell(new GetMsg(88), null);
        delay();
        System.out.println("Recovering node 20 and 30");
        nodeList.get(1).tell(new RecoveryMsg(bootstrap), null);
        delay();
        nodeList.get(2).tell(new RecoveryMsg(bootstrap), null);
        delay();
        System.out.println("Client 1 retries update after recovery (should succeed)");
        client_1.tell(new UpdateMsg(88, "URANIUM"), null);
        delay();
        System.out.println("Client 2 retries get after recovery (should see URANIUM)");
        client_2.tell(new GetMsg(88), null);
        delay();
        System.out.println("=== END QUORUM FAILURE TEST ===");
        delay(10000);
    }

    private static void quorumTimeoutTest() {
        // TODO
        // === QUORUM TIMEOUT TEST ===
        logger.log("MAIN", "QUORUM TIMEOUT TEST");
        System.out.println("=== START QUORUM TIMEOUT TEST ===");

        System.out.println("Crashing node 20 and 30");
        nodeList.get(1).tell(new CrashMsg(), null);
        nodeList.get(2).tell(new CrashMsg(), null);
        delay(500);

        System.out.println("Client 1 attempts update with no quorum (simulate timeout)");
        client_1.tell(new UpdateMsg(500, "RHODIUM"), null);

        // Wait longer to simulate timeout (client waits but quorum can't form)
        //delay(2000);

        System.out.println("Client 2 attempts get after expected timeout");
        client_2.tell(new GetMsg(500), null);
        delay();

        System.out.println("=== END QUORUM TIMEOUT TEST ===");
        delay(1000);

        System.out.println("Print network storage after tests");
        for (ActorRef node : nodeList) {
            node.tell(new LogStorage(), null);
        }
        delay(10000);
    }

    private static void additionalTests() {
        // TODO: Separate and do better
        // ADDITIONAL EDGE TESTS
        logger.log("MAIN", "ADDITIONAL EDGE TESTS");
        System.out.println("Concurrent writes to same key");
        client_1.tell(new UpdateMsg(55, "ZINC"), null);
        client_2.tell(new UpdateMsg(55, "COPPER"), null);
        delay();
        System.out.println("Reading value after concurrent writes");
        client_1.tell(new GetMsg(55), null);
        delay();

        System.out.println("Update and multiple gets for stability");
        client_1.tell(new UpdateMsg(77, "IRON"), null);
        delay();
        client_2.tell(new GetMsg(77), null);
        delay();
        client_2.tell(new GetMsg(77), null);
        delay();
        client_2.tell(new GetMsg(77), null);
        delay();

        System.out.println("GET on unknown key (should fail gracefully or return null)");
        client_1.tell(new GetMsg(9999), null);
        delay();

        System.out.println("Crash one node, then update");
        nodeList.get(1).tell(new CrashMsg(), null);
        delay();
        client_1.tell(new UpdateMsg(123, "ALUMINUM"), null);
        delay();
        System.out.println("Crash another node, now quorum lost");
        nodeList.get(2).tell(new CrashMsg(), null);
        delay();
        client_2.tell(new GetMsg(123), null);
        delay();

        System.out.println("Write, then have a responsible replica leave");
        client_1.tell(new UpdateMsg(200, "LEAD"), null);
        delay();
        System.out.println("Node 10 leaves");
        nodeList.get(3).tell(new LeaveMsg(), null);
        delay();
        client_2.tell(new GetMsg(200), null);
        delay();

        System.out.println("Simultaneous leave and recovery");
        nodeList.get(1).tell(new LeaveMsg(), null);
        delay();
        nodeList.get(2).tell(new RecoveryMsg(bootstrap), null);
        delay();
        client_1.tell(new UpdateMsg(300, "NICKEL"), null);
        delay();
        client_2.tell(new GetMsg(300), null);
        delay(10000);
    }
}