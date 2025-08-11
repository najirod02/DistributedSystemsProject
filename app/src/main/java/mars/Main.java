package mars;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import mars.Node.JoinMsg;
import mars.Node.LeaveMsg;
import mars.Node.CrashMsg;
import mars.Node.RecoveryMsg;

import mars.Node.LogStorage;
import mars.Client.UpdateMsg;
import mars.Client.UpdateNodeListMsg;
import mars.Client.GetMsg;

import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import java.io.File;

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
    private static final Map<Integer, ActorRef> nodeMap = new HashMap<>();
    private static final List<ActorRef> clientList = new LinkedList<>();
    private static final Set<ActorRef> forClients = new HashSet<>();    // Nodes list to give to the Clients
    private static ActorRef bootstrap;

    public static void main(String[] args) {
        //FIXME: Close all Akka if assumption is violated
        //TODO: Other tests
        //at the moment it is hard to understand what is going on
        //maybe leave uncommented the tests you want
        //even better, wait for user input before starting new test

        //NOTE: The bootstrap node is always up and running

        System.out.println("Creating 40 - bootstrap");
        bootstrap = system.actorOf(Node.props(40, logger), "40");
        bootstrap.tell(new Node.JoinMsg(null), null);
        nodeMap.put(40, bootstrap);
        delay(1000);

        System.out.println("Join 20 - 30 - 10");
        nodeMap.put(20, system.actorOf(Node.props(20, logger), "20"));
        nodeMap.get(20).tell(new JoinMsg(bootstrap), null);
        delay(2000);

        
        nodeMap.put(30, system.actorOf(Node.props(30, logger), "30"));
        nodeMap.get(30).tell(new JoinMsg(bootstrap), null);
        delay(2000);

        nodeMap.put(10, system.actorOf(Node.props(10, logger), "10"));
        nodeMap.get(10).tell(new JoinMsg(bootstrap), null);
        delay(2000);
        

        for(Integer key: nodeMap.keySet()) {
            forClients.add(nodeMap.get(key));
        }

        clientList.add(system.actorOf(Client.props("C1", logger, forClients), "C1"));
        clientList.add(system.actorOf(Client.props("C2", logger, forClients), "C2"));

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
            System.out.println("6. Additional Edge Tests");
            System.out.println("7. Interactive Test");
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
                    additionalTests();
                    break;
                case 7:
                    interactiveTest();
                    // Exit after Interactive Test
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
        system.terminate();
        logger.closeStream();
    }

    // TESTS //
    private static void concurrentUpdateTest() {
        // BASIC CONCURRENT UPDATE TEST
        logger.log("MAIN", "BASIC CONCURRENT UPDATE TEST");
        System.out.println("== START BASIC CONCURRENT UPDATE TEST ==");
        
        clientList.get(0).tell(new UpdateMsg(42, "GOLD"), null);
        delay(2000);
        clientList.get(1).tell(new UpdateMsg(42, "SILVER"), null);
        
        System.out.println("Waiting for operation to complete...");
        delay(10000);
        System.out.println("== END BASIC CONCURRENT UPDATE TEST ==");
    }

    private static void sequentialConsistencyTest() {
        // SEQUENTIAL CONSISTENCY TEST
        logger.log("MAIN", "SEQUENTIAL CONSISTENCY TEST");
        System.out.println("== START SEQUENTIAL CONSISTENCY TEST ==");

        clientList.get(0).tell(new UpdateMsg(42, "GOLD"), null);
        delay();
        clientList.get(1).tell(new UpdateMsg(42, "SILVER"), null);
        delay(10000);
        
        clientList.get(0).tell(new GetMsg(42), null);
        delay();
        clientList.get(1).tell(new GetMsg(42), null);
        delay(10000);
        
        clientList.get(0).tell(new UpdateMsg(42, "PLATINUM"), null);
        delay();
        clientList.get(1).tell(new GetMsg(42), null);
        delay(10000);
        
        clientList.get(0).tell(new GetMsg(42), null);
        delay();
        
        System.out.println("Waiting for operation to complete...");
        delay(10000);
        System.out.println("== END SEQUENTIAL CONSISTENCY TEST ==");
    }

    private static void crashRecoveryTest() {
        // CRASH & RECOVERY
        logger.log("MAIN", "CRASH & RECOVERY");
        
        System.out.println("== START CRASH & RECOVERY TEST ==");
        System.out.println("Crashing nodes 20 and 30");

        forClients.remove(nodeMap.get(20)); // remove the node that is crashing
        forClients.remove(nodeMap.get(30));
        clientList.get(0).tell(new UpdateNodeListMsg(forClients), null);
        clientList.get(1).tell(new UpdateNodeListMsg(forClients), null);
        
        nodeMap.get(20).tell(new CrashMsg(), null);
        delay(2000);
        nodeMap.get(30).tell(new CrashMsg(), null);
        delay(2000);
        
        System.out.println("Client 1 attempts update on key 15 (should fail)");
        clientList.get(0).tell(new UpdateMsg(15, "SILVER"), null);
        delay(2000);
        
        System.out.println("Recovering nodes 20 and 30");
        nodeMap.get(20).tell(new RecoveryMsg(bootstrap), null);
        delay(2000);
        nodeMap.get(30).tell(new RecoveryMsg(bootstrap), null);
        delay(2000);

        forClients.add(nodeMap.get(20)); // add the node that is recovering
        clientList.get(0).tell(new UpdateNodeListMsg(forClients), null);
        clientList.get(1).tell(new UpdateNodeListMsg(forClients), null);

        System.out.println("Client 1 attempts update on key 15 (should succeed)");
        clientList.get(0).tell(new UpdateMsg(15, "PLATINUM"), null);
        delay(2000);
        System.out.println("Client 2 attempts get on key 15 (should succeed)");
        clientList.get(1).tell(new GetMsg(15), null);
        
        System.out.println("Waiting for operation to complete...");
        delay(10000);
        
        System.out.println("== END CRASH & RECOVERY TEST ==");
    }

    private static void nodeLeaveTest() {
        // NODE LEAVE
        logger.log("MAIN", "NODE LEAVE");
        System.out.println("== START NODE LEAVE TEST ==");
        System.out.println("Node 20 leaving the network");
        
        forClients.remove(nodeMap.get(20)); // remove the node that is leaving
        updateClients();
        
        nodeMap.get(20).tell(new LeaveMsg(), null);
        delay(2000);

        System.out.println("Client 1 attempts update on key 9 after node 20 leaves (should work)");
        clientList.get(0).tell(new UpdateMsg(9, "BRONZE"), null);
        delay(2000);

        // Node re-join
        System.out.println("Node 20 joining the network");
        nodeMap.get(20).tell(new JoinMsg(bootstrap), null);
        delay(2000);
        
        forClients.add(nodeMap.get(20));
        updateClients();

        System.out.println("Client 1 attempts get on key 9 after node 20 re-joins (should work)");
        clientList.get(0).tell(new GetMsg(9), null);

        System.out.println("Waiting for operation to complete...");
        delay(10000);
        System.out.println("== END NODE LEAVE TEST ==");
    }

    private static void quorumFailureTest() {
        // QUORUM FAILURE TEST
        logger.log("MAIN", "QUORUM FAILURE TEST");
        System.out.println("=== START QUORUM FAILURE TEST ===");
        
        forClients.remove(nodeMap.get(20));
        forClients.remove(nodeMap.get(30));
        updateClients();
        
        System.out.println("Crashing node 20 and 30");
        nodeMap.get(20).tell(new CrashMsg(), null);
        delay(2000);
        nodeMap.get(30).tell(new CrashMsg(), null);
        delay(2000);
        
        System.out.println("Client 1 attempts update with no quorum (should fail)");
        clientList.get(0).tell(new UpdateMsg(88, "TUNGSTEN"), null);
        delay(10000);
        System.out.println("Client 2 attempts get with no quorum (should fail)");
        clientList.get(1).tell(new GetMsg(88), null);
        delay(10000);
        
        System.out.println("Recovering node 20 and 30");
        nodeMap.get(20).tell(new RecoveryMsg(bootstrap), null);
        delay(2000);
        nodeMap.get(30).tell(new RecoveryMsg(bootstrap), null);
        delay(2000);

        forClients.add(nodeMap.get(20));
        forClients.add(nodeMap.get(30));
        updateClients();

        System.out.println("Client 1 retries update after recovery (should succeed)");
        clientList.get(0).tell(new UpdateMsg(88, "URANIUM"), null);
        delay(2000);
        System.out.println("Client 2 retries get after recovery (should see URANIUM)");
        clientList.get(1).tell(new GetMsg(88), null);
        delay();
        System.out.println("=== END QUORUM FAILURE TEST ===");
        delay(10000);
    }

    private static void additionalTests() {
        // TODO: Separate and make better
        // ADDITIONAL EDGE TESTS
        /*
        logger.log("MAIN", "ADDITIONAL EDGE TESTS");
        System.out.println("Concurrent writes to same key");
        clientList.get(0).tell(new UpdateMsg(55, "ZINC"), null);
        clientList.get(1).tell(new UpdateMsg(55, "COPPER"), null);
        delay();
        System.out.println("Reading value after concurrent writes");
        clientList.get(0).tell(new GetMsg(55), null);
        delay();

        System.out.println("Update and multiple gets for stability");
        clientList.get(0).tell(new UpdateMsg(77, "IRON"), null);
        delay();
        clientList.get(1).tell(new GetMsg(77), null);
        delay();
        clientList.get(1).tell(new GetMsg(77), null);
        delay();
        clientList.get(1).tell(new GetMsg(77), null);
        delay();

        System.out.println("GET on unknown key (should fail gracefully or return null)");
        clientList.get(0).tell(new GetMsg(9999), null);
        delay();

        System.out.println("Crash one node, then update");
        nodeMap.get(20).tell(new CrashMsg(), null);
        delay();
        clientList.get(0).tell(new UpdateMsg(123, "ALUMINUM"), null);
        delay();
        System.out.println("Crash another node, now quorum lost");
        nodeMap.get(30).tell(new CrashMsg(), null);
        delay();
        clientList.get(1).tell(new GetMsg(123), null);
        delay();

        System.out.println("Write, then have a responsible replica leave");
        clientList.get(0).tell(new UpdateMsg(200, "LEAD"), null);
        delay();
        System.out.println("Node 10 leaves");
        nodeMap.get(10).tell(new LeaveMsg(), null);
        delay();
        clientList.get(1).tell(new GetMsg(200), null);
        delay();

        System.out.println("Simultaneous leave and recovery");
        nodeMap.get(20).tell(new LeaveMsg(), null);
        delay();
        nodeMap.get(30).tell(new RecoveryMsg(bootstrap), null);
        delay();
        clientList.get(0).tell(new UpdateMsg(300, "NICKEL"), null);
        delay();
        clientList.get(1).tell(new GetMsg(300), null);
        delay(10000);
        */
    }

    private static void interactiveTest() {
        // The user decides the operation
        boolean runTests = true;
        Scanner scanner = new Scanner(System.in);

        System.out.println("== START INTERACTIVE TEST ==");

        while(runTests) {
            printNetwork();
            System.out.println("Choose an operation:");
            System.out.println("1. Update");
            System.out.println("2. Get");
            System.out.println("3. Join");
            System.out.println("4. Leave");
            System.out.println("5. Crash");
            System.out.println("6. Recover");
            System.out.println("7. Exit");
            System.out.print("Enter your choice (1-7): ");
            String choice_s;
            int choice = -1;
            try {
                choice_s = scanner.nextLine();
                System.out.println("You chose: " + choice_s);
                choice = Integer.parseInt(choice_s);
            } catch (NumberFormatException e) {
                System.out.println("Invalid input. Please enter a number between 1 and 7.");
                continue;
            }
            switch (choice) {
                case 1:
                    System.out.println();
                    get();
                    System.out.println();
                    break;
                case 2:
                    System.out.println();
                    update();
                    System.out.println();
                    break;
                case 3:
                    System.out.println();
                    join();
                    System.out.println();
                    break;
                case 4:
                    System.out.println();
                    leave();
                    System.out.println();
                    break;
                case 5:
                    System.out.println();
                    crash();
                    System.out.println();
                    break;
                case 6:
                    System.out.println();
                    recover();
                    System.out.println();
                    break;
                case 7:
                    System.out.println("Exiting...");
                    runTests = false;
                    break;
                default:
                    System.out.println("Invalid choice. Please enter a number between 1 and 8.");
            }
        }
        scanner.close();   
        System.out.println("== END INTERACTIVE TEST ==");
    }

    // OPERATIONS //
    private static void get() {
        // For simplicity, use only C1
        System.out.print("Choose the key to query: ");
        Scanner scanner = new Scanner(System.in);
        int choice;
        try {
            choice = Integer.parseInt(scanner.nextLine());
            System.out.println("You chose: " + choice);
        } catch (NumberFormatException e) {
            System.out.println("Invalid input. Please enter a non negative number.");
            scanner.close();
            return;
        }
        scanner.close();

        // Assume non negative choice
        if(choice < 0) {
            System.out.println("Invalid input. Please enter a non negative number.");
            return;
        }
        
        clientList.get(0).tell(new GetMsg(choice), null);
        System.out.println("Waiting for operation to complete...");
        delay(10000);
        System.out.println("Get completed. Check the logs.");
    }

    private static void update() {
        // For simplicity, use only C1
        System.out.print("Choose the key to update: ");
        Scanner scanner = new Scanner(System.in);
        int key;
        String value;
        try {
            key = Integer.parseInt(scanner.nextLine());
            System.out.println("You chose: " + key);
        } catch (NumberFormatException e) {
            System.out.println("Invalid input. Please enter a non negative number.");
            scanner.close();
            return;
        }

        // Assume non negative choice
        if(key < 0) {
            System.out.println("Invalid input. Please enter a non negative number.");
            scanner.close();
            return;
        }

        try {
            value = scanner.nextLine();
            System.out.println("You chose: " + value);
        } catch (NumberFormatException e) {
            System.out.println("Invalid input.");
            scanner.close();
            return;
        }
        scanner.close();
        
        clientList.get(0).tell(new UpdateMsg(key, value), null);
        System.out.println("Waiting for operation to complete...");
        delay(10000);
        System.out.println("Update completed. Check the logs.");
    }

    private static void join() {
        System.out.print("Choose the ID of the node to add to the Network: ");
        Scanner scanner = new Scanner(System.in);
        int choice;
        try {
            choice = Integer.parseInt(scanner.nextLine());
            System.out.println("You chose: " + choice);
        } catch (NumberFormatException e) {
            System.out.println("Invalid input. Please enter a non negative number.");
            scanner.close();
            return;
        }
        scanner.close();

        // Assume non negative choice
        if(choice < 0) {
            System.out.println("Invalid input. Please enter a non negative number.");
            return;
        }

        // Check if not already in Network
        if (nodeMap.containsKey(choice)) {
            System.out.println("Node already in the Network.");
            return;
        }

        ActorRef newPeer = system.actorOf(Node.props(choice, logger), ""+choice);
        newPeer.tell(new JoinMsg(bootstrap), null);
        System.out.println("Waiting for operation to complete...");
        delay(5000);

        // Check success by seeking file in logs
        File f = new File(LOGGER_FILE_BASE_PATH + "/" + choice);
        if(f.exists() && !f.isDirectory()) {
            System.out.println("Join operation succeded!");
            nodeMap.put(choice, newPeer);
            forClients.add(newPeer);
            updateClients();
        }
        else {
            System.out.println("Join operation failed.");
        }
    }

    private static void leave() {
        //For simplicity, bootstrap (40) cannot leave
        System.out.print("Put the ID of the node that should leave the Network: ");
        Scanner scanner = new Scanner(System.in);
        int choice;
        try {
            choice = Integer.parseInt(scanner.nextLine());
            System.out.println("You chose: " + choice);
        } catch (NumberFormatException e) {
            System.out.println("Invalid input. Please enter a non negative number.");
            scanner.close();
            return;
        }
        scanner.close();

        // Assume non negative choice
        if(choice < 0) {
            System.out.println("Invalid input. Please enter a non negative number.");
            return;
        }

        // Check of Bootstrap
        if (choice == 40) {
            System.out.println("Node 40 is Bootstrap. Bootstrap cannot Leave the Network.");
            return;
        }

        // Check if not already in Network
        if (!nodeMap.containsKey(choice)) {
            System.out.println("Node not in the Network.");
            return;
        }

        nodeMap.get(choice).tell(new LeaveMsg(), null);
        System.out.println("Waiting for operation to complete...");
        delay(5000);

        // Check success by seeking file in logs
        File f = new File(LOGGER_FILE_BASE_PATH + "/" + choice);
        if(!f.exists() || f.isDirectory()) {
            System.out.println("Leave operation succeded!");
            forClients.remove(nodeMap.get(choice));
            updateClients();
            nodeMap.remove(choice);
        }
        else {
            System.out.println("Join operation failed.");
        }
    }

    private static void crash() {
        //For simplicity, bootstrap (40) cannot crash
        System.out.print("Put the ID of the node that should crash: ");
        Scanner scanner = new Scanner(System.in);
        int choice;
        try {
            choice = Integer.parseInt(scanner.nextLine());
            System.out.println("You chose: " + choice);
        } catch (NumberFormatException e) {
            System.out.println("Invalid input. Please enter a non negative number.");
            scanner.close();
            return;
        }
        scanner.close();

        // Assume non negative choice
        if(choice < 0) {
            System.out.println("Invalid input. Please enter a non negative number.");
            return;
        }

        // Check of Bootstrap
        if (choice == 40) {
            System.out.println("Node 40 is Bootstrap. Bootstrap cannot Crash.");
            return;
        }

        // Check if not already in Network
        if (!nodeMap.containsKey(choice)) {
            System.out.println("Node not in the Network.");
            return;
        }

        forClients.remove(nodeMap.get(choice));
        updateClients();
        nodeMap.get(choice).tell(new CrashMsg(), null);
        System.out.println("Waiting for operation to complete...");
        delay(5000);
    }

    private static void recover() {
        System.out.print("Put the ID of the node that should recover: ");
        Scanner scanner = new Scanner(System.in);
        int choice;
        try {
            choice = Integer.parseInt(scanner.nextLine());
            System.out.println("You chose: " + choice);
        } catch (NumberFormatException e) {
            System.out.println("Invalid input. Please enter a non negative number.");
            scanner.close();
            return;
        }
        scanner.close();

        // Assume non negative choice
        if(choice < 0) {
            System.out.println("Invalid input. Please enter a non negative number.");
            return;
        }

        // Check if not already in Network
        if (!nodeMap.containsKey(choice)) {
            System.out.println("Node not in the Network.");
            return;
        }

        nodeMap.get(choice).tell(new RecoveryMsg(bootstrap), null);
        forClients.add(nodeMap.get(choice));
        updateClients();

        System.out.println("Waiting for operation to complete...");
        delay(5000);
    }

    private static void addClient() {
        //TODO
    }

    private static void dropClient() {
        //TODO
    }


    // UTILS //
    private static void printNetwork() {
        //TODO
        System.out.println("Print network storage after tests");
        for (Integer key : nodeMap.keySet()) {
            nodeMap.get(key).tell(new LogStorage(), null);
        }
    }

    private static void updateClients() {
        for(ActorRef c: clientList) {
            c.tell(new UpdateNodeListMsg(forClients), null);
        }
    }
}