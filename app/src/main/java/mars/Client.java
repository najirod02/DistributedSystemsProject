package mars;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import mars.Node.GetResponse;
import mars.Node.UpdateResponse;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;

public class Client extends AbstractActor{
    private static final long TIMEOUT = 10000;//in millis, should be large enough to 
                                             //allow coordinator to respond even with maximum delay
    private static Random rand = new Random();

    private String name;
    private Logger logger;
    private List<ActorRef> nodeList;//so that client can ask randomly any node
    private boolean waitingForResponse = false;//in case i contact a crashed node

    // MESSAGES --------------------------------------------
    public static class TimeoutMsg implements Serializable {}

    public static class UpdateNodeListMsg implements Serializable {
        public final Set<ActorRef> nodeSet;
        public UpdateNodeListMsg(Set<ActorRef> nodeSet){
            this.nodeSet = Collections.unmodifiableSet(nodeSet);
        }
    }

    public static class UpdateMsg implements Serializable {
        public final Integer key;
        public final String value;
        public UpdateMsg(Integer key, String value){
            this.key = key;
            this.value = value;
        }
    }

    public static class GetMsg implements Serializable{
        public final Integer key;
        public GetMsg(Integer key){
            this.key = key;
        }
    }

    // CONSTRUCTOR -----------------------------------------
    public Client(String name, Logger logger, Set<ActorRef> nodeSet){
        this.name = name;
        this.logger = logger;
        this.nodeList = new ArrayList<>(nodeSet);
    }

    static public Props props(String name, Logger logger, Set<ActorRef> nodeSet) {
        return Props.create(Client.class, () -> new Client(name, logger, nodeSet));
    }

    // UTILS -----------------------------------------
    private void scheduleTimeout() {
        getContext().getSystem().scheduler().scheduleOnce(
            Duration.create(TIMEOUT, TimeUnit.MILLISECONDS),
            getSelf(),
            new TimeoutMsg(),
            getContext().getSystem().dispatcher(),
            getSelf()
        );
    }

    // BEHAVIOR -----------------------------------------
    /**
     * update the peer list of the client
     * this messages is sent directly from the main as it is the one
     * that knows exactly who is in the network
     * @param msg contains the list of all the nodes that belong to the network
     */
    public void onUpdateNodeList(UpdateNodeListMsg msg){
        this.nodeList = new ArrayList<>(msg.nodeSet);
        logger.log(this.name, "Updated node list");
    }

    /**
     * make the client make an update request
     * @param msg contains both key and value to be stored/written on the storage 
     */
    public void onUpdateMsg(UpdateMsg msg){
        //ask randomly a node (coordinator) to store/update a value in the storage
        ActorRef coordinator = this.nodeList.get(rand.nextInt(this.nodeList.size()));
        coordinator.tell(new UpdateMsg(msg.key, msg.value), getSelf());
        waitingForResponse = true;
        scheduleTimeout();
        logger.log(this.name, "Requested UPDATE to " + coordinator.path().name());
    }

    /**
     * make the client make a get request
     * @param msg contains the key of the value to be retrieved
     */
    public void onGetMsg(GetMsg msg){
        //ask randomly a node (coordinator) to retrieve a value in the storage
        ActorRef coordinator = this.nodeList.get(rand.nextInt(this.nodeList.size()));
        coordinator.tell(new GetMsg(msg.key), getSelf());
        waitingForResponse = true;
        scheduleTimeout();
        logger.log(this.name, "Requested GET to " + coordinator.path().name());
    }

    /**
     * handler for update response given by the coordinator
     * @param msg contains a boolean to know if the update has been successful
     */
    private void onUpdateResponse(UpdateResponse msg){
        //check if response is valid and not outside time window
        if(waitingForResponse){
            waitingForResponse = false;
            if(!msg.isValid){
                logger.log(this.name, "UPDATE request to " + getSender().path().name() + " is NOT valid");
                return;
            }
            logger.log(this.name, "UPDATE request to " + getSender().path().name() + " valid");
        }
    }

    /**
     * handler for get response given by the coordinator
     * @param msg contains a boolean to know if the get has been successful
     */
    private void onGetResponse(GetResponse msg){
        //check if response is valid and not outside time window
        if(waitingForResponse){
            waitingForResponse = false;
            if(!msg.isValid){
                logger.log(this.name, "GET request to " + getSender().path().name() + " is NOT valid");
                return;
            }
            if(msg.value != null)
                logger.log(this.name, "Retrieved: " + msg.value + " with version " + msg.version + " from " + getSender().path().name());
            else
                logger.log(this.name, "Retrieved nothing from " + getSender().path().name());
        }
    }

    /**
     * timeout handler in the case a response from a coordinator is taking too much time
     * (meaning that the node crashed long before)
     * @param msg
     */
    private void onTimeoutMsg(TimeoutMsg msg){
        if(waitingForResponse){ 
            waitingForResponse = false;
            logger.log(this.name, "Timeout expired waiting for response, giving up...");
        }
    }

    //define the mapping between the received message types and actor methods
    @Override
    public Receive createReceive() {
      return receiveBuilder()
        .match(UpdateMsg.class, this::onUpdateMsg)
        .match(GetMsg.class, this::onGetMsg)
        .match(UpdateNodeListMsg.class, this::onUpdateNodeList)
        .match(UpdateResponse.class, this::onUpdateResponse)
        .match(GetResponse.class, this::onGetResponse)
        .match(TimeoutMsg.class, this::onTimeoutMsg)
        .matchAny(msg -> {
            //any other message is ignored
        })
        .build();
    }
}