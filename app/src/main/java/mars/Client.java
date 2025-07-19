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
import java.util.Random;

public class Client extends AbstractActor{
    private static Random rand = new Random();

    private String name;
    private Logger logger;
    private List<ActorRef> nodeList;//so that client can ask randomly any node

    // MESSAGES --------------------------------------------
    public static class UpdateNodeListMsg implements Serializable {
        public final List<ActorRef> nodeList;
        public UpdateNodeListMsg(List<ActorRef> nodeList){
            this.nodeList = Collections.unmodifiableList(new ArrayList<>(nodeList));
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
    public Client(String name, Logger logger, List<ActorRef> nodeList){
        this.name = name;
        this.logger = logger;
        this.nodeList = nodeList;
    }

    static public Props props(String name, Logger logger, List<ActorRef> nodeList) {
        return Props.create(Client.class, () -> new Client(name, logger, nodeList));
    }

    // UTILS -----------------------------------------
    
    // BEHAVIOR -----------------------------------------
    /**
     * update the peer list of the client
     * @param msg contains the list of all the nodes that belong to the network
     */
    public void onUpdateNodeList(UpdateNodeListMsg msg){
        this.nodeList = msg.nodeList;
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
        logger.log(this.name, "Requested GET to " + coordinator.path().name());
    }

    private void onUpdateResponse(UpdateResponse msg){
        //check if response is valid
        if(!msg.isValid){
            logger.log(this.name, "Update request to " + getSender().path().name() + " is NOT valid");
            return;
        }

        logger.log(this.name, "Update request to " + getSender().path().name() + " valid");
    }

    private void onGetResponse(GetResponse msg){
        //check if response is valid
        if(!msg.isValid){
            logger.log(this.name, "Get request to " + getSender().path().name() + " is NOT valid");
            return;
        }

        logger.log(this.name, "Retrieved: " + msg.value + " with version " + msg.version + " from " + getSender().path().name());
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
        .matchAny(msg -> {
            //any other message is ignored
        })
        .build();
    }
}