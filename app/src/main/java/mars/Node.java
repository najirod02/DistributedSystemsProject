package mars;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import mars.Client.GetMsg;
import mars.Client.UpdateMsg;

import java.io.Serializable;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Node extends AbstractActor{
    
    enum State {
        JOIN,
        LEAVE,
        CRASH,
        RECOVERY,
    }

    private Integer name;
    private Logger logger;
    private Map<Integer, VersionedValue> storage;
    public State state;//so that clients/nodes avoid to contact a crashed node and wait indefinetly
    private boolean isCoordinator;
    private boolean isQuorumMet;//as timeout msg is scheduled, check flag to determine 
                                //if quorum has been reached before the timeout
    private List<ActorRef> peerList;
    private int N = 4,//replication factor
                R = 3,//reading quorum 
                W = 3,//writing quorum
                T = 1000;//in millis

    // private class to store both value and version inside the storage
    private class VersionedValue{
        public String value;
        public Integer version;

        public VersionedValue(String value, Integer version){
            this.value = value;
            this.version = version;
        }
    }

    // MESSAGES --------------------------------------------
    public static class TimeoutMsg implements Serializable {}

    public static class UpdateResponse implements Serializable {
        public final boolean isValid;
        public UpdateResponse(boolean isValid){
            this.isValid = isValid;
        }
    }

    public static class GetResponse implements Serializable{
        public final boolean isValid;
        public final String value;
        public final Integer version;
        public GetResponse(boolean isValid, String value, Integer version){
            this.isValid = isValid;
            this.value = value;
            this.version = version;
        }
    }

    // CONSTRUCTOR -----------------------------------------
    public Node(Integer name, Logger logger){
        this.name = name;
        this.logger = logger;
        this.peerList = new ArrayList<>();
        
        this.storage = new HashMap<>();
        this.state = State.JOIN;
        this.isCoordinator = false;
        this.isQuorumMet = false;

    }

    static public Props props(Integer name, Logger logger) {
        return Props.create(Node.class, () -> new Node(name, logger));
    }

    // UTILS -----------------------------------------
    /**
     * function called after class constructor
     * it allows to execute the join procedure in order
     */
    private void onJoinMsg(){
        //here it will update its peerList
    }

    // BEHAVIOR -----------------------------------------
    //TODO: finish the quorum logic
    private void onUpdateMsg(UpdateMsg msg){
        Integer version = 0;
        //check if the item is already stored
        VersionedValue item = storage.get(msg.key);
        if(item != null){
            version = item.version;
        }
        //in both cases increment so that if it is the first time, it will
        //have version 1 otherwise, it will be incremented by 1
        storage.put(msg.key, new VersionedValue(msg.value, ++version));

        //respond to client
        getSender().tell(new UpdateResponse(true), getSelf());
        logger.log(this.name.toString(), "Finished update request for " + getSender().path().name());
    }

    private void onGetMsg(GetMsg msg){
        VersionedValue item = storage.get(msg.key);

        //respond to client
        if(item == null){
            //doesn't exist or no quorum
            getSender().tell(new GetResponse(false, null, -1), getSelf());
            logger.log(this.name.toString(), "Finished get request for " + getSender().path().name() + " with ERROR");
        } else {
            getSender().tell(new GetResponse(true, item.value, item.version), getSelf());
            logger.log(this.name.toString(), "Finished get request for " + getSender().path().name());
        }
    }

    /**
     * when requested, the node will enter in a crashed state
     * where specific methods are available
     */
    private void crash(){
        getContext().become(crashed());
    }

    //define the mapping between the received message types and actor methods
    @Override
    public Receive createReceive() {
      return receiveBuilder()
        .match(UpdateMsg.class, this::onUpdateMsg)
        .match(GetMsg.class, this::onGetMsg)
        .matchAny(msg -> {
            //any other message is ignored
        })
        .build();
    }

    final AbstractActor.Receive crashed(){
        return receiveBuilder()
            .matchAny(msg -> {})
            .build();
    }

}