package mars;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import mars.Client.GetMsg;
import mars.Client.UpdateMsg;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.sound.midi.SysexMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

public class Node extends AbstractActor{
    
    enum State {
        JOIN,
        LEAVE,
        STABLE,
        CRASH,
        RECOVERY,
    }

    private Integer name;
    private Logger logger;
    private Map<Integer, VersionedValue> storage;
    public State state;//so that clients/nodes avoid to contact a crashed node and wait indefinetly
    private boolean isCoordinator;
    private boolean isQuorumReached;//as timeout msg is scheduled, check flag to determine 
                                //if quorum has been reached before the timeout
    private Set<ActorRef> peerList;//contains also yourself
    private int N = 2,//replication factor
                R = 3,//reading quorum 
                W = 3,//writing quorum
                T = 1000,//in millis
                n_responses = 0;

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

    public static class RequestPeersMsg implements Serializable {}

    public static class PeersMsg implements Serializable {
        public final Set<ActorRef> peerList;
        public PeersMsg(Set<ActorRef> peerList){
            this.peerList = peerList;
        }
    }

    public static class JoinMsg implements Serializable {
        public final ActorRef bootstrap;
        public JoinMsg(ActorRef bootstrap){
            this.bootstrap = bootstrap;
        }
    }

    public static class ItemsRequestMsg implements Serializable {}

    public static class ItemRepartitioningMsg implements Serializable {
        public final Map<Integer, VersionedValue> storage;
        public ItemRepartitioningMsg(Map<Integer, VersionedValue> storage){
            this.storage = storage;
        }
    }

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
        //create a tree set that doesn't allow for duplicates (that is no same node)
        //and makes sure that the list is ordered so that a "ring" topology is maintained
        /*
         * e.g
         * 10 - 30 - 45
         * joining 35
         * 10 - 30 - 35 - 45
         * joining 45
         * 10 - 30 - 35 - 45
         */
        this.peerList = new TreeSet<ActorRef>((a1, a2) -> {
            String name1 = a1.path().name();
            String name2 = a2.path().name();
            return Integer.compare(Integer.parseInt(name1), Integer.parseInt(name2));
        });

        this.peerList.add(getSelf());//add yourself to list
        
        this.storage = new HashMap<>();
        this.state = State.JOIN;
        this.isCoordinator = false;
        this.isQuorumReached = false;
    }

    static public Props props(Integer name, Logger logger) {
        return Props.create(Node.class, () -> new Node(name, logger));
    }

    // UTILS --------------------------------------------
    private ArrayList<ActorRef> setToList(Set<ActorRef> set){
        return new ArrayList<>(set);
    }

    public void mergeStorage(Map<Integer, VersionedValue> incomingStorage) {
        for (Map.Entry<Integer, VersionedValue> entry : incomingStorage.entrySet()) {
            Integer key = entry.getKey();
            VersionedValue incomingValue = entry.getValue();

            //only accept keys less than this node's name
            if (key >= this.name) continue;

            VersionedValue currentValue = this.storage.get(key);
            
            if (currentValue == null || incomingValue.version > currentValue.version) {
                this.storage.put(key, new VersionedValue(incomingValue.value, incomingValue.version));
            }
        }
    }

    private void cleanupStorageAfterNewNodeJoin(ActorRef newNode) {
        int newNodeName = Integer.parseInt(newNode.path().name());
        ArrayList<ActorRef> sortedNodes = new ArrayList<>(this.peerList);

        Set<Integer> keysToRemove = new TreeSet<>();

        for (Integer key : this.storage.keySet()) {
            //determine the first node responsible for this key
            //find the first node with name >= key (wrap if necessary)
            int startIndex = 0;
            for (int i = 0; i < sortedNodes.size(); i++) {
                int nodeName = Integer.parseInt(sortedNodes.get(i).path().name());
                if (nodeName >= key) {
                    startIndex = i;
                    break;
                }
            }

            //collect the N responsible nodes for the key
            List<ActorRef> responsibleNodes = new ArrayList<>();
            for (int i = 0; i < N; i++) {
                responsibleNodes.add(sortedNodes.get((startIndex + i) % sortedNodes.size()));
            }

            //if this node is not among the responsible replicas, mark key for removal
            if (!responsibleNodes.contains(getSelf())) {
                keysToRemove.add(key);
            }
        }

        //remove the keys this node should no longer hold
        for (Integer key : keysToRemove) {
            this.storage.remove(key);
            logger.log(this.name.toString(), "Removed key " + key + " (no longer responsible after node " + newNodeName + " joined)");
        }
    }


    // BEHAVIOR -----------------------------------------
    /**
     * message send from the main to tell the node to join the network
     * it requires to obtain the peer list from a given bootstrap
     * obtain the corresponding items from the nodes
     * and finally, update everyone of the new peer list
     * @param msg the message containing the bootstrap node
     */
    private void onJoinMsg(JoinMsg msg){
        //msg will contain the bootstrap node to ask for the peer list
        this.state = State.JOIN;
        ActorRef bootstrap = msg.bootstrap;
        logger.log(this.name.toString(), "Requesting peers list from " + bootstrap.path().name());
        bootstrap.tell(new RequestPeersMsg(), getSelf());
    }

    /**
     * the bootstrap will wait for this message in order to return the peer list
     * @param msg contains the latest peer list
     */
    public void onRequestPeersMsg(RequestPeersMsg msg){
        // the bootstrap simply send back its list of peers
        getSender().tell(new PeersMsg(this.peerList), getSelf());
    }

    /**
     * a node that receives a message containin a peer list will update its own list
     * in order to know exactly which nodes are present in the network.
     * in the case the node is a joining one, it will update all the other nodes
     * otherwise, will simply update its list
     * @param msg contains latest peers list
     */
    private void onPeersMsg(PeersMsg msg){
        //update own list with the one from the bootstrap / other node
        this.peerList.addAll(msg.peerList);
        logger.log(this.name.toString(), "Updated peer list with now " + this.peerList.size() + " nodes");

        //the joining node needs to announce itself to the network
        //if it is a stable node, it doesn't need to update other nodes
        if(this.state == State.STABLE){
            cleanupStorageAfterNewNodeJoin(getSender());
            return;  
        } 

        //the joining node request the data to its neighbor nodes
        /*
         * e.g.
         * 10 - 20 - 30 - 40
         * 20 needs to replicate data with N = 2
         * 20 takes from 30 and 40
         */
        ArrayList<ActorRef> list = setToList(this.peerList);
        int startingIndex = list.indexOf(getSelf());
        for (int i = 1; i <= N; ++i) {
            int index = (startingIndex + i) % list.size();
            list.get(index).tell(new ItemsRequestMsg(), getSelf());
            logger.log(this.name.toString(), "Request storage to node " + list.get(index).path().name());
        }
    }

    /**
     * a node is requested to share all its storage to another node
     * @param msg
     */
    private void onItemsRequestMsg(ItemsRequestMsg msg){
        getSender().tell(new ItemRepartitioningMsg(this.storage), getSelf());
    }

    /**
     * if the node receives all responses, update its storage
     * @param msg contains the storage of the sender
     */
    private void onItemRepartitioningMsg(ItemRepartitioningMsg msg){
        n_responses++;

        //update if required the local storage
        mergeStorage(msg.storage);

        if(n_responses == N){
            logger.log(this.name.toString(), "Obtained all stores");
            //annouce to all nodes the new joining node
            //send new list to all other nodes except yourself
            for(ActorRef node : this.peerList){
                if(node != getSelf()){
                    node.tell(new PeersMsg(this.peerList), getSelf());
                }
            }

            this.state = State.STABLE;//now the node is stable in the network
            n_responses = 0;
            logger.log(this.name.toString(), "Now STABLE in the network with " + this.peerList.size() + " nodes");
        } 
    }

    //TODO: finish the quorum logic
    //logic about that keys must be less than the node name unless we need to wrap around
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
        this.state = State.CRASH;
    }

    //define the mapping between the received message types and actor methods
    @Override
    public Receive createReceive() {
      return receiveBuilder()
        .match(JoinMsg.class, this::onJoinMsg)
        .match(RequestPeersMsg.class, this::onRequestPeersMsg)
        .match(PeersMsg.class, this::onPeersMsg)
        .match(ItemsRequestMsg.class, this::onItemsRequestMsg)
        .match(ItemRepartitioningMsg.class, this::onItemRepartitioningMsg)
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