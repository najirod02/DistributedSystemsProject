package mars;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import mars.Client.GetMsg;
import mars.Client.UpdateMsg;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

//TODO: Swap the order of tell and delay
/**
 * NOTE: a node stores a key only if the key is strictly less than the node itself
 * 
 * e.g.
 * key = 10, node = 10 -> DON'T store
 * key = 9, node = 10 -> store
 */
public class Node extends AbstractActor{
    
    private static final int N = 3,//replication factor
                             R = 2,//reading quorum 
                             W = 2,//writing quorum
                             T = 1000,//in millis
                             MIN_DELAY = 100,//in millis
                             MAX_DELAY = 200;//in millis
                             //note that the max delay obtainable is MAX_DELAY + MIN_DELAY

    private static Random random = new Random();

    private enum State {
        JOIN,
        LEAVE,
        STABLE,
        CRASH,
        RECOVERY,
    }

    private enum Quorum {
        GET,
        UPDATE,
        GET_ACK,
        UPDATE_ACK,
        UPDATE_CONFIRM,//used to confirm the update to the replicas
        UPDATE_REJECT,//used to reject the update to the replicas
    }

    private Integer name;
    private Logger logger;
    private Map<Integer, VersionedValue> storage;
    public State state;//so that clients/nodes avoid to contact a crashed node and wait indefinetly
    private final Map<UUID, PendingRequest> pendingGets = new HashMap<>();
    private final Map<UUID, PendingRequest> pendingUpdates = new HashMap<>();
    // used to track the latest proposed non-commited version of each key
    // key -> version
    //Note that, because of the assumptions, the replica will always Leave/Crash with porposedVersions empty.
    private final Map<Integer, ProposedVersion> proposedVersions = new HashMap<>();

    private Set<ActorRef> peerList;//contains also yourself

    private boolean waitingForResponses = false;//in order to "ignore" the timeout
    private int item_repartition_responses = 0;

    // private class to store both value and version inside the storage
    private class VersionedValue{
        public String value;
        public Integer version;

        public VersionedValue(String value, Integer version){
            this.value = value;
            this.version = version;
        }
    }

    private class PendingRequest {
        public ActorRef client;
        public VersionedValue latest = new VersionedValue(null, 0);
        public int responses = 0;

        public PendingRequest(ActorRef client) {
            this.client = client;
        }
    }
    
    private class ProposedVersion {
        public Integer version;
        public UUID requestId;

        public ProposedVersion(Integer version, UUID requestId) {
            this.version = version;
            this.requestId = requestId;
        }
    }

    // MESSAGES --------------------------------------------

    // --- STATE MESSAGES -------------------------------
    public static class JoinMsg implements Serializable {
        public final ActorRef bootstrap;
        public JoinMsg(ActorRef bootstrap){
            this.bootstrap = bootstrap;
        }
    }

    public static class LeaveMsg implements Serializable {}

    public static class CrashMsg implements Serializable {}

    public static class RecoveryMsg implements Serializable {
        private final ActorRef recoverNode;//the one to ask for the peer list
        public RecoveryMsg(ActorRef recoverNode){
            this.recoverNode = recoverNode;
        }
    }
    
    // --- SERVICE MESSAGES -------------------------------
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

    // --- PROCEDURE MESSAGES -------------------------------
    public static class RequestPeersMsg implements Serializable {}

    public static class PeersMsg implements Serializable {
        public final Set<ActorRef> peerList;//the set of the nodes in the network
        public final Map<Integer, VersionedValue> dataToStore;//in case of leaving, share the data to store
        public final State senderState;//useful to understand if the sender is a joining or a leaving node
        public PeersMsg(Set<ActorRef> peerList, Map<Integer, VersionedValue> dataToStore, State senderState){
            this.peerList = peerList;
            this.dataToStore = dataToStore != null ? dataToStore : new HashMap<>();
            this.senderState = senderState;
        }
    }

    public static class ItemsRequestMsg implements Serializable {}

    public static class ItemRepartitioningMsg implements Serializable {
        public final Map<Integer, VersionedValue> storage;
        public ItemRepartitioningMsg(Map<Integer, VersionedValue> storage){
            this.storage = storage;
        }
    }

    public static class QuorumRequestMsg implements Serializable {
        public final Integer key;
        public final VersionedValue value;
        public final Quorum request;//to distinguish between a get, update request and a confirm response
        public final UUID requestId;
        public QuorumRequestMsg(Integer key, VersionedValue value, Quorum request, UUID requestId){
            this.key = key;
            this.value = value;
            this.request = request;
            this.requestId = requestId;
        }
    }

    // --- OTHER MESSAGES -------------------------------
    public static class LogStorage implements Serializable {}

    public static class TimeoutMsg implements Serializable {}

    public static class QuorumTimeoutMsg implements Serializable {
        public final Integer key;
        public final Quorum request;
        public final UUID requestId;
        public QuorumTimeoutMsg(Integer key, Quorum request, UUID requestId){
            this.key = key;
            this.request = request;
            this.requestId = requestId;
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
    }

    static public Props props(Integer name, Logger logger) {
        return Props.create(Node.class, () -> new Node(name, logger));
    }

    // UTILS --------------------------------------------
    /**
     * a simple function to convert the set into an array list
     * @param set a set containing actor refs
     * @return a list containing all the actor refs of the set
     */
    private ArrayList<ActorRef> setToList(Set<ActorRef> set){
        return new ArrayList<>(set);
    }

    /**
     * given the list of peers, find the responsible replica of a given key
     * @param key
     * @param sortedPeers
     * @return the index of the first responsbile replica
     */
    private int findResponsibleReplicaIndex(int key, List<ActorRef> sortedPeers) {
        for (int i = 0; i < sortedPeers.size(); i++) {
            int nodeId = Integer.parseInt(sortedPeers.get(i).path().name());
            if (nodeId > key) {
                return i;
            }
        }
        //if no node has ID > key, wrap to first
        return 0;
    }

    /**
     * given the list of peers and the first responsible replica, return the list of N responsibles
     * @param sortedPeers
     * @param startIndex
     * @return the list of responsible replicas
     */
    private List<ActorRef> getNextN(List<ActorRef> sortedPeers, int startIndex) {
        List<ActorRef> result = new ArrayList<>();

        if(sortedPeers.size() == 0) return result;

        for (int i = 0; i < N; ++i) {
            int idx = (startIndex + i) % sortedPeers.size();
            result.add(sortedPeers.get(idx));
        }
        return result;
    }

    /**
     * given the key checks if the node is responsible of it
     * @param key 
     * @param sortedPeers
     * @return true if it is responsible, false otherwise
     */
    private boolean isResponsible(int key, List<ActorRef> sortedPeers) {
        int primaryIndex = findResponsibleReplicaIndex(key, sortedPeers);
        List<ActorRef> responsibleNodes = getNextN(sortedPeers, primaryIndex);
        ActorRef self = getSelf();

        return responsibleNodes.contains(self);
    }

    /**
     * when a node request the storage from other nodes, it needs to merge them to its
     * storage checking that the key-value pare stored is the latest version and that
     * the node can actually store it based on the key value
     * @param incomingStorage the storage it needs to be merged
     */
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

    /**
     * after a node joins the network, all "previous" replicas need to check if they need
     * to drop certain key-value pairs based on the name of the new node
     * @param newNode the new node that has joined the network
     */
    private void cleanupStorageAfterNewNodeJoin(ActorRef newNode) {
        int newNodeName = Integer.parseInt(newNode.path().name());
        ArrayList<ActorRef> sortedNodes = new ArrayList<>(this.peerList);

        Set<Integer> keysToRemove = new TreeSet<>();
        
        // TODO: use isResponsible method to check if the node is responsible for the key instead of manually checking?
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
            logger.log(this.name.toString() + " " + this.state, "Removed key " + key + " (no longer responsible after node " + newNodeName + " joined)");
        }
    }

    /**
     * execute a specific action based on the node state after a certain amount of
     * responses are received or the timeout has been triggered
     */
    private void proceedAfterResponses() {
        if (state == State.JOIN) {
            for (ActorRef node : this.peerList) {
                if (node != getSelf()) {
                    node.tell(new PeersMsg(this.peerList, null, State.JOIN), getSelf());
                    delay();
                }
            }
        }

        //the recovery node doesn't need to announce itself
        if(state == State.RECOVERY)
            getContext().become(createReceive());

        state = State.STABLE;
        waitingForResponses = false;//to ignore the timeout
        item_repartition_responses = 0;
        logger.log(this.name.toString() + " " + state, "Now STABLE in the network with " + this.peerList.size() + " nodes");
    }

    private void sendGetToReplicas(UUID requestId, Integer key, List<ActorRef> replicas) {
        for(ActorRef node : replicas){
            node.tell(new QuorumRequestMsg(key, null, Quorum.GET, requestId), getSelf());
            logger.log(this.name.toString() + " " + this.state, "Sending GET request to node " + node.path().name());
            delay();
        }

        //schedule timeout to check if quorum is valid
        getContext().getSystem().scheduler().scheduleOnce(
            Duration.create(T, TimeUnit.MILLISECONDS),
            getSelf(),
            new QuorumTimeoutMsg(key, Quorum.GET, requestId),
            getContext().getSystem().dispatcher(),
            ActorRef.noSender()
        );
    }

    private void sendUpdateToReplicas(UUID requestId, Integer key, String value, Integer version, List<ActorRef> replicas) {
        for(ActorRef node : replicas){
            node.tell(new QuorumRequestMsg(key, new VersionedValue(value, version), Quorum.UPDATE, requestId), getSelf());
            logger.log(this.name.toString() + " " + this.state, "Sending UPDATE request to node " + node.path().name());
            delay();
        }

        //schedule timeout to check if quorum is valid
        getContext().getSystem().scheduler().scheduleOnce(
            Duration.create(T, TimeUnit.MILLISECONDS),
            getSelf(),
            new QuorumTimeoutMsg(key, Quorum.UPDATE, requestId),
            getContext().getSystem().dispatcher(),
            ActorRef.noSender()
        );
    }

    /**
     * during unicast transmissions, add some delay
     * to simulate the network
     */
    private static void delay(){
        try{
            Thread.sleep(random.nextInt(MAX_DELAY) + MIN_DELAY);
        }catch(InterruptedException e){
            System.err.println(e);
        }
    }

    // BEHAVIOR -----------------------------------------

    // --- STATE HANDLERS -------------------------------
    /**
     * message send from the main to tell the node to join the network
     * it requires to obtain the peer list from a given bootstrap
     * obtain the corresponding items from the nodes
     * and finally, update everyone of the new peer list
     * @param msg the message containing the bootstrap node
     */
    private void onJoinMsg(JoinMsg msg){
        logger.log(this.name.toString() + " " + this.state, "Joining the network");
        //msg will contain the bootstrap node to ask for the peer list
        this.state = State.JOIN;
        if(msg.bootstrap != null) {
            ActorRef bootstrap = msg.bootstrap;
            logger.log(this.name.toString() + " " + this.state, "Requesting peers list from " + bootstrap.path().name());
            bootstrap.tell(new RequestPeersMsg(), getSelf());
            delay();
        }
        else {
            this.state = State.STABLE;
            logger.log(this.name.toString() + " " + state, "Now STABLE in the network with " + this.peerList.size() + " nodes");
        }
    }

    /**
     * message send from the main to tell the node to leave the network
     * it requires to announce every other node that it is leaving
     * and passes its data items to any node that becomes responsible
     * for them
     * @param msg
     */
    private void onLeaveMsg(LeaveMsg msg){
        //use the peer list to announce everyone of the departing
        this.state = State.LEAVE;
        Set<ActorRef> newPeerList = new TreeSet<>(this.peerList);
        newPeerList.remove(getSelf());
        logger.log(this.name.toString()  + " " + this.state, "Starting leaving procedure");

        //compute which key to send to which node
        Map<ActorRef, Map<Integer, VersionedValue>> nodeToDataMap = new HashMap<>();
        List<ActorRef> sortedPeers = setToList(newPeerList);

        for (Map.Entry<Integer, VersionedValue> entry : this.storage.entrySet()) {
            int key = entry.getKey();
            VersionedValue item = entry.getValue();

            int idx = findResponsibleReplicaIndex(key, sortedPeers);
            List<ActorRef> targets = getNextN(sortedPeers, idx);

            for (ActorRef target : targets) {
                nodeToDataMap.computeIfAbsent(target, k -> new HashMap<>()).put(key, item);
            }
        }

        //send peer list + specific data to each node
        for (ActorRef node : newPeerList) {
            Map<Integer, VersionedValue> payload = nodeToDataMap.getOrDefault(node, new HashMap<>());
            node.tell(new PeersMsg(newPeerList, payload, State.LEAVE), getSelf());
            delay();
            logger.log(this.name.toString()  + " " + this.state, "Tell " + node.path().name() + " to update peer list (+ storage)");
        }

        //update local peer list too not only of the other nodes
        this.peerList.clear();
        this.storage.clear();
        logger.log(this.name.toString()  + " " + this.state, "Left the network and cleared peer list and storage");
    }

    /**
     * when requested, the node will enter in a crashed state
     * @param msg
     */
    private void onCrashMsg(CrashMsg msg){
        this.state = State.CRASH;
        getContext().become(crashed());
        logger.log(this.name.toString() + " " + this.state, "Node crashed");
    }

    /**
     * when requested, the node will enter in a recovery mode
     * @param msg
     */
    private void onRecoverMsg(RecoveryMsg msg){
        this.state = State.RECOVERY;
        getContext().become(recovered());
        logger.log(this.name.toString() + " " + this.state, "Node is recovering from node " + msg.recoverNode.path().name());
        //ask the recover node for the peer list
        //and then forget/request items based on such list
        msg.recoverNode.tell(new RequestPeersMsg(), getSelf());
        delay();
    }

    // --- SERVICE HANDLERS -------------------------------
    private void onUpdateMsg(UpdateMsg msg){
        if(this.state != State.STABLE) {
            throw new IllegalStateException("Node " + this.name + " is not in STABLE state, cannot process UPDATE request. Assumptions violated.");
        }
        
        //store the pending get request of the client
        PendingRequest pending = new PendingRequest(getSender());
        UUID requestId = UUID.randomUUID();
        pendingGets.put(requestId, pending);
        pendingUpdates.put(requestId, pending);

        logger.log(this.name.toString() + " " + this.state, "Received UPDATE request from " + getSender().path().name() + " with ID " + requestId);

        //send quorum request to replica nodes
        List<ActorRef> peers = setToList(this.peerList);
        int firstReplicaIndex = findResponsibleReplicaIndex(msg.key, peers);
        List<ActorRef> replicas = getNextN(peers, firstReplicaIndex);
        //replicas.add(peers.get(firstReplicaIndex));
        
        // DONE: GET before UPDATE
        sendGetToReplicas(requestId, msg.key, replicas);
    }

    //changing of variables 
    private void onGetMsg(GetMsg msg){
        if(this.state != State.STABLE) {
            throw new IllegalStateException("Node " + this.name + " is not in STABLE state, cannot process GET request. Assumptions violated.");
        }

        //store the pending get request of the client
        PendingRequest pending = new PendingRequest(getSender());
        UUID requestId = UUID.randomUUID();
        pendingGets.put(requestId, pending);

        logger.log(this.name.toString() + " " + this.state, "Received GET request from " + getSender().path().name() + " with ID " + requestId);

        //send quorum request to replica nodes
        List<ActorRef> peers = setToList(this.peerList);
        int firstReplicaIndex = findResponsibleReplicaIndex(msg.key, peers);
        List<ActorRef> replicas = getNextN(peers, firstReplicaIndex);
        //replicas.add(peers.get(firstReplicaIndex));

        //DONE: GET
        sendGetToReplicas(requestId, msg.key, replicas);
    }

    // --- PROCEDURE HANDLERS -------------------------------
    /**
     * the bootstrap will wait for this message in order to return the peer list
     * @param msg contains the latest peer list
     */
    public void onRequestPeersMsg(RequestPeersMsg msg){
        // the bootstrap simply send back its list of peers
        getSender().tell(new PeersMsg(this.peerList, null, State.STABLE), getSelf());
        delay();
    }

    /**
     * a node that receives a message containin a peer list will update its own list
     * in order to know exactly which nodes are present in the network.
     * in the case the node is a joining one, it will update all the other nodes
     * otherwise, will simply update its list
     * @param msg contains latest peers list
     */
    private void onPeersMsg(PeersMsg msg) {
        //handle JOIN announcement from another node
        if (this.state == State.STABLE && msg.senderState == State.JOIN) {
            this.peerList.addAll(msg.peerList); // update peer list
            logger.log(this.name.toString() + " " + this.state, "Updated peer list after JOIN, now " + this.peerList.size() + " nodes");
            cleanupStorageAfterNewNodeJoin(getSender());
            return;
        }

        //handle LEAVE announcement from another node
        if (this.state == State.STABLE && msg.senderState == State.LEAVE) {
            //replace peer list (sender already removed themselves)
            this.peerList.clear();
            this.peerList.addAll(msg.peerList);
            logger.log(this.name.toString() + " " + this.state, "Updated peer list after LEAVE, now " + this.peerList.size() + " nodes");

            //store only if version is greater than the one in local storage
            for (Map.Entry<Integer, VersionedValue> entry : msg.dataToStore.entrySet()) {
                int key = entry.getKey();
                VersionedValue incoming = entry.getValue();
                VersionedValue existing = this.storage.get(key);

                if (existing == null || incoming.version > existing.version) {
                    this.storage.put(key, incoming);
                    logger.log(this.name.toString() + " " + this.state, "Stored key " + key + " from LEAVE of " + getSender().path().name() + " with version " + incoming.version);
                } else {
                    logger.log(this.name.toString() + " " + this.state, "Ignored key " + key + " from LEAVE (existing version " + existing.version + " >= " + incoming.version + ")");
                }
            }
            return;
        }

        //handle RECOVERY
        if (this.state == State.RECOVERY){
            //replace peer list entirely with the recovered one
            this.peerList.clear();
            this.peerList.addAll(msg.peerList);
            logger.log(this.name.toString() + " " + this.state, "Recovered peer list with " + this.peerList.size() + " nodes");

            //remove keys no longer responsible for
            storage.entrySet().removeIf(entry -> !isResponsible(entry.getKey(), setToList(this.peerList)));

            //send ItemsRequestMsg to peers you want data from
            ArrayList<ActorRef> peers = setToList(this.peerList);
            int selfIndex = peers.indexOf(getSelf());
            waitingForResponses = true;
            for (int i = 1; i <= N; ++i) {
                int idx = (selfIndex + i) % peers.size();
                peers.get(idx).tell(new ItemsRequestMsg(), getSelf());
                logger.log(this.name + " " + this.state, "Requesting storage from " + peers.get(idx).path().name());
                delay();
            }

            //schedule timeout
            getContext().getSystem().scheduler().scheduleOnce(
                Duration.create(T, TimeUnit.MILLISECONDS),
                getSelf(),
                new TimeoutMsg(),
                getContext().getSystem().dispatcher(),
                ActorRef.noSender()
            );
            return;
        }
        if(this.state != State.JOIN) {
            throw new IllegalStateException("Node " + this.name + " is not in STABLE nor RECOVERY nor JOIN state, cannot process PeersMsg. Assumptions violated.");
        }
        //otherwise, likely this node just joined and is syncing from others
        this.peerList.addAll(msg.peerList);
        logger.log(this.name.toString() + " " + this.state, "Updated peer list with now " + this.peerList.size() + " nodes");

        //send ItemsRequest to next N peers
        /*
         * e.g.
         * 10 - 20 - 30 - 40
         * 20 needs to replicate data with N = 2
         * 20 takes from 30 and 40
         */
        ArrayList<ActorRef> list = setToList(this.peerList);
        int startingIndex = list.indexOf(getSelf());
        waitingForResponses = true;
        for (int i = 1; i <= N; ++i) {
            int index = (startingIndex + i) % list.size();
            list.get(index).tell(new ItemsRequestMsg(), getSelf());
            logger.log(this.name.toString() + " " + this.state, "Request storage to node " + list.get(index).path().name());
            delay();
        }

        //schedule timeout
        getContext().getSystem().scheduler().scheduleOnce(
            Duration.create(T, TimeUnit.MILLISECONDS),
            getSelf(),
            new TimeoutMsg(),
            getContext().getSystem().dispatcher(),
            ActorRef.noSender()
        );
    }

    /**
     * a node is requested to share all its storage to another node
     * @param msg
     */
    private void onItemsRequestMsg(ItemsRequestMsg msg){
        getSender().tell(new ItemRepartitioningMsg(this.storage), getSelf());
        delay();
    }

    /**
     * if the node receives all responses, update its storage
     * @param msg contains the storage of the sender
     */
    private void onItemRepartitioningMsg(ItemRepartitioningMsg msg){
        item_repartition_responses++;
        mergeStorage(msg.storage);

        //if all responses obtained, procede with computation
        //otherwise wait at most the TIMEOUT
        if (item_repartition_responses == N) {
            proceedAfterResponses();
        }
    }

    private void onQuorumRequestMsg(QuorumRequestMsg msg){
        // DONE: Fix Read/Write conflicts
        // Idea: Just reject READ
        if(msg.request == Quorum.GET){
            //get request
            //the node responds to the coordinator with the stored value
            if(!proposedVersions.containsKey(msg.key)) {
                getSender().tell(new QuorumRequestMsg(msg.key, this.storage.get(msg.key), Quorum.GET_ACK, msg.requestId), getSelf());
                logger.log(this.name.toString() + " " + this.state, "Responding to GET request from " + getSender().path().name() + " for request ID " + msg.requestId);
                delay();
            }
            else {
                //if the key is already proposed, it means that the node is in the middle of an UPDATE
                //so it cannot respond to GET requests
                logger.log(this.name.toString() + " " + this.state, "Ignoring GET request " + msg.requestId + " from " + getSender().path().name() + " (key already proposed)");
            }
        } else if(msg.request == Quorum.UPDATE){
            // System.out.println("Received UPDATE request from " + getSender().path().name() + " for key " + msg.key + " with value " + msg.value.value + " and version " + msg.value.version);
            //update request
            Integer stored_version = 0;
            //check if the item is already stored
            VersionedValue item = storage.get(msg.key);
            if(item != null){
                stored_version = item.version;
            }
            //in both cases increment so that if it is the first time, it will
            //have version 1 otherwise, it will be incremented by 1
            // DONE: Reply with success only if the proposed version is greater than the current stored version
            // DONE: and of the version of the last UPDATE request.  
            
            if(stored_version < msg.value.version && (!proposedVersions.containsKey(msg.key) || 
            proposedVersions.get(msg.key).version < msg.value.version)) {
                //Note that if you jump a version before committing, sequential consistency is not violated.
                ProposedVersion proposedVersion = new ProposedVersion(msg.value.version, msg.requestId);
                proposedVersions.put(msg.key, proposedVersion);
                getSender().tell(new QuorumRequestMsg(msg.key, msg.value, Quorum.UPDATE_ACK, msg.requestId), getSelf());
                logger.log(this.name.toString() + " " + this.state, "Responding to UPDATE request from " + getSender().path().name() + " for request ID " + msg.requestId + " with version " + msg.value.version);
                delay();
            }
            else {
                //if the version is not greater, just ignore the request
                //and send an UPDATE_REJECT to the coordinator
                logger.log(this.name.toString() + " " + this.state, "Ignoring UPDATE request from " + getSender().path().name() + " for request ID " + msg.requestId + " (version not greater)");
            }
            
        } else if(msg.request == Quorum.GET_ACK){
            //the coordinator is getting back and ack for get
            //check if value received is the latest version
            PendingRequest pending = pendingGets.get(msg.requestId);
            
            if(pending == null) return;//in case of errors

            if(msg.value != null && msg.value.version > pending.latest.version){
                pending.latest.version = msg.value.version;
                pending.latest.value = msg.value.value;
            }
            
            logger.log(this.name.toString() + " " + this.state, "Received GET_ACK from " + getSender().path().name() + " for request ID " + msg.requestId);

            //DONE: Case of GET for UPDATE
            if(++pending.responses == R){
                if(!pendingUpdates.containsKey(msg.requestId)) {
                    //quorum reached, respond to client
                    pending.client.tell(new GetResponse(true, pending.latest.value, pending.latest.version), getSelf());
                    logger.log(this.name.toString() + " " + this.state, "GET request " + msg.requestId + " finalized");
                    pendingGets.remove(msg.requestId);
                    delay();
                } else {
                    //if it is for an UPDATE, do UPDATE
                    //send UPDATE to replicas
                    //Note that, by assumption, peers will be the same for GET and consequent UPDATE
                    List<ActorRef> peers = setToList(this.peerList);
                    int firstReplicaIndex = findResponsibleReplicaIndex(msg.key, peers);
                    List<ActorRef> replicas = getNextN(peers, firstReplicaIndex);
                    pendingGets.get(msg.requestId).responses = 0;
                    pendingGets.remove(msg.requestId);
                    sendUpdateToReplicas(msg.requestId, msg.key, pending.latest.value, pending.latest.version + 1, replicas);
                }
            }
        } else if(msg.request == Quorum.UPDATE_ACK){
            //the coordinator is getting back and ack for update
            PendingRequest pending = pendingUpdates.get(msg.requestId);

            if(pending == null) return;//in case of errors
            
            logger.log(this.name.toString() + " " + this.state, "Received UPDATE_ACK from " + getSender().path().name() + " for request ID " + msg.requestId);
            logger.log(this.name.toString() + " " + this.state, "Pending Responses: " + pending.responses);

            if(++pending.responses == W){
                //DONE:quorum reached, send confirmation to nodes         
                List<ActorRef> peers = setToList(this.peerList);
                int firstReplicaIndex = findResponsibleReplicaIndex(msg.key, peers);
                List<ActorRef> replicas = getNextN(peers, firstReplicaIndex);

                for(ActorRef replica : replicas){
                    replica.tell(new QuorumRequestMsg(msg.key, new VersionedValue(msg.value.value, msg.value.version), Quorum.UPDATE_CONFIRM, msg.requestId), getSelf());
                    logger.log(this.name.toString() + " " + this.state, "Sending UPDATE ACK to node " + replica.path().name() + " for request ID " + msg.requestId);
                    delay();
                }

                //quorum reached, respond to client
                pending.client.tell(new UpdateResponse(true), getSelf());
                logger.log(this.name.toString() + " " + this.state, "UPDATE request " + msg.requestId + " finalized");
                pendingUpdates.remove(msg.requestId);
                delay();
            }
        }
        // Note that, assuming that no node can crash during the UPDATE process, each node will eventually
        // receive an UPDATE_CONFIRM or UPDATE_REJECT for each value stored in proposedVersions.
        // If you don't include the Ref to the Coordinator in proposedVersions, it may happen that if another
        // coordinator sends an UPDATE_REJECT for a key that was already proposed by another coordinator.
        else if(msg.request == Quorum.UPDATE_CONFIRM) {
            //the replica gets a confirmation or reject from the coordinator
            // DONE: Write only if the version is greater than the one stored
            // DONE: Because of quorum, the case where the version is equal should not happen
            Integer stored_version = 0;
            VersionedValue item = storage.get(msg.key);
            if(item != null){
                stored_version = item.version;
            }
            //We need this check in the case the COMMIT of the coordinator who proposed the version propagates
            //faster than the COMMIT of the coordinator who proposed the previous version.
            if(stored_version < msg.value.version) {
                this.storage.put(msg.key, new VersionedValue(msg.value.value, ++stored_version)); 
            }
            //Because of FIFO and reliable channels, it cannot happen that proposedVersions.get(msg.key).version < msg.value.version
            //Neither that proposedVersions.get(msg.key) == null
            if(proposedVersions.get(msg.key).version < msg.value.version) {
                throw new IllegalStateException("Proposed version is less than the stored version. This should not happen due to FIFO and reliable channels guarantees.");
            }
            else if(proposedVersions.get(msg.key).version == msg.value.version) {
                proposedVersions.remove(msg.key);
            }
        }
        else if(msg.request == Quorum.UPDATE_REJECT) {
            //the replica gets a reject from the coordinator
            // DONE
            //DONE: Use UUID instead of coordinator
            if(proposedVersions.get(msg.key) != null && proposedVersions.get(msg.key).requestId == msg.requestId) {
            //if the proposed version is the same as the one stored, remove it
                proposedVersions.remove(msg.key);
            }
        }
    }

    // --- OTHER MESSAGES -------------------------------
    /**
     * not a required functionality but becomes useful in debugging
     * to check the storage of a node
     * @param msg
     */
    private void onLogStorageMsg(LogStorage msg){
        StringBuilder sb = new StringBuilder("Current storage:\n");
    
        storage.entrySet().stream()
            .sorted(Map.Entry.comparingByKey()) // optional, makes output deterministic
            .forEach(entry -> {
                int key = entry.getKey();
                VersionedValue v = entry.getValue();
                sb.append(String.format("  Key: %d -> Value: \"%s\" (v%d)\n", key, v.value, v.version));
            });

        if (storage.isEmpty()) {
            sb.append("  [empty]");
        }

        logger.log(this.name.toString() + " " + this.state, sb.toString());
    }

    /**
     * when the timeout is received, execute a specific action based on node state
     * @param msg
     */
    private void onTimeoutMsg(TimeoutMsg msg) {
        //if the node is in a stable state, it means it requested
        //an update or get from a quorum
        //otherwise it is in a recovery or join state
        if(waitingForResponses && this.state != State.STABLE && item_repartition_responses < N) {
            logger.log(this.name.toString() + " " + this.state, "Timeout expired waiting for responses, proceeding with " + item_repartition_responses + " responses");
            proceedAfterResponses();
        }
    }

    private void onQuorumTimeoutMsg(QuorumTimeoutMsg msg) {
        if (msg.request == Quorum.GET) {
            //DONE: Case of GET for UPDATE
            PendingRequest pending = pendingGets.get(msg.requestId);
            if (pending != null && pending.responses < R) {
                if(!pendingUpdates.containsKey(msg.requestId)) {
                    pending.client.tell(new GetResponse(false, null, -1), getSelf());
                    logger.log(this.name.toString() + " " + this.state, "GET request " + msg.requestId + " TIMEOUT, no quorum");
                    pendingGets.remove(msg.requestId);//clean up
                    delay();
                }
                else {
                    pending.client.tell(new UpdateResponse(false), getSelf());
                    logger.log(this.name.toString() + " " + this.state, "GET request before UPDATE " + msg.requestId + " TIMEOUT, no quorum");
                    pendingGets.remove(msg.requestId);//clean up
                    pendingUpdates.remove(msg.requestId);//clean up
                    delay();
                }
            }
        } else if (msg.request == Quorum.UPDATE) {
            PendingRequest pending = pendingUpdates.get(msg.requestId);
            if (pending != null && pending.responses < W) {
                // DONE: Send REJECT to the replicas
                List<ActorRef> peers = setToList(this.peerList);
                int firstReplicaIndex = findResponsibleReplicaIndex(msg.key, peers);
                List<ActorRef> replicas = getNextN(peers, firstReplicaIndex);

                for(ActorRef replica : replicas){
                    replica.tell(new QuorumRequestMsg(msg.key, null, Quorum.UPDATE_REJECT, msg.requestId), getSelf());
                    logger.log(this.name.toString() + " " + this.state, "Sending UPDATE REJECT to node " + replica.path().name() + " for request ID " + msg.requestId);
                    delay();
                }

                //Send reject to the client
                pending.client.tell(new UpdateResponse(false), getSelf());
                logger.log(this.name.toString() + " " + this.state, "UPDATE request " + msg.requestId + " TIMEOUT, no quorum");
                pendingUpdates.remove(msg.requestId);//clean up
                delay();
            }
        }
    }

    //define the mapping between the received message types and actor methods
    @Override
    public Receive createReceive() {
      return receiveBuilder()
        //state handlers
        .match(JoinMsg.class, this::onJoinMsg)
        .match(LeaveMsg.class, this::onLeaveMsg)
        .match(CrashMsg.class, this::onCrashMsg)

        //service handlers
        .match(GetMsg.class, this::onGetMsg)
        .match(UpdateMsg.class, this::onUpdateMsg)

        //procedure handlers
        .match(RequestPeersMsg.class, this::onRequestPeersMsg)
        .match(PeersMsg.class, this::onPeersMsg)
        .match(ItemsRequestMsg.class, this::onItemsRequestMsg)
        .match(ItemRepartitioningMsg.class, this::onItemRepartitioningMsg)
        .match(QuorumRequestMsg.class, this::onQuorumRequestMsg)
        
        //others handlers
        .match(LogStorage.class, this::onLogStorageMsg)
        .match(TimeoutMsg.class, this::onTimeoutMsg)
        .match(QuorumTimeoutMsg.class, this::onQuorumTimeoutMsg)

        .matchAny(msg -> {
            //any other message is ignored
        })
        .build();
    }

    final AbstractActor.Receive crashed(){
        return receiveBuilder()
            .match(RecoveryMsg.class, this::onRecoverMsg)
            .matchAny(msg -> {
                //any other message is ignored
            })
            .build();
    }

    final AbstractActor.Receive recovered(){
        return receiveBuilder()
            .match(PeersMsg.class, this::onPeersMsg)
            .match(ItemRepartitioningMsg.class, this::onItemRepartitioningMsg)
            .match(TimeoutMsg.class, this::onTimeoutMsg)
            .matchAny(msg -> {
                //any other message is ignored
            })
            .build();
    }

}