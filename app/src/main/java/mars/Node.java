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

// FIXME: Find a better way to handle the throws
// FIXME: Debug UPDATE

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

    public enum State {
        JOIN,
        LEAVE,
        STABLE,
        OUT,
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
    private final Map<Integer, VersionedValue> proposedValues = new HashMap<>();    // Used during Leave procedure

    private Set<ActorRef> peerSet;//contains also yourself

    private int item_repartition_responses = 0;
    // to track which neighbour is available during JOIN
    private final boolean[] available_neighbours = new boolean[2*N - 1]; 
    private final boolean[] ignore_nuple = new boolean[N];

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
        public Integer key;
        public VersionedValue latest;
        public int responses = 0;

        // For GET
        public PendingRequest(ActorRef client) {
            this.client = client;
            this.key = null; // no key for GET
            latest = new VersionedValue(null, 0); // no value for GET
        }

        // For UPDATE
        public PendingRequest(ActorRef client, Integer key, String value) {
            this.client = client;
            this.key = key;
            this.latest = new VersionedValue(value, -1);
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
        public final Set<ActorRef> peerSet;//the set of the nodes in the network
        public final State senderState;//useful to understand if the sender is a joining or a leaving node
        public PeersMsg(Set<ActorRef> peerSet, State senderState){
            this.peerSet = peerSet;
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

    public static class LeaveAnnouncement implements Serializable {
        public final Map<Integer, VersionedValue> dataToStore;//in case of leaving, share the data to store

        public LeaveAnnouncement(Map<Integer, VersionedValue> dataToStore) {
            this.dataToStore = dataToStore;
        }
    }

    public static class LeaveAck implements Serializable {}
    
    public static class LeaveOutcomeMsg implements Serializable {
        boolean commit;

        public LeaveOutcomeMsg (boolean commit) {
            this.commit = commit;
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

    public static class LeaveTimeoutMsg implements Serializable {}

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
        this.peerSet = new TreeSet<ActorRef>((a1, a2) -> {
            String name1 = a1.path().name();
            String name2 = a2.path().name();
            return Integer.compare(Integer.parseInt(name1), Integer.parseInt(name2));
        });

        this.peerSet.add(getSelf());//add yourself to list
        
        this.storage = new HashMap<>();
        this.state = State.OUT;
        getContext().become(out());
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

    private static int modulo(int a, int b) {
        return (a % b + b) % b; // to handle negative values correctly
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
            int idx = modulo((startIndex + i), sortedPeers.size());
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

            //only accept keys that the replica is responsible for
            ArrayList<ActorRef> sortedPeers = setToList(this.peerSet);
            if (!isResponsible(key, sortedPeers)) continue;

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
        ArrayList<ActorRef> sortedPeers = setToList(this.peerSet);

        for (Integer key : this.storage.keySet()) {
            //if this node is not among the responsible replicas, mark key for removal
            if (!isResponsible(key, sortedPeers)) {
                this.storage.remove(key);
                logger.log(this.name.toString() + " " + this.state, "Removed key " + key + " (no longer responsible after node " + newNodeName + " joined)");
            }
        }
    }

    private void proceedJoinRecovery() {
        if (state == State.JOIN) {
            for (ActorRef node : this.peerSet) {
                if (node != getSelf()) {
                    delay();
                    node.tell(new PeersMsg(this.peerSet, State.JOIN), getSelf());
                }
            }
        }

        // Context changed in both JOIN and RECOVERY
        this.state = State.STABLE;
        getContext().become(createReceive());
        logger.log(this.name.toString() + " " + state, "Now STABLE in the network with " + this.peerSet.size() + " nodes");
    }

    private void proceedLeave(boolean success) {
        // Tell to everyone the choice and change state
        ArrayList<ActorRef> newPeers = new ArrayList<>(this.peerSet);
        newPeers.remove(getSelf());

        for(ActorRef peer: newPeers) {
            delay();
            peer.tell(new LeaveOutcomeMsg(success), getSelf());
        }

        if(success) {
            this.state = State.OUT;
            getContext().become(out());
        }
        else {
            this.state = State.STABLE;
        }
    }

    private void sendGetToReplicas(UUID requestId, Integer key, List<ActorRef> replicas) {
        for(ActorRef node : replicas){
            logger.log(this.name.toString() + " " + this.state, "Sending GET request to node " + node.path().name());
            delay();
            node.tell(new QuorumRequestMsg(key, null, Quorum.GET, requestId), getSelf());
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
            logger.log(this.name.toString() + " " + this.state, "Sending UPDATE request to node " + node.path().name());
            delay();
            node.tell(new QuorumRequestMsg(key, new VersionedValue(value, version), Quorum.UPDATE, requestId), getSelf());
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

    private int getNeighbourIdx(ActorRef neighbour) {
        ArrayList<ActorRef> sortedPeers = new ArrayList<>(this.peerSet);
        int selfIdx = sortedPeers.indexOf(getSelf());
        int neighIdx = sortedPeers.indexOf(neighbour);
        // Index in sortedPeers of the first neighbour in available_neighbours
        int startIndex = modulo((selfIdx - N + 1), sortedPeers.size());
        int idx = modulo((neighIdx - startIndex), sortedPeers.size());
        
        if(neighIdx == selfIdx || idx > 2*N - 1) {
            idx = -1;
        }
        else {
            if (idx > N - 1) idx--; // N-1 should be the place of self, so shift by 1
        }

        return idx;
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
    // Check that, for each n-uple that may contain a key
    // for which the node is responsible, R replicas are up. 
    // To do so, use the GET method to query the ID of the node 
    // that precedes the first replica of the n-uple.
    private void onJoinMsg(JoinMsg msg){
        logger.log(this.name.toString() + " " + this.state, "Joining the network");
        //msg will contain the bootstrap node to ask for the peer list
        this.state = State.JOIN;
        getContext().become(joinRecover());
        this.storage.clear();

        if(msg.bootstrap != null) {
            ActorRef bootstrap = msg.bootstrap;
            logger.log(this.name.toString() + " " + this.state, "Requesting peers list from " + bootstrap.path().name());
            delay();
            bootstrap.tell(new RequestPeersMsg(), getSelf());
        }
        else {
            this.state = State.STABLE;
            getContext().become(createReceive());
            logger.log(this.name.toString() + " " + state, "Now STABLE in the network with " + this.peerSet.size() + " nodes");
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
        if(this.state != State.STABLE) {
            throw new IllegalStateException("Node " + this.name + " is not in STABLE state, cannot process LEAVE request. Assumptions violated.");
        }
        this.state = State.LEAVE;
        Set<ActorRef> newPeerList = new TreeSet<>(this.peerSet);
        newPeerList.remove(getSelf());
        logger.log(this.name.toString()  + " " + this.state, "Starting leaving procedure");

        //compute which key to send to which node
        Map<ActorRef, Map<Integer, VersionedValue>> nodeToDataMap = new HashMap<>();
        List<ActorRef> sortedPeers = setToList(newPeerList);

        for(int i=0; i<N; i++) {
            ignore_nuple[i] = true;
        }

        for (Map.Entry<Integer, VersionedValue> entry : this.storage.entrySet()) {
            int key = entry.getKey();
            VersionedValue item = entry.getValue();

            int idx = findResponsibleReplicaIndex(key, sortedPeers);
            List<ActorRef> targets = getNextN(sortedPeers, idx);

            for (ActorRef target : targets) {
                nodeToDataMap.computeIfAbsent(target, k -> new HashMap<>()).put(key, item);
            }
            
            // Should not ignore this N-uple as the leaving node contians a key that will be stored
            // there after the leave
            int nuple = getNeighbourIdx(sortedPeers.get(idx));
            if(nuple >= N) {
                throw new IllegalStateException("Node " + sortedPeers.get(idx).path().name() + " cannot be a neighbour first replica after leave.");
            }
            ignore_nuple[nuple] = false;
        }
        
        // To speedup the process if all neighbours are running
        item_repartition_responses = 0;
        // Prepare available_neighbours
        for(int i=0; i<2*N - 1; i++) {
            available_neighbours[i] = false;
        }

        //send peer list + specific data to each node
        for (ActorRef node : newPeerList) {
            Map<Integer, VersionedValue> payload = nodeToDataMap.getOrDefault(node, new HashMap<>());
            logger.log(this.name.toString()  + " " + this.state, "Tell " + node.path().name() + " to update peer list (+ storage)");
            delay();
            node.tell(new LeaveAnnouncement(payload), getSelf());
        }

        // Set Leave Timeout
        getContext().getSystem().scheduler().scheduleOnce(
            Duration.create(T, TimeUnit.MILLISECONDS),
            getSelf(),
            new LeaveTimeoutMsg(),
            getContext().getSystem().dispatcher(),
            ActorRef.noSender()
        );
    }

    /**
     * when requested, the node will enter in a crashed state
     * @param msg
     */
    private void onCrashMsg(CrashMsg msg){
        this.state = State.CRASH;
        getContext().become(crashed());

        if(!proposedVersions.isEmpty()){
            throw new IllegalStateException("Node " + this.name + " is CRASHING but has pending proposed versions. Assumptions violated.");
        }

        logger.log(this.name.toString() + " " + this.state, "Node crashed");
    }

    /**
     * when requested, the node will enter in a recovery mode
     * @param msg
     */
    private void onRecoverMsg(RecoveryMsg msg){
        this.state = State.RECOVERY;
        getContext().become(joinRecover());
        logger.log(this.name.toString() + " " + this.state, "Node is recovering from node " + msg.recoverNode.path().name());
        //ask the recover node for the peer list
        //and then forget/request items based on such list
        delay();
        msg.recoverNode.tell(new RequestPeersMsg(), getSelf());
    }

    // --- SERVICE HANDLERS -------------------------------
    private void onUpdateMsg(UpdateMsg msg){
        if(this.state != State.STABLE) {
            throw new IllegalStateException("Node " + this.name + " is not in STABLE state, cannot process UPDATE request. Assumptions violated.");
        }
        
        //store the pending get request of the client
        PendingRequest pendingGet = new PendingRequest(getSender());
        PendingRequest pendingUpdate = new PendingRequest(getSender(), msg.key, msg.value);
        UUID requestId = UUID.randomUUID();
        pendingGets.put(requestId, pendingGet);
        pendingUpdates.put(requestId, pendingUpdate);

        logger.log(this.name.toString() + " " + this.state, "Received UPDATE request from " + getSender().path().name() + " with ID " + requestId);

        //send quorum request to replica nodes
        List<ActorRef> peers = setToList(this.peerSet);
        int firstReplicaIndex = findResponsibleReplicaIndex(msg.key, peers);
        List<ActorRef> replicas = getNextN(peers, firstReplicaIndex);
        //replicas.add(peers.get(firstReplicaIndex));
        
        //GET before UPDATE
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
        List<ActorRef> peers = setToList(this.peerSet);
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
        delay();
        getSender().tell(new PeersMsg(this.peerSet, State.STABLE), getSelf());
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
            this.peerSet.addAll(msg.peerSet); // update peer list
            logger.log(this.name.toString() + " " + this.state, "Updated peer list after JOIN, now " + this.peerSet.size() + " nodes");
            cleanupStorageAfterNewNodeJoin(getSender());
            return;
        }

        if( this.state != State.JOIN && this.state != State.RECOVERY) {
            throw new IllegalStateException("Node " + this.name + " is not in expected state, cannot process PeersMsg.");
        }

        this.peerSet.clear();
        this.peerSet.addAll(msg.peerSet);
        this.peerSet.add(getSelf());//add yourself to the list
        logger.log(this.name.toString() + " " + this.state, "Updated peer list with " + this.peerSet.size() + " nodes");

        //For the first joins
        if(this.state == State.JOIN && this.peerSet.size() <= N) {
            proceedJoinRecovery();
            return;
        }

        //handle RECOVERY
        if (this.state == State.RECOVERY){
            //remove keys no longer responsible for
            storage.entrySet().removeIf(entry -> !isResponsible(entry.getKey(), setToList(this.peerSet)));
        }
        //handle JOIN
        else {
            for(int i=0; i<N; i++) {
                available_neighbours[i] = false;
            }
        }

        // The remaining part is the same for JOIN and RECOVERY
        // NOTE: While in RECOVERY state requesting the storage is not necessary to guarantee consistency,
        // it is necessary in JOIN state to see if it should abort the JOIN or not.

        //send ItemsRequestMsg to peers you want data from
        ArrayList<ActorRef> sortedPeers = setToList(this.peerSet);
        int selfIndex = sortedPeers.indexOf(getSelf());
        item_repartition_responses = 0;
        for (int i = -N+1; i < N; ++i) {
            if (i == 0) continue; // skip self
            int idx = modulo((selfIndex + i), sortedPeers.size());
            logger.log(this.name + " " + this.state, "Requesting storage to " + sortedPeers.get(idx).path().name());
            delay();
            sortedPeers.get(idx).tell(new ItemsRequestMsg(), getSelf());
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
        delay();
        getSender().tell(new ItemRepartitioningMsg(this.storage), getSelf());
    }

    /**
     * if the node receives all responses, update its storage
     * @param msg contains the storage of the sender
     */
    private void onItemRepartitioningMsg(ItemRepartitioningMsg msg){
        item_repartition_responses++;

        if(this.state == State.JOIN) {
            int idx = getNeighbourIdx(getSender()); 
            if(idx < 0) {
                throw new IllegalStateException("Node " + this.name + " received ItemRepartitioningMsg from " + getSender().path().name() + " but the index is negative.");
            }
            // If 2*N - 1 > this.peerSet.size() - 1, then one node 
            // will appear twice in available_neighbours
            while(idx < 2*N - 1) {   
                available_neighbours[idx] = true;
                idx += this.peerSet.size() - 1;
            }
        }

        mergeStorage(msg.storage);

        //if all responses obtained, procede with computation
        //otherwise wait at most the TIMEOUT
        if (item_repartition_responses == 2*N - 1) {
            proceedJoinRecovery();
        }
    }

    private void onQuorumRequestMsg(QuorumRequestMsg msg){
        // DONE: Fix Read/Write conflicts
        // Idea: Just reject READ
        if(msg.request == Quorum.GET){
            //get request
            //the node responds to the coordinator with the stored value
            if(!proposedVersions.containsKey(msg.key)) {
                logger.log(this.name.toString() + " " + this.state, "Responding to GET request from " + getSender().path().name() + " for request ID " + msg.requestId);
                delay();
                getSender().tell(new QuorumRequestMsg(msg.key, this.storage.get(msg.key), Quorum.GET_ACK, msg.requestId), getSelf());
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
                logger.log(this.name.toString() + " " + this.state, "Responding to UPDATE request from " + getSender().path().name() + " for request ID " + msg.requestId + " with version " + msg.value.version);
                delay();
                getSender().tell(new QuorumRequestMsg(msg.key, msg.value, Quorum.UPDATE_ACK, msg.requestId), getSelf());
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

            if(++pending.responses == R){
                if(!pendingUpdates.containsKey(msg.requestId)) {
                    //quorum reached, respond to client
                    logger.log(this.name.toString() + " " + this.state, "Value " + pending.latest.value + " with version " + pending.latest.version + " is the latest for key " + msg.key);
                    logger.log(this.name.toString() + " " + this.state, "GET request " + msg.requestId + " finalized");
                    pendingGets.remove(msg.requestId);
                    delay();
                    pending.client.tell(new GetResponse(true, pending.latest.value, pending.latest.version), getSelf());
                } else {
                    //if it is for an UPDATE, do UPDATE
                    //send UPDATE to replicas
                    //Note that, by assumption, peers will be the same for GET and consequent UPDATE
                    List<ActorRef> peers = setToList(this.peerSet);
                    int firstReplicaIndex = findResponsibleReplicaIndex(msg.key, peers);
                    List<ActorRef> replicas = getNextN(peers, firstReplicaIndex);
                    pendingGets.remove(msg.requestId);

                    PendingRequest pendingUpdate = pendingUpdates.get(msg.requestId);
                    sendUpdateToReplicas(msg.requestId, pendingUpdate.key, pendingUpdate.latest.value, pending.latest.version + 1, replicas);
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
                List<ActorRef> peers = setToList(this.peerSet);
                int firstReplicaIndex = findResponsibleReplicaIndex(msg.key, peers);
                List<ActorRef> replicas = getNextN(peers, firstReplicaIndex);

                for(ActorRef replica : replicas){
                    logger.log(this.name.toString() + " " + this.state, "Sending UPDATE ACK to node " + replica.path().name() + " for request ID " + msg.requestId);
                    delay();
                    replica.tell(new QuorumRequestMsg(msg.key, new VersionedValue(msg.value.value, msg.value.version), Quorum.UPDATE_CONFIRM, msg.requestId), getSelf());
                }

                //quorum reached, respond to client
                logger.log(this.name.toString() + " " + this.state, "UPDATE request " + msg.requestId + " finalized");
                pendingUpdates.remove(msg.requestId);
                delay();
                pending.client.tell(new UpdateResponse(true), getSelf());
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

            if(proposedVersions.get(msg.key) != null && proposedVersions.get(msg.key).version == msg.value.version) {
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

    private void onLeaveAnnouncement(LeaveAnnouncement msg) {
        // Check that proposedValues is empty
        if(!proposedValues.isEmpty()) {
            throw new IllegalStateException("Concurrent Leave operations. Assumptions violated.");
        }

        //store only if version is greater than the one in local storage
        for (Map.Entry<Integer, VersionedValue> entry : msg.dataToStore.entrySet()) {
            int key = entry.getKey();
            VersionedValue incoming = entry.getValue();
            VersionedValue existing = this.storage.get(key);

            if (existing == null || incoming.version > existing.version) {
                proposedValues.put(key, incoming);
                logger.log(this.name.toString() + " " + this.state, "Proposed key " + key + " from LEAVE of " + getSender().path().name() + " with version " + incoming.version);
            } else {
                logger.log(this.name.toString() + " " + this.state, "Ignored key " + key + " from LEAVE (existing version " + existing.version + " >= " + incoming.version + ")");
            }
        }

        if(!msg.dataToStore.isEmpty()) {
            delay();
            getSender().tell(new LeaveAck(), getSelf());
        }
    }

    private void onLeaveAck(LeaveAck msg) {
        int idx = getNeighbourIdx(getSender());
        if(idx < 0) {
            throw new IllegalStateException("Node " + this.name + " received LeaveAck from " + getSender().path().name() + ".");
        }
        // If 2*N - 1 > this.peerSet.size() - 1, then one node 
        // will appear twice in available_neighbours
        while(idx < 2*N -1) {
                available_neighbours[idx] = true;
                idx += this.peerSet.size() - 1;
        }

        if(++item_repartition_responses == 2*N - 1) {
            //all responses received, proceed with the next steps
            logger.log(this.name.toString() + " " + this.state, "Received all LEAVE ACKs, proceeding with the next steps");
            proceedLeave(true);
        } else {
            logger.log(this.name.toString() + " " + this.state, "Received LEAVE ACK from " + getSender().path().name() + ", waiting for more responses");
        }
    }

    private void onLeaveOutcomeMsg(LeaveOutcomeMsg msg) {
        // Commit changes if Leave done 
        if(msg.commit) {
            for(Integer key: proposedValues.keySet()) {
                this.storage.put(key, proposedValues.get(key));
            }
            this.peerSet.remove(getSender());
            logger.log(this.name.toString() + " " + this.state, "Updated peer list after LEAVE, now " + this.peerSet.size() + " nodes");
        }

        proposedValues.clear();
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
        //Handle timeout for JOIN and RECOVERY
        //if the node is in a stable state, it means it requested
        //an update or get from a quorum
        //otherwise it is in a recovery or join state

        // O(N) solution
        if(item_repartition_responses < 2*N - 1) {
            if(this.state == State.JOIN) {
                int count = 0;
                // First N-uple
                for (int i=0; i<N; i++) {
                    if(available_neighbours[i]) count++;
                }

                // Next neighbours
                for(int i=N; i<2*N-1 && count >= R; i++) {
                    if(available_neighbours[i-N]) count--;
                    if(available_neighbours[i]) count++;
                }

                // Check if quorum is reached for every N-uple
                if(count < R) {
                    logger.log(this.name.toString() + " " + this.state, "Timeout expired waiting for responses, cancelling JOIN procedure.");
                    this.state = State.OUT;
                    getContext().become(out());
                } else {
                    logger.log(this.name.toString() + " " + this.state, "Quorum reached with " + item_repartition_responses + " responses, proceeding with the next steps");
                    proceedJoinRecovery();
                }
            }
            else {
                logger.log(this.name.toString() + " " + this.state, "Timeout expired waiting for responses, proceeding with " + item_repartition_responses + " responses");
                proceedJoinRecovery();
            }
        }
    }

    private void onQuorumTimeoutMsg(QuorumTimeoutMsg msg) {
        if (msg.request == Quorum.GET) {
            //DONE: Case of GET for UPDATE
            PendingRequest pending = pendingGets.get(msg.requestId);
            if (pending != null && pending.responses < R) {
                if(!pendingUpdates.containsKey(msg.requestId)) {
                    logger.log(this.name.toString() + " " + this.state, "GET request " + msg.requestId + " TIMEOUT, no quorum");
                    pendingGets.remove(msg.requestId);//clean up
                    delay();
                    pending.client.tell(new GetResponse(false, null, -1), getSelf());
                }
                else {
                    logger.log(this.name.toString() + " " + this.state, "GET request before UPDATE " + msg.requestId + " TIMEOUT, no quorum");
                    pendingGets.remove(msg.requestId);//clean up
                    pendingUpdates.remove(msg.requestId);//clean up
                    delay();
                    pending.client.tell(new UpdateResponse(false), getSelf());
                }
            }
        } else if (msg.request == Quorum.UPDATE) {
            PendingRequest pending = pendingUpdates.get(msg.requestId);
            if (pending != null && pending.responses < W) {
                // DONE: Send REJECT to the replicas
                List<ActorRef> peers = setToList(this.peerSet);
                int firstReplicaIndex = findResponsibleReplicaIndex(msg.key, peers);
                List<ActorRef> replicas = getNextN(peers, firstReplicaIndex);

                for(ActorRef replica : replicas){
                    logger.log(this.name.toString() + " " + this.state, "Sending UPDATE REJECT to node " + replica.path().name() + " for request ID " + msg.requestId);
                    delay();
                    replica.tell(new QuorumRequestMsg(msg.key, null, Quorum.UPDATE_REJECT, msg.requestId), getSelf());
                }

                //Send reject to the client
                logger.log(this.name.toString() + " " + this.state, "UPDATE request " + msg.requestId + " TIMEOUT, no quorum");
                pendingUpdates.remove(msg.requestId);//clean up
                delay();
                pending.client.tell(new UpdateResponse(false), getSelf());
            }
        }
    }

    private void onLeaveTimeoutMsg(LeaveTimeoutMsg msg) {
        if(item_repartition_responses < 2*N - 1) {
            // Check that for each n-uple t o not ignore W replicas are available
            int count = 0;
            int nuple = 0;

            // First N-uple
            for (int i=0; i<N; i++) {
                if(available_neighbours[i]) count++;
            }

            // Next neighbours
            for(int i=N; i<2*N-1 && (ignore_nuple[nuple] || count >= W); i++) {
                if(available_neighbours[i-N]) count--;
                if(available_neighbours[i]) count++;
                nuple++;
            }

            // Check if quorum is reached for every N-uple
            if(!ignore_nuple[nuple] && count < W) {
                logger.log(this.name.toString() + " " + this.state, "Timeout expired waiting for responses, cancelling JOIN procedure.");
                proceedLeave(false);
            } else {
                logger.log(this.name.toString() + " " + this.state, "Quorum reached with " + item_repartition_responses + " responses, proceeding with the next steps");
                proceedLeave(true);
            }
        }
    }

    //define the mapping between the received message types and actor methods
    @Override
    public Receive createReceive() {
      return receiveBuilder()
        //state handlers
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
        .match(LeaveAnnouncement.class, this::onLeaveAnnouncement)
        .match(LeaveAck.class, this::onLeaveAck)
        .match(LeaveOutcomeMsg.class, this::onLeaveOutcomeMsg)
        
        //others handlers
        .match(LogStorage.class, this::onLogStorageMsg)
        .match(TimeoutMsg.class, this::onTimeoutMsg)
        .match(QuorumTimeoutMsg.class, this::onQuorumTimeoutMsg)
        .match(LeaveTimeoutMsg.class, this::onLeaveTimeoutMsg)

        .matchAny(msg -> {
            //any other message is ignored
        })
        .build();
    }

    final AbstractActor.Receive out(){
        return receiveBuilder()
            .match(JoinMsg.class, this::onJoinMsg)
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

    final AbstractActor.Receive joinRecover(){
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