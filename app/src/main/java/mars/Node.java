package mars;

import akka.Version;
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
                             T = 1000;//in millis
                    
    enum State {
        JOIN,
        LEAVE,
        STABLE,
        CRASH,
        RECOVERY,
    }

    enum Quorum {
        GET,
        UPDATE,
        GET_ACK,
        UPDATE_ACK
    }

    private Integer name;
    private Logger logger;
    private Map<Integer, VersionedValue> storage;
    public State state;//so that clients/nodes avoid to contact a crashed node and wait indefinetly
    private boolean isQuorumReached;//as timeout msg is scheduled, check flag to determine 
                                //if quorum has been reached before the timeout
    private ActorRef client;//the client that makes the request
    private VersionedValue latestValue;
    private final Map<UUID, PendingRequest> pendingGets = new HashMap<>();
    private final Map<UUID, PendingRequest> pendingUpdates = new HashMap<>();

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
        public VersionedValue latest = new VersionedValue(null, -1);
        public int responses = 0;

        public PendingRequest(ActorRef client) {
            this.client = client;
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
        public final Quorum request;
        public final UUID requestId;
        public QuorumTimeoutMsg(Quorum request, UUID requestId){
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
        this.isQuorumReached = false;
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
        //msg will contain the bootstrap node to ask for the peer list
        this.state = State.JOIN;
        ActorRef bootstrap = msg.bootstrap;
        logger.log(this.name.toString() + " " + this.state, "Requesting peers list from " + bootstrap.path().name());
        bootstrap.tell(new RequestPeersMsg(), getSelf());
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
    }

    // --- SERVICE HANDLERS -------------------------------

    private void onUpdateMsg(UpdateMsg msg){
        logger.log(this.name.toString() + " " + this.state, "Received UPDATE request from " + getSender().path().name());
        
        //store the pending get request of the client
        PendingRequest pending = new PendingRequest(getSender());
        UUID requestId = UUID.randomUUID();
        pendingUpdates.put(requestId, pending);

        //send quorum request to replica nodes
        List<ActorRef> peers = setToList(this.peerList);
        int firstReplicaIndex = findResponsibleReplicaIndex(msg.key, peers);
        List<ActorRef> replicas = getNextN(peers, firstReplicaIndex);
        //replicas.add(peers.get(firstReplicaIndex));

        for(ActorRef node : replicas){
            node.tell(new QuorumRequestMsg(msg.key, new VersionedValue(msg.value, -1), Quorum.UPDATE, requestId), getSelf());
            logger.log(this.name.toString() + " " + this.state, "Sending UPDATE request to node " + node.path().name());
        }

        //schedule timeout to check if quorum is valid
        getContext().getSystem().scheduler().scheduleOnce(
            Duration.create(T, TimeUnit.MILLISECONDS),
            getSelf(),
            new QuorumTimeoutMsg(Quorum.GET, requestId),
            getContext().getSystem().dispatcher(),
            ActorRef.noSender()
        );
    }

    //changing of variables 
    private void onGetMsg(GetMsg msg){
        logger.log(this.name.toString() + " " + this.state, "Received GET request from " + getSender().path().name());

        //store the pending get request of the client
        PendingRequest pending = new PendingRequest(getSender());
        UUID requestId = UUID.randomUUID();
        pendingGets.put(requestId, pending);

        //send quorum request to replica nodes
        List<ActorRef> peers = setToList(this.peerList);
        int firstReplicaIndex = findResponsibleReplicaIndex(msg.key, peers);
        List<ActorRef> replicas = getNextN(peers, firstReplicaIndex);
        //replicas.add(peers.get(firstReplicaIndex));

        for(ActorRef node : replicas){
            node.tell(new QuorumRequestMsg(msg.key, null, Quorum.GET, requestId), getSelf());
            logger.log(this.name.toString() + " " + this.state, "Sending GET request to node " + node.path().name());
        }

        //schedule timeout to check if quorum is valid
        getContext().getSystem().scheduler().scheduleOnce(
            Duration.create(T, TimeUnit.MILLISECONDS),
            getSelf(),
            new QuorumTimeoutMsg(Quorum.UPDATE, requestId),
            getContext().getSystem().dispatcher(),
            ActorRef.noSender()
        );
    }

    // --- PROCEDURE HANDLERS -------------------------------
    /**
     * the bootstrap will wait for this message in order to return the peer list
     * @param msg contains the latest peer list
     */
    public void onRequestPeersMsg(RequestPeersMsg msg){
        // the bootstrap simply send back its list of peers
        getSender().tell(new PeersMsg(this.peerList, null, State.STABLE), getSelf());
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
        if(msg.request == Quorum.GET){
            //get request
            //the node responds to the coordinaotr with the stored value
            getSender().tell(new QuorumRequestMsg(msg.key, this.storage.get(msg.key), Quorum.GET_ACK, msg.requestId), getSelf());
            logger.log(this.name.toString() + " " + this.state, "Responding to GET request from " + getSender().path().name());
        } else if(msg.request == Quorum.UPDATE){
            //update request
            Integer version = 0;
            //check if the item is already stored
            VersionedValue item = storage.get(msg.key);
            if(item != null){
                version = item.version;
            }
            //in both cases increment so that if it is the first time, it will
            //have version 1 otherwise, it will be incremented by 1
            this.storage.put(msg.key, new VersionedValue(msg.value.value, ++version));
            getSender().tell(new QuorumRequestMsg(msg.key, this.storage.get(msg.key), Quorum.UPDATE_ACK, msg.requestId), getSelf());
            logger.log(this.name.toString() + " " + this.state, "Responding to UPDATE request from " + getSender().path().name());
        } else if(msg.request == Quorum.GET_ACK){
            //the coordinator is getting back and ack for get
            //check if value received is the latest version
            PendingRequest pending = pendingGets.get(msg.requestId);
            
            if(pending == null) return;//in case of errors

            if(msg.value != null && msg.value.version > pending.latest.version){
                pending.latest.version = msg.value.version;
                pending.latest.value = msg.value.value;
            }

            if(++pending.responses == R){
                //quorum reached, respond to client
                pending.client.tell(new GetResponse(true, pending.latest.value, pending.latest.version), getSelf());
                logger.log(this.name.toString() + " " + this.state, "GET request finalized");
                pendingGets.remove(msg.requestId);
            }
        } else if(msg.request == Quorum.UPDATE_ACK){
            //the coordinator is getting back and ack for update
            PendingRequest pending = pendingUpdates.get(msg.requestId);

            if(pending == null) return;//in case of errors

            if(++pending.responses == W){
                //quorum reached, respond to client
                pending.client.tell(new UpdateResponse(true), getSelf());
                logger.log(this.name.toString() + " " + this.state, "UPDATE request finalized");
                pendingUpdates.remove(msg.requestId);
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
            PendingRequest pending = pendingGets.get(msg.requestId);
            if (pending != null && pending.responses < R) {
                pending.client.tell(new GetResponse(false, null, -1), getSelf());
                logger.log(this.name.toString() + " " + this.state, "GET request TIMEOUT, no quorum");
                pendingGets.remove(msg.requestId);//clean up
            }
        } else if (msg.request == Quorum.UPDATE) {
            PendingRequest pending = pendingUpdates.get(msg.requestId);
            if (pending != null && pending.responses < W) {
                pending.client.tell(new UpdateResponse(false), getSelf());
                logger.log(this.name.toString() + " " + this.state, "UPDATE request TIMEOUT, no quorum");
                pendingUpdates.remove(msg.requestId);
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