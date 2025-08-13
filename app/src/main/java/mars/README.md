# Messages List

Given the large number of messages exchanged between Client and Node, this section provides a concise list with brief descriptions of each message type and its purpose.

Go back to [README.md](../../../../../README.md)

## Node Messages

### **State Messages**

| Message                    | Purpose                                                                              |
| -------------------------- | ------------------------------------------------------------------------------------ |
| `JoinMsg(bootstrap)`       | Sent by **Main** to a node when joining; node queries the topology from `bootstrap`. |
| `LeaveMsg`                 | Sent by **Main** to a node to initiate leaving.                                      |
| `CrashMsg`                 | Sent by **Main** to a node to simulate a crash.                                      |
| `RecoveryMsg(recoverNode)` | Sent by **Main** to a recovering node; asks `recoverNode` for peer list.             |

---

### **Service Messages**

| Message                                | Purpose                                                              |
| -------------------------------------- | -------------------------------------------------------------------- |
| `UpdateResponse(isValid)`              | From **Coordinator** to **Client** with outcome of an update.        |
| `GetResponse(isValid, value, version)` | From **Coordinator** to **Client** with the result of a get request. |

---

### **Procedure Messages**

| Message                                            | Purpose                                                                                   |
| -------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `RequestPeersMsg`                                  | Sent to a stable node to request network topology (joining/recovering nodes).             |
| `PeersMsg(peerSet, senderState)`                   | Contains network topology; sent in response to `RequestPeersMsg` or to announce presence. |
| `ItemsRequestMsg`                                  | Request another node’s storage (joining/recovering nodes to neighbors).                   |
| `ItemRepartitioningMsg(storage)`                   | Share storage with the receiver (reply to `ItemsRequestMsg`).                             |
| `QuorumRequestMsg(key, value, request, requestId)` | Used for **all quorum-based** messages.                     |
| `LeaveAnnouncement(dataToStore)`                   | Sent by a leaving node to share data for which others will be responsible.                |
| `LeaveAck`                                         | Sent by nodes receiving non-empty `LeaveAnnouncement.dataToStore`.                        |
| `LeaveOutcomeMsg(commit)`                          | Outcome of a leave request (`true` = commit, `false` = reject).                           |

---

### **Other Messages**

| Message                                     | Purpose                                                              |
| ------------------------------------------- | -------------------------------------------------------------------- |
| `LogStorage(color)`                         | Ask a node to print its storage (`GREEN` = stable, `RED` = crashed). |
| `TimeoutMsg`                                | Timeout while waiting for `PeersMsg` during join/recovery.           |
| `QuorumTimeoutMsg(key, request, requestId)` | Timeout while waiting for quorum acknowledgements.                   |
| `LeaveTimeoutMsg`                           | Timeout while waiting for `LeaveAck` messages.                       |

## Client Messages

| Message                      | Purpose                                                     |
| ---------------------------- | ----------------------------------------------------------- |
| `TimeoutMsg`                 | Timeout while waiting for a response from the Node.         |
| `UpdateNodeListMsg(nodeSet)` | Update the Client’s view of the active nodes in the system. |
| `UpdateMsg(key, value)`      | Request to update a key with a new value.                   |
| `GetMsg(key)`                | Request to retrieve the value for a given key.              |
