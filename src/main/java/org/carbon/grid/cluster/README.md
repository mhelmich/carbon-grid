# CarbonGrid Cluster Module

This page explains the inner workings of the CarbonGrid Cluster Module. It specifically details how a cluster is established, how failover works, etc.

TOC:
* the general carbon grid trade offs
  * in-memory, shared-nothing
  * the concept of ownership
  * the concept of availability and failure detection
  * goals and non-goals of the cluster mechanism
* structure kept track of in consul
  * how to get the global cluster state
* distributed decision making
  * crush
* interaction between cache and cluster
  * message passing between the two
* the lifecycle of a node
  * how does a new node find its role in the cluster
  * leader and follower allocation
  * failover or how followers become leaders

## CarbonGrid Cluster Goals

* Availability - CarbonGrid should recover from a configured number of independent node failures and remain operational.
* Scalability - CarbonGrid should still scale (near) linearly as nodes are added.

## General Clustering Concepts in CarbonGrid

Before we dive right in, let's clarify a few general concepts and designs.

### Consensus

The centerpiece of every cluster is its consensus mechanism - that is how the cluster makes sure all its nodes have the same view of the world.
CarbonGrid nodes all coordinate via a shared consul cluster acting as a central source of truth.

### Cache Lines and Ownership in CarbonGrid

Following the [MOESI protocol](http://developer.amd.com/wordpress/media/2012/10/24593_APM_v21.pdf) every cache line is its own independent entity. A cache line is the alpha and omega in terms of data grouping. All data is organized in cache lines and cache lines are the only entity to reason about. They can be identified via a unique id and their state can be manipulated via message passing to other nodes. Their content though can only be changed after acquiring ownership. That has the nice property that no data-mutating messages are sent ever. Furthermore, CarbonGrid implements a shared-nothing node cluster in which data is only mutated locally after acquiring ownership of a cache line.

### Node Connectivity in the Cluster

Every node in the cluster has the ability to connect to all other nodes in the cluster via a persistent TCP connection. While that seems to be wasteful, the trade off is between taking the hit of session-based connections or implementing message ordering and delivery guarantees on top of a session-less protocol like UDP. Faced with that decision I went with TCP connections. If network communication becomes a bottleneck, we can always build ordering and guaranteed delivery on UDP.

### Liveness of a Node
A node can have various states and a lot of information can be associated with a node. The most important one though is its liveness. Liveness refers to whether a node is available (up) or whether it is not available (failed, down, crashed, etc.). The most part of CarbonGrid clustering code is there to establish a global view on which nodes are available, which roles available nodes have, and how these available nodes can be reached.
The liveness concept bases on consul sessions. All values that nodes create in consul are tied to consul sessions. Clients are configured in a way that values are being deleted if the session times out. Failure to touch (update) the session for an extended period of time will expire the session and with it delete all values the node has created. A node is determined dead if its session expires and its NodeInfo disappeared.

### Roles of a Node

The cluster consists of leader and follower nodes. Regardless of the sharing state of cache lines, the cluster tries to always keep each cache line highly available. Even if the cache line is in shared state with several up-to-date versions in other nodes, the cluster will proceed replicating cache lines for high availability. This replication happens between leader nodes and follower nodes. Each node fulfills both of these roles (is leader and follower) at the same time.

### NodeId
The node id is a cluster-wide unique id that a node acquires at startup and retains for its entire life. Node ids are given out in a consecutive, monotonically increasing block. Newer ids are greater than older ones. However nodes that join a cluster will try to fill gaps between the node ids that have been given out. As an example: There are three nodes in our cluster - node ids 100, 101, and 102. If the node with the id 101 were to fail, there is a gap in the consecutive block of node ids. The next node to join will try to fill this gap and acquire the id 101. That means the moment a node leaves the cluster its old node id becomes available to the next node to join. 

### NodeInfo 
This contains all the information that a node publishes about itself in consul. The node info contains this nodes id, information about its physical location (in terms of data centers, racks, machines, etc.), the ids of all its leader nodes, and the ids of all its follower nodes.

### Listening for Changes in the Cluster

Each node listens for changes in the NodeInfo or liveness of other nodes.
All nodes listen on every other nodes NodeInfo object. When this NodeInfo object changes, a callback runs inside every other node, in order to react to the changed cluster landscape. All failover mechanisms are triggered by these callbacks.

## The Lifecycle of a Node

A node starts up and creates a session with consul (this session is used to determine liveness). Each node then has the possibility to react on changes in the cluster (e.g. start a leader election, leader promotion, etc.). With all its metadata in place, CarbonGrid nodes are leader and follower at the same time. There are no partitions and the highest level data grouping is a cache line. Ownership is defined on basis of cache lines and therefore nodes start up and have no ownership of any cache lines but they are part of the cluster. As they received (or make) requests they become owners of cache lines. This scheme has the advantage of not needing constantly rebalance workload by mapping partitions back and forth.
