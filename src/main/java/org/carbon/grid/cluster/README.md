# CarbonGrid Cluster Module

This page explains the inner workings of the CarbonGrid cluster module. Specifically it details how a cluster is established, how failover works, etc.

## Consensus

The centerpiece of every cluster is its consensus mechanism - that is how the cluster makes sure all its nodes have the same view of the world.
CarbonGrid nodes all coordinate via a shared consul cluster acting as a central source of truth.

TOC:
* explain the general carbon grid trade offs
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
