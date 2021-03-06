/*
 * Copyright 2017 Marco Helmich
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.carbon.grid.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.carbon.grid.CarbonGrid;
import org.carbon.grid.CarbonGridNodeIsShuttingDownException;
import org.carbon.grid.cluster.GloballyUniqueIdAllocator;
import org.carbon.grid.cluster.MyNodeId;
import org.carbon.grid.cluster.ReplicaIdSupplier;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is the main class.
 * It implements the MEOSI protocol and makes sure all cache lines are always in
 * a consistent state.
 *
 * Design ideas:
 * http://blog.paralleluniverse.co/2012/07/26/galaxy-internals-part-1/
 * http://blog.paralleluniverse.co/2012/08/03/galaxy-internals-part-2/
 * http://blog.paralleluniverse.co/2012/08/09/galaxy-internals-part-3/
 *
 * http://developer.amd.com/wordpress/media/2012/10/24593_APM_v21.pdf
 *
 * ToDo List:
 * - MOESI coherence protocol implementation
 * - fail over / topology changes / node failures
 */
@Singleton
class InternalCacheImpl implements InternalCache {
    private final static Logger logger = LoggerFactory.getLogger(InternalCacheImpl.class);

    // the cache lines I own
    private final NonBlockingHashMapLong<CacheLine> owned = new NonBlockingHashMapLong<>();
    // the cache lines I'm sharing and somebody else owns
    private final NonBlockingHashMapLong<CacheLine> shared = new NonBlockingHashMapLong<>();
    // this list is collecting all replication futures that are in-flight
    // after futures complete, they remove themselves out of this list
    // during a graceful shutdown, the cache is waiting for all these futures to complete before shutting down
    private final ArrayList<CompletableFuture<Void>> replicationFutures = new ArrayList<>();
    private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);

    final GridCommunications comms;
    private final Provider<Short> myNodeIdProvider;
    private short myNodeId = -1;
    private final CarbonGrid.ServerConfig serverConfig;
    private final CarbonGrid.CacheConfig cacheConfig;
    private final Provider<GloballyUniqueIdAllocator> idAllocatorProvider;
    private final Provider<ReplicaIdSupplier> replicaIdSupplierProvider;
    private final Replication replicationModule;

    // this field establishes a local node state with regards to backups
    // replica nodes will only back up cache lines that come with a
    // greater epoch than the one they know
    // during failover the node with the highest epoch value
    // will win the leader election
    private long localLeaderEpoch = Long.MIN_VALUE;

    @Inject
    InternalCacheImpl(
            @MyNodeId Provider<Short> myNodeIdProvider,
            CarbonGrid.CacheConfig cacheConfig,
            CarbonGrid.ServerConfig serverConfig,
            Provider<GloballyUniqueIdAllocator> idAllocatorProvider,
            Provider<ReplicaIdSupplier> replicaIdSupplierProvider,
            Replication replicationModule
    ) {
        this.myNodeIdProvider = myNodeIdProvider;
        this.serverConfig = serverConfig;
        this.cacheConfig = cacheConfig;
        this.idAllocatorProvider = idAllocatorProvider;
        this.replicaIdSupplierProvider = replicaIdSupplierProvider;
        this.replicationModule = replicationModule;
        this.comms = new GridCommunications(myNodeIdProvider, serverConfig, this);
    }

    @Override
    public void handlePeerChange(Map<Short, InetSocketAddress> nodeIdToAddr) {
        comms.setPeers(nodeIdToAddr);
    }

    ///////////////////////////////////////
    ////////////////////////////////
    //////////////////////////
    // ENTRY POINT OF ASYNC RESPONSE PROCESSING
    // INSIDE OF NETTY WORKER THREADS
    // KEEP THIS QUICK AND NIMBLE
    @Override
    public void handleResponse(Message.Response response) {
        preHandler(response);
        switch (response.type) {
            case ACK:
                handleACK((Message.ACK)response);
                return;
            case PUT:
                handlePUT((Message.PUT)response);
                return;
            case PUTX:
                handlePUTX((Message.PUTX)response);
                return;
            case INVACK:
                handleINVACK((Message.INVACK)response);
                return;
            case OWNER_CHANGED:
                handleOWNER_CHANGED((Message.OWNER_CHANGED)response);
                return;
            case BACKUP_ACK:
                handleBACKUP_ACK((Message.BACKUP_ACK)response);
                return;
            default:
                throw new RuntimeException("Unknown type " + response.type);
        }
    }

    ///////////////////////////////////////
    ////////////////////////////////
    //////////////////////////
    // ENTRY POINT OF ASYNC REQUEST PROCESSING
    // INSIDE OF NETTY WORKER THREADS
    // KEEP THIS QUICK AND NIMBLE
    @Override
    public Message.Response handleRequest(Message.Request request) {
        preHandler(request);
        switch (request.type) {
            case GET:
                return handleGET((Message.GET)request);
            case GETX:
                return handleGETX((Message.GETX)request);
            case INV:
                return handleINV((Message.INV)request);
            case BACKUP:
                return handleBACKUP((Message.BACKUP)request);
            case REMOVE_BACKUP:
                return handleREMOVE_BACKUP((Message.REMOVE_BACKUP)request);
            case CHANGE_OWNER:
                return handleCHANGE_OWNER((Message.CHANGE_OWNER)request);
            default:
                throw new RuntimeException("Unknown type " + request.type);
        }
    }

    // this method returns a boolean indicating whether this message
    // should be processed right away or whether it was queued up for later
    private boolean preHandler(Message msg) throws ShouldNotProcessException {
        long lineId = msg.lineId;
        CacheLine line = innerGetLineLocally(lineId);
        if (line != null && line.isLocked()) {
            // throw an exception indicating that this message should remain in the backlog
            // while this is not great, it's pretty effective (code-writing-wise)
            // this exception bridges multiple levels of abstraction and is only necessary
            // for the first message for a cache line that's coming here
            throw new ShouldNotProcessException("Line is locked");
        }

        if (isShuttingDown.get()) {
            throw new CarbonGridNodeIsShuttingDownException("Node is shutting down");
        }

        return true;
    }

    private Message.Response handleGETX(Message.GETX getx) {
        logger.info("cache handler {} getx: {}", this, getx);
        CacheLine line = owned.remove(getx.lineId);
        if (line != null) {
            // I'm the owner
            // let's demote the cache line to invalid,
            // pack it up and ship it to the requester
            Set<Short> sharersToSend = line.getSharers();
            ByteBuf bufferToSend = line.resetReaderAndGetReadOnlyData().retain();

            // change local status and status of cache line
            line.setState(CacheLineState.INVALID);
            line.setOwner(getx.getSender());
            line.clearSharers();
            line.releaseData();
            shared.put(getx.lineId, line);

            // massage sharers list
            sharersToSend.remove(getx.getSender());
            sharersToSend.remove(myNodeId());

            // compose message
            return new Message.PUTX(
                    getx.getMessageSequenceNumber(),
                    myNodeId(),
                    line.getId(),
                    line.getVersion(),
                    sharersToSend,
                    bufferToSend
            );
        } else {
            // I'm not the owner
            // let's see whether I find the line
            // in the sharer map and see who the new owner is
            line = shared.get(getx.lineId);
            if (line == null) {
                return new Message.ACK(getx.getMessageSequenceNumber(), myNodeId(), getx.lineId);
            } else {
                return new Message.OWNER_CHANGED(
                        getx.getMessageSequenceNumber(),
                        myNodeId(),
                        line.getId(),
                        line.getOwner(),
                        MessageType.GETX
                );
            }
        }
    }

    private Message.Response handleGET(Message.GET get) {
        logger.info("cache handler {} get: {}", this, get);
        CacheLine line = owned.get(get.lineId);
        if (line != null) {
            if (CacheLineState.EXCLUSIVE.equals(line.getState())) {
                line.setState(CacheLineState.OWNED);
            }
            line.addSharer(get.getSender());
            return new Message.PUT(
                    get.getMessageSequenceNumber(),
                    myNodeId(),
                    line.getId(),
                    line.getVersion(),
                    line.resetReaderAndGetReadOnlyData().retain()
            );
        } else {
            line = shared.get(get.lineId);
            if (line != null) {
                // I'm not the owner
                // let's see whether I find the line
                // in the sharer map and see who the new owner is
                return new Message.OWNER_CHANGED(
                        get.getMessageSequenceNumber(),
                        myNodeId(),
                        line.getId(),
                        line.getOwner(),
                        MessageType.GET
                );
            } else {
                // I don't know this cache line at all
                // looks like I'm part of a desperate broadcast
                // I'll just acknowledge the message and move on with my life
                return new Message.ACK(
                        get.getMessageSequenceNumber(),
                        myNodeId(),
                        get.lineId
                );
            }
        }
    }

    private void handleACK(Message.ACK ack) {
        logger.info("cache handler {} ack: {}", this, ack);
    }

    private void handlePUTX(Message.PUTX putx) {
        logger.info("cache handler {} putx: {}", this, putx);
        CacheLine oldLine = shared.get(putx.lineId);
        if (oldLine == null) {
            oldLine = owned.get(putx.lineId);
            if (oldLine == null) {
                // looks like I didn't have this log line before
                // pure luxury, I can create a brand new object
                CacheLine newLine = new CacheLine(
                        putx.lineId,
                        putx.version,
                        myNodeId(),
                        putx.sharers.isEmpty() ? CacheLineState.EXCLUSIVE : CacheLineState.OWNED,
                        putx.data
                );
                newLine.setSharers(putx.sharers);
                owned.put(putx.lineId, newLine);
            } else {
                // promote line from shared to owned
                // It's somewhat important to keep the same object around
                // (as opposed to creating a new one and tossing the old one away).
                // One reason for that is that there might be other threads
                // waiting to grab a lock on this line.
                // If we switch the objects under the waiters all hell will break loose.
                oldLine.setOwner(myNodeId());
                oldLine.setSharers(putx.sharers);
                oldLine.setState(
                        oldLine.getSharers().isEmpty() ? CacheLineState.EXCLUSIVE : CacheLineState.OWNED
                );
                oldLine.setData(putx.data);
                oldLine.setVersion(putx.version);
            }
        } else {
            // promote line from shared to owned
            // It's somewhat important to keep the same object around
            // (as opposed to creating a new one and tossing the old one away).
            // One reason for that is that there might be other threads
            // waiting to grab a lock on this line.
            // If we switch the objects under the waiters all hell will break loose.
            oldLine.setOwner(myNodeId());
            oldLine.setSharers(putx.sharers);
            oldLine.setState(
                    oldLine.getSharers().isEmpty() ? CacheLineState.EXCLUSIVE : CacheLineState.OWNED
            );
            oldLine.setData(putx.data);
            oldLine.setVersion(putx.version);
            // I rather live with double-occurrence than the line not
            // showing up at all
            owned.put(putx.lineId, oldLine);
            shared.remove(putx.lineId);
        }

        replicateCacheLine(putx.lineId);
    }

    private void handlePUT(Message.PUT put) {
        logger.info("cache handler {} put: {}", this, put);
        // this is fairly straight forward
        // just take the new data and shove it in
        CacheLine line = shared.get(put.lineId);
        if (line == null) {
            line = new CacheLine(put.lineId, put.version, put.getSender(), CacheLineState.SHARED, put.data);
            shared.put(put.lineId, line);
        } else {
            line.setVersion(put.version);
            line.setOwner(put.sender);
            line.setState(CacheLineState.SHARED);
            line.setData(put.data);
        }

        replicateCacheLine(line);
    }

    private Message.Response handleINV(Message.INV inv) {
        logger.info("cache handler {} inv: {}", this, inv);
        CacheLine line = shared.get(inv.lineId);
        if (line == null) {
            line = new CacheLine(inv.lineId, -1, (short)-1, CacheLineState.INVALID, null);
            shared.put(inv.lineId, line);
        } else {
            // we're in luck I know the line
            line.setState(CacheLineState.INVALID);
            line.setOwner(inv.getSender());
            line.releaseData();
        }
        return new Message.INVACK(
                inv.getMessageSequenceNumber(),
                myNodeId(),
                inv.lineId
        );
    }

    private void handleINVACK(Message.INVACK invack) {
        logger.info("cache handler {} invack: {}", this, invack);
        CacheLine line = owned.get(invack.lineId);
        if (line != null) {
            line.removeSharer(invack.getSender());
            if (line.getSharers().isEmpty()) {
                line.setState(CacheLineState.EXCLUSIVE);
            }
        }
    }

    private void handleOWNER_CHANGED(Message.OWNER_CHANGED ownerChanged) {
        logger.info("cache handler {} ownerChanged: {}", this, ownerChanged);
        CacheLine line = shared.get(ownerChanged.lineId);
        if (line != null) {
            line.setState(CacheLineState.INVALID);
            line.setOwner(ownerChanged.newOwner);

            final Message.Request messageToSend;
            switch (ownerChanged.originalMsgType) {
                case GET:
                    messageToSend = new Message.GET(myNodeId(), ownerChanged.lineId);
                    break;
                case GETX:
                    messageToSend = new Message.GETX(myNodeId(), ownerChanged.lineId);
                    break;
                default:
                    throw new RuntimeException("unknown message type " + ownerChanged.originalMsgType);
            }

            comms.reactToResponse(ownerChanged, ownerChanged.newOwner, messageToSend);
        }
    }

    private Message.Response handleCHANGE_OWNER(Message.CHANGE_OWNER changeOwner) {
        logger.info("cache handler {} changeOwner: {}", this, changeOwner);
        return null;
    }

    private Message.Response handleBACKUP(Message.BACKUP backup) {
        logger.info("cache handler {} backup: {}", this, backup);
        CacheLine lineToBackUp = new CacheLine(backup.lineId, backup.version, backup.sender, CacheLineState.INVALID, backup.buffer);
        replicationModule.backUp(backup.sender, backup.leaderEpoch, lineToBackUp);
        return new Message.BACKUP_ACK(backup.messageSequenceNumber, myNodeId(), backup.lineId, backup.leaderEpoch);
    }

    private void handleBACKUP_ACK(Message.BACKUP_ACK backupAck) {
        logger.info("cache handler {} backupAck: {}", this, backupAck);
    }

    private Message.Response handleREMOVE_BACKUP(Message.REMOVE_BACKUP removeBackup) {
        logger.info("cache handler {} removeBackup: {}", this, removeBackup);
        replicationModule.stopBackupFor(removeBackup.sender);
        return new Message.BACKUP_ACK(removeBackup.messageSequenceNumber, myNodeId(), removeBackup.lineId, Long.MIN_VALUE);
    }




    /////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////
    /////////////////////////////////////////////
    //                       NORTH OF HERE IS ASYNC NETTY CODE
    //           MESSAGE HANDLERS AND ALL CODE THAT BLOCKS NETTY THREADS
    //                         NETTY CODE CANNOT BLOCK !!!
    //                   SOUTH OF HERE IS CUSTOMER FACING API CODE
    //            CUSTOMER-FACING CODE CAN BLOCK (WAIT ON FUTURES, ETC.)




    private long nextClusterUniqueCacheLineId() {
        return idAllocatorProvider.get().nextUniqueId();
    }

    @Override
    public int getMaxCacheLineSize() {
        return cacheConfig.maxCacheLineSize();
    }

    @Override
    public ByteBuf allocateBuffer(int capacity) {
        assertNodeIsStillUp();
        return PooledByteBufAllocator.DEFAULT.directBuffer(capacity, getMaxCacheLineSize());
    }

    @Override
    public long allocateEmpty(Transaction txn) throws IOException {
        assertNodeIsStillUp();
        TransactionImpl t = (TransactionImpl) txn;
        long newLineId = nextClusterUniqueCacheLineId();
        CacheLine line = new CacheLine(
                newLineId,
                0,
                myNodeId(),
                CacheLineState.EXCLUSIVE,
                null
        );
        line.lock();
        owned.put(line.getId(), line);
        t.recordUndo(line, null);
        return line.getId();
    }

    @Override
    public long allocateWithData(ByteBuf buffer, Transaction txn) throws IOException {
        assertNodeIsStillUp();
        TransactionImpl t = (TransactionImpl) txn;
        if (buffer.capacity() > getMaxCacheLineSize()) throw new IllegalArgumentException("Buffer too big! The buffer can only have a max size of " + getMaxCacheLineSize() + " bytes");

        long newLineId = nextClusterUniqueCacheLineId();
        CacheLine line = new CacheLine(
                newLineId,
                0,
                myNodeId(),
                CacheLineState.EXCLUSIVE,
                null
        );

        line.lock();
        t.recordUndo(line, buffer);
        owned.put(line.getId(), line);
        return line.getId();
    }

    @Override
    public long allocateWithData(byte[] bites, Transaction txn) throws IOException {
        assertNodeIsStillUp();
        ByteBuf buffer = allocateBuffer(bites.length).writeBytes(bites);
        return allocateWithData(buffer, txn);
    }

    @Override
    public ByteBuf get(long lineId) throws IOException {
        assertNodeIsStillUp();
        CacheLine line = getLineLocally(lineId);
        if (line == null) {
            CacheLine remoteLine = getLineRemotely(lineId);
            if (remoteLine == null) {
                return null;
            } else {
                return remoteLine.resetReaderAndGetReadOnlyData().retain();
            }
        } else {
            return line.resetReaderAndGetReadOnlyData().retain();
        }
    }

    @Override
    public ByteBuf getx(long lineId, Transaction txn) throws IOException {
        assertNodeIsStillUp();
        TransactionImpl t = (TransactionImpl) txn;
        CacheLine line = getxLineRemotely(lineId);
        if (line == null) {
            return null;
        } else {
            line.lock();
            t.addToLockedLines(line.getId());
            return line.resetReaderAndGetReadOnlyData().retain();
        }
    }

    @Override
    public void put(long lineId, ByteBuf buffer, Transaction txn) throws IOException {
        assertNodeIsStillUp();
        TransactionImpl t = (TransactionImpl) txn;
        CacheLine line = getLineLocally(lineId);
        if (line == null || !CacheLineState.EXCLUSIVE.equals(line.getState())) {
            line = getxLineRemotely(lineId);
            if (line == null) {
                line = new CacheLine(
                        lineId,
                        0,
                        myNodeId(),
                        CacheLineState.EXCLUSIVE,
                        null
                );
                owned.put(lineId, line);
            }
        }

        line.lock();
        t.recordUndo(line, buffer);
        buffer.retain();
    }

    @Override
    public Transaction newTransaction() {
        assertNodeIsStillUp();
        return new TransactionImpl(this);
    }

    private CacheLine getxLineRemotely(long lineId) throws IOException {
        Message.GETX getx = new Message.GETX(myNodeId(), lineId);
        Future<Void> getxFuture = innerGenericGetLineRemotely(getx);
        try {
            getxFuture.get(serverConfig.timeout(), TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException xcp) {
            throw new IOException(xcp);
        }

        CacheLine line = owned.get(lineId);
        if (line == null) {
            return null;
        } else {
            CompletableFuture<Void> invalidateFuture = comms.send(line.getSharers(), new Message.INV(myNodeId(), lineId));

            try {
                invalidateFuture.get(serverConfig.timeout(), TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException xcp) {
                throw new IOException(xcp);
            }

            return getLineLocally(lineId);
        }
    }

    private CacheLine getLineRemotely(long lineId) throws IOException {
        Message.GET get = new Message.GET(myNodeId(), lineId);
        Future<Void> getFuture = innerGenericGetLineRemotely(get);
        try {
            getFuture.get(serverConfig.timeout(), TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException xcp) {
            throw new IOException(xcp);
        }
        return getLineLocally(lineId);
    }

    private Future<Void> innerGenericGetLineRemotely(Message.Request anyGetMessage) throws IOException {
        // it might be worth checking the shared map to see whether
        // we find a stub but the line is invalid
        long lineId = anyGetMessage.lineId;
        CacheLine line = getLineLocally(lineId);
        if (line != null) {
            // AHA!!
            return innerUnicast(line.getOwner(), anyGetMessage);
        } else {
            // nope we don't know anything
            // we gotta find out where this thing is first
            line = new CacheLine(lineId, -1, (short)-1, CacheLineState.INVALID, null);
            shared.put(lineId, line);
            return innerBroadcast(anyGetMessage);
        }
    }

    private Future<Void> innerUnicast(short nodeToSendTo, Message msgToSend) throws IOException {
        return comms.send(nodeToSendTo, msgToSend);
    }

    private Future<Void> innerBroadcast(Message.Request msgToSend) throws IOException {
        final Future<Void> future;
        MessageType[] msgToWaitFor = msgToSend.messagesToWaitForUntilFutureCompletes();
        if (msgToWaitFor == null) {
            future = comms.broadcast(msgToSend);
        } else {
            future = comms.broadcast(msgToSend, msgToWaitFor);
        }
        return future;
    }

    // this method will return null even if we know about the line
    // but can't use it (in other words: the line is in state I)
    private CacheLine getLineLocally(long lineId) {
        CacheLine line = innerGetLineLocally(lineId);
        if (line != null && CacheLineState.INVALID.equals(line.getState())) {
            // if this line is marked invalid, pretend we don't know it :)
            return null;
        } else {
            return line;
        }
    }

    @VisibleForTesting
    CacheLine innerGetLineLocally(long lineId) {
        CacheLine line = owned.get(lineId);
        if (line == null) {
            line = shared.get(lineId);
        }
        return line;
    }

    void makeUncommittedChange(long lineId, ByteBuf buffer, TransactionImpl txn) {
        CacheLine lineToChange = owned.get(lineId);
        assert lineToChange != null;
        // you can only make changes if you're the exclusive holder of this cache line
        if (!CacheLineState.EXCLUSIVE.equals(lineToChange.getState())) {
            throw new IllegalStateException("Cache line " + lineId + " is about to be changed but it's in state " + lineToChange.getState());
        }
        txn.recordUndo(lineToChange, buffer);
    }

    short myNodeId() {
        if (myNodeId == -1) {
            synchronized (myNodeIdProvider) {
                if (myNodeId == -1) {
                    myNodeId = myNodeIdProvider.get();
                }
            }
        }
        return myNodeId;
    }

    // this is called after a transaction is rolled back or committed
    // while a transaction locks cache lines, all incoming messages are queued up
    // after these cache lines are released, someone needs to process these messages
    void processMessagesForId(long cacheLineId) {
        comms.processMessagesForId(cacheLineId);
    }

    // it's a little unfortunate to have this code here as opposed to in the
    // Replication class ... I might devise of a grand scheme to redirect
    // backup messages to the replication class later but for now this will do
    // this is called from a transaction or a put* handler
    // in both cases this nodes acquires ownership of new cache lines
    // and those need to be backed up
    void replicateCacheLine(long lineId) {
        CacheLine line = owned.get(lineId);
        replicateCacheLine(line);
    }

    private void replicateCacheLine(CacheLine line) {
        if (line != null) {
            localLeaderEpoch++;
            Message backupMsg = new Message.BACKUP(
                    myNodeId(),
                    line.getId(),
                    localLeaderEpoch,
                    localLeaderEpoch,
                    line.getVersion(),
                    line.resetReaderAndGetReadOnlyData()
            );
            try {
                List<Short> replicaIds = replicaIdSupplierProvider.get().get();
                // collect the completion future
                CompletableFuture<Void> replicationFuture = comms.send(replicaIds, backupMsg);
                // add it to the list of futures to wait for a graceful shutdown
                replicationFutures.add(replicationFuture);
                // if the future completes, remove it from the list of all futures
                replicationFuture.thenAcceptAsync((aVoid -> {
                    synchronized (replicationFutures) {
                        replicationFutures.remove(replicationFuture);
                    }
                }));
            } catch (IOException xcp) {
                throw new RuntimeException(xcp);
            }
        }
    }

    private void assertNodeIsStillUp() {
        if (isShuttingDown.get()) {
            throw new CarbonGridNodeIsShuttingDownException("This node is going down...");
        }
    }

    @Override
    public void close() throws IOException {
        isShuttingDown.set(true);
        logger.info("Waiting for all replication futures to complete...");
        List<CompletableFuture<Void>> repFutures;
        // make a defensive copy
        synchronized (replicationFutures) {
            repFutures = ImmutableList.copyOf(replicationFutures);
        }
        for (int i = 0; i < repFutures.size(); i++) {
            CompletableFuture<Void> f = repFutures.get(i);
            try {
                f.get(10, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException xcp) {
                logger.error("", xcp);
            }
            logger.info("Waited for {} futures. {} more to go...", i, repFutures.size() - i);
        }
        logger.info("Shutting down comms...");
        comms.close();
        logger.info("Releasing local data...");
        for (CacheLine line : owned.values()) {
            line.releaseData();
        }
        for (CacheLine line : shared.values()) {
            line.releaseData();
        }
    }

    @Override
    public String toString() {
        return "myNodeId: " + myNodeId();
    }
}
