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

import com.google.common.hash.Funnel;
import com.google.common.hash.Hashing;
import io.netty.buffer.ByteBuf;
import org.cliffc.high_scale_lib.NonBlockingHashSet;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

abstract class Message implements Persistable {

    final MessageType type;
    int messageSequenceNumber;
    short sender;
    long lineId;

    Message(MessageType type) {
        this.type = type;
    }

    @Override
    public void write(MessageOutput out) throws IOException {
        out.writeByte(type.ordinal);
        out.writeInt(messageSequenceNumber);
        out.writeShort(sender);
        out.writeLong(lineId);
    }

    @Override
    public void read(MessageInput in) throws IOException {
        messageSequenceNumber = in.readInt();
        sender = in.readShort();
        lineId = in.readLong();
    }

    @Override
    public int byteSize() {
        return calcMessagesByteSize();
    }

    protected int calcMessagesByteSize() {
        return 1     // message type byte
             + 4     // message id int
             + 2     // sender short
             + 8     // line id long
             ;
    }

    Message copy() {
        throw new NotImplementedException();
    }

    int getMessageSequenceNumber() {
        return messageSequenceNumber;
    }

    void setMessageSequenceNumber(int messageSequenceNumber) {
        this.messageSequenceNumber = messageSequenceNumber;
    }

    short getSender() {
        return sender;
    }

    void writeByteBuf(MessageOutput out, ByteBuf buffer) throws IOException {
        if (buffer == null) {
            out.writeInt(0);
        } else {
            out.writeInt(buffer.capacity());
            out.writeByteBuf(buffer.resetReaderIndex());
            buffer.release();
        }
    }

    @Override
    public String toString() {
        return "message type: " + type + " messageSequenceNumber: " + messageSequenceNumber + " sender: " + sender + " lineId: " + lineId;
    }

    @Override
    public boolean equals(Object obj) {
        if (Message.class.isInstance(obj)) {
            Message that = (Message)obj;
            return this.type.equals(that.type)
                    && this.sender == that.sender
                    && this.messageSequenceNumber == that.messageSequenceNumber
                    && this.lineId == that.lineId;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Hashing.goodFastHash(32)
                .hashObject(this, messageFunnel)
                .asInt();
    }

    private static final Funnel<Message> messageFunnel = (Funnel<Message>) (msg, into) -> into
            .putByte(msg.type.ordinal)
            .putShort(msg.sender)
            .putInt(msg.messageSequenceNumber)
            .putLong(msg.lineId)
            ;

    // the same as
    // if (dis == null && dat == null) return true;
    // else if (dis != null && dat != null) return ByteBufUtil.equals(dis, dat);
    // else return false;
    boolean equalsByteBuf(ByteBuf dis, ByteBuf dat) {
        return dis == null && dat == null
                || dis != null && dat != null && dis.capacity() == dat.capacity();
                                            // && ByteBufUtil.equals(dis, dat);
                                            // I somewhat wanted to use this but
                                            // equals also depends on the underlying implementation
                                            // of the ByteBuf instance
                                            // for now ByteBufs are the same if they have the same size *sigh*
    }

    static abstract class Request extends Message {
        Request(MessageType type) {
            super(type);
        }

        // not nice but effective
        // this method indicates to the cache what it should wait for in case of broadcasts
        // the standard behavior (returning null) for broadcasts means:
        // "wait for a message to come back from each node I asked before completing"
        // needless to say this is little efficient when it comes to broadcasting GETs
        // in those cases the node waits for ACKs from all nodes before completing ...
        // ... even if the first message that was received is the PUT
        // by returning MessageType.PUT a message can indicate that a broadcast should
        // only wait for one PUT before completing
        // this array can have multiple entries and also multiple entires of the same message type
        MessageType[] messagesToWaitForUntilFutureCompletes() {
            return null;
        }
    }

    static abstract class Response extends Message {
        Response(MessageType type) {
            super(type);
        }
    }

    static class GET extends Request {
        private GET() {
            super(MessageType.GET);
        }

        GET(short sender, long lineId) {
            super(MessageType.GET);
            this.sender = sender;
            this.lineId = lineId;
        }

        @Override
        GET copy() {
            return new GET(sender, lineId);
        }

        @Override
        public boolean equals(Object obj) {
            if (GET.class.isInstance(obj)) {
                GET that = (GET)obj;
                return super.equals(that);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        MessageType[] messagesToWaitForUntilFutureCompletes() {
            return new MessageType[] { MessageType.PUT };
        }
    }

    static class PUT extends Response {
        int version;
        ByteBuf data;

        private PUT() {
            super(MessageType.PUT);
        }

        PUT(int requestMessageSequenceNumber, short sender, long lineId, int version, ByteBuf data) {
            super(MessageType.PUT);
            this.messageSequenceNumber = requestMessageSequenceNumber;
            this.sender = sender;
            this.lineId = lineId;
            this.version = version;
            this.data = data;
        }

        @Override
        public int calcMessagesByteSize() {
            return super.calcMessagesByteSize()
                    + 4                                        // version number int
                    + 4                                        // bytebuf size
                    + ((data == null) ? 0 : data.capacity())   // buffer content
                    ;
        }

        @Override
        public void write(MessageOutput out) throws IOException {
            super.write(out);
            out.writeInt(version);
            writeByteBuf(out, data);
        }

        @Override
        public void read(MessageInput in) throws IOException {
            super.read(in);
            version = in.readInt();
            int bytesToRead = in.readInt();
            data = (bytesToRead == 0) ? null : in.readByteBuf(bytesToRead);
        }

        @Override
        public boolean equals(Object obj) {
            if (PUT.class.isInstance(obj)) {
                PUT that = (PUT)obj;
                return super.equals(that)
                        && this.version == that.version
                        && equalsByteBuf(this.data, that.data);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Hashing.goodFastHash(32)
                    .hashObject(this, putFunnel)
                    .asInt();
        }

        private static final Funnel<PUT> putFunnel = (Funnel<PUT>) (msg, into) -> into
                .putByte(msg.type.ordinal)
                .putShort(msg.sender)
                .putInt(msg.messageSequenceNumber)
                .putLong(msg.lineId)
                .putInt(msg.version)
                .putInt((msg.data == null) ? 0 : msg.data.capacity())
                ;
    }

    static class ACK extends Response {
        private ACK() {
            super(MessageType.ACK);
        }

        ACK(int requestMessageSequenceNumber, short sender, long lineId) {
            super(MessageType.ACK);
            this.messageSequenceNumber = requestMessageSequenceNumber;
            this.sender = sender;
            this.lineId = lineId;
        }

        @Override
        public boolean equals(Object obj) {
            if (ACK.class.isInstance(obj)) {
                ACK that = (ACK)obj;
                return super.equals(that);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }

    static class GETX extends Request {
        private GETX() {
            super(MessageType.GETX);
        }

        GETX(short sender, long lineId) {
            super(MessageType.GETX);
            this.sender = sender;
            this.lineId = lineId;
        }

        @Override
        GETX copy() {
            return new GETX(sender, lineId);
        }

        @Override
        public boolean equals(Object obj) {
            if (GETX.class.isInstance(obj)) {
                GETX that = (GETX)obj;
                return super.equals(that);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        MessageType[] messagesToWaitForUntilFutureCompletes() {
            return new MessageType[] { MessageType.PUTX };
        }
    }

    static class PUTX extends Response {
        int version;
        Set<Short> sharers;
        ByteBuf data;

        private PUTX() {
            super(MessageType.PUTX);
        }

        PUTX(int requestMessageSequenceNumber, short sender, long lineId, int version, Set<Short> sharers, ByteBuf data) {
            super(MessageType.PUTX);
            this.messageSequenceNumber = requestMessageSequenceNumber;
            this.sender = sender;
            this.lineId = lineId;
            this.version = version;
            this.sharers = sharers;
            this.data = data;
        }

        @Override
        public int calcMessagesByteSize() {
            return super.calcMessagesByteSize()
                    + 4                                          // version number int
                    + 2                                          // num sharers short
                    + ((sharers == null) ? 0 :(2 * sharers.size())) // all sharers short
                    + 4                                          // bytebuf size
                    + ((data == null) ? 0 : data.capacity())     // buffer content
                    ;
        }

        @Override
        public void write(MessageOutput out) throws IOException {
            super.write(out);
            out.writeInt(version);
            if (sharers == null) {
                out.writeShort(0);
            } else {
                out.writeShort(sharers.size());
                for (Short s : sharers) {
                    out.writeShort(s);
                }
            }
            writeByteBuf(out, data);
        }

        @Override
        public void read(MessageInput in) throws IOException {
            super.read(in);
            version = in.readInt();
            short numSharers = in.readShort();
            if (numSharers == 0) {
                sharers = null;
            } else {
                sharers = new NonBlockingHashSet<>();
                for (int i = 0; i < numSharers; i++) {
                    sharers.add(in.readShort());
                }
            }
            int bytesToRead = in.readInt();
            data = (bytesToRead == 0) ? null : in.readByteBuf(bytesToRead);
        }

        @Override
        public boolean equals(Object obj) {
            if (PUTX.class.isInstance(obj)) {
                PUTX that = (PUTX)obj;
                return super.equals(that)
                        && this.version == that.version
                        && Objects.equals(this.sharers, that.sharers)
                        && equalsByteBuf(this.data, that.data);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Hashing.goodFastHash(32)
                    .hashObject(this, putxFunnel)
                    .asInt();
        }

        private static final Funnel<PUTX> putxFunnel = (Funnel<PUTX>) (msg, into) -> into
                .putByte(msg.type.ordinal)
                .putShort(msg.sender)
                .putInt(msg.messageSequenceNumber)
                .putLong(msg.lineId)
                .putInt(msg.version)
                .putInt((msg.sharers == null) ? 0 : msg.sharers.hashCode())
                .putInt((msg.data == null) ? 0 : msg.data.capacity())
                ;
    }

    static class OWNER_CHANGED extends Response {
        short newOwner;
        // this is used in the handler to compose
        // a request to the new owner
        MessageType originalMsgType;

        private OWNER_CHANGED() {
            super(MessageType.OWNER_CHANGED);
        }

        OWNER_CHANGED(int requestMessageId, short sender, long lineId, short newOwner, MessageType originalType) {
            super(MessageType.OWNER_CHANGED);
            this.messageSequenceNumber = requestMessageId;
            this.sender = sender;
            this.lineId = lineId;
            this.newOwner = newOwner;
            this.originalMsgType = originalType;
        }

        @Override
        public int calcMessagesByteSize() {
            return super.calcMessagesByteSize()
                    + 2       // new owner short
                    + 1       // original message type byte
                    ;
        }

        @Override
        public void write(MessageOutput out) throws IOException {
            super.write(out);
            out.writeShort(newOwner);
            out.writeByte(originalMsgType.ordinal);
        }

        @Override
        public void read(MessageInput in) throws IOException {
            super.read(in);
            newOwner = in.readShort();
            originalMsgType = MessageType.fromByte(in.readByte());
        }

        @Override
        public boolean equals(Object obj) {
            if (OWNER_CHANGED.class.isInstance(obj)) {
                OWNER_CHANGED that = (OWNER_CHANGED)obj;
                return super.equals(that)
                        && this.newOwner == that.newOwner
                        && this.originalMsgType.ordinal == that.originalMsgType.ordinal;
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Hashing.goodFastHash(32)
                    .hashObject(this, ownerChangedFunnel)
                    .asInt();
        }

        private static final Funnel<OWNER_CHANGED> ownerChangedFunnel = (Funnel<OWNER_CHANGED>) (msg, into) -> into
                .putByte(msg.type.ordinal)
                .putShort(msg.sender)
                .putInt(msg.messageSequenceNumber)
                .putLong(msg.lineId)
                .putShort(msg.newOwner)
                .putByte(msg.originalMsgType.ordinal)
                ;
    }

    static class INV extends Request {
        private INV() {
            super(MessageType.INV);
        }

        INV(short sender, long lineId) {
            super(MessageType.INV);
            this.sender = sender;
            this.lineId = lineId;
        }

        @Override
        public boolean equals(Object obj) {
            if (INV.class.isInstance(obj)) {
                INV that = (INV)obj;
                return super.equals(that);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        INV copy() {
            return new INV(sender, lineId);
        }
    }

    static class INVACK extends Response {
        private INVACK() {
            super(MessageType.INVACK);
        }

        INVACK(int requestMessageId, short sender, long lineId) {
            super(MessageType.INVACK);
            this.messageSequenceNumber = requestMessageId;
            this.sender = sender;
            this.lineId = lineId;
        }

        @Override
        public boolean equals(Object obj) {
            if (INVACK.class.isInstance(obj)) {
                INVACK that = (INVACK)obj;
                return super.equals(that);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }

    static class BACKUP extends Request {
        // this is an ever incrementing long (possibly even the system timestamp)
        // this is used to establish a "global state" among all replicas of a leader
        // replicas will go and compete for leadership with the latest message number they received
        // the replica with the highest message number wins and will become new leader
        long leaderEpoch;
        // after a failover the new leader might need to replay a bunch of messages to the rest of the replicas
        // for that replicas keep a short history of all changes
        // the leader however provides replicas with the latest state that all replicas know about
        // everything older than this is safe to discard as all replicas know about this state already
        long lastAckedLeaderEpoch;
        int version;
        ByteBuf buffer;

        private BACKUP() {
            super(MessageType.BACKUP);
        }

        BACKUP(short sender, long lineId, long leaderEpoch, long lastAckedLeaderEpoch, int version, ByteBuf buffer) {
            super(MessageType.BACKUP);
            this.sender = sender;
            this.lineId = lineId;
            this.leaderEpoch = leaderEpoch;
            this.lastAckedLeaderEpoch = lastAckedLeaderEpoch;
            this.version = version;
            this.buffer = buffer;
        }

        @Override
        public int calcMessagesByteSize() {
            return super.calcMessagesByteSize()
                    + 8                                          // running message number long
                    + 8                                          // last acked message number long
                    + 4                                          // version number int
                    + 4                                          // bytebuf size
                    + ((buffer == null) ? 0 : buffer.capacity()) // buffer content
                    ;
        }

        @Override
        public void write(MessageOutput out) throws IOException {
            super.write(out);
            out.writeLong(leaderEpoch);
            out.writeLong(lastAckedLeaderEpoch);
            out.writeInt(version);
            writeByteBuf(out, buffer);
        }

        @Override
        public void read(MessageInput in) throws IOException {
            super.read(in);
            leaderEpoch = in.readLong();
            lastAckedLeaderEpoch = in.readLong();
            version = in.readInt();
            int bytesToRead = in.readInt();
            buffer = (bytesToRead == 0) ? null : in.readByteBuf(bytesToRead);
        }

        @Override
        public boolean equals(Object obj) {
            if (BACKUP.class.isInstance(obj)) {
                BACKUP that = (BACKUP)obj;
                return super.equals(that)
                        && this.leaderEpoch == that.leaderEpoch
                        && this.lastAckedLeaderEpoch == that.lastAckedLeaderEpoch
                        && this.version == that.version
                        && this.buffer.capacity() == that.buffer.capacity();
            } else {
                return false;
            }
        }

        @Override
        BACKUP copy() {
            // beware of the reference counting
            // every time a message is copied, you need to increment the ref count
            // because sending a buffer decreases it by one
            return new BACKUP(sender, lineId, leaderEpoch, lastAckedLeaderEpoch, version, buffer.resetReaderIndex().retain());
        }

        @Override
        public int hashCode() {
            return Hashing.goodFastHash(32)
                    .hashObject(this, backupFunnel)
                    .asInt();
        }

        private static final Funnel<BACKUP> backupFunnel = (Funnel<BACKUP>) (msg, into) -> into
                .putByte(msg.type.ordinal)
                .putShort(msg.sender)
                .putInt(msg.messageSequenceNumber)
                .putLong(msg.lineId)
                .putLong(msg.leaderEpoch)
                .putLong(msg.lastAckedLeaderEpoch)
                .putInt(msg.version)
                .putInt((msg.buffer == null) ? 0 : msg.buffer.capacity())
                ;
    }

    static class BACKUP_ACK extends Response {
        long ackedLeaderEpoch;

        private BACKUP_ACK() {
            super(MessageType.BACKUP_ACK);
        }

        BACKUP_ACK(int requestMessageId, short sender, long lineId, long ackedLeaderEpoch) {
            super(MessageType.BACKUP_ACK);
            this.messageSequenceNumber = requestMessageId;
            this.sender = sender;
            this.lineId = lineId;
            this.ackedLeaderEpoch = ackedLeaderEpoch;
        }

        @Override
        public int calcMessagesByteSize() {
            return super.calcMessagesByteSize()
                    + 8 // running message number long
                    ;
        }

        @Override
        public void write(MessageOutput out) throws IOException {
            super.write(out);
            out.writeLong(ackedLeaderEpoch);
        }

        @Override
        public void read(MessageInput in) throws IOException {
            super.read(in);
            ackedLeaderEpoch = in.readLong();
        }

        @Override
        public boolean equals(Object obj) {
            if (BACKUP_ACK.class.isInstance(obj)) {
                BACKUP_ACK that = (BACKUP_ACK)obj;
                return super.equals(that)
                        && this.ackedLeaderEpoch == that.ackedLeaderEpoch;
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Hashing.goodFastHash(32)
                    .hashObject(this, backupAckFunnel)
                    .asInt();
        }

        private static final Funnel<BACKUP_ACK> backupAckFunnel = (Funnel<BACKUP_ACK>) (msg, into) -> into
                .putByte(msg.type.ordinal)
                .putShort(msg.sender)
                .putInt(msg.messageSequenceNumber)
                .putLong(msg.lineId)
                .putLong(msg.ackedLeaderEpoch)
                ;
    }

    static class REMOVE_BACKUP extends Request {
        private REMOVE_BACKUP() {
            super(MessageType.REMOVE_BACKUP);
        }

        REMOVE_BACKUP(short sender, long lineId) {
            super(MessageType.REMOVE_BACKUP);
            this.sender = sender;
            this.lineId = lineId;
        }

        @Override
        REMOVE_BACKUP copy() {
            return new REMOVE_BACKUP(sender, lineId);
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (REMOVE_BACKUP.class.isInstance(obj)) {
                REMOVE_BACKUP that = (REMOVE_BACKUP)obj;
                return super.equals(that);
            } else {
                return false;
            }
        }
    }

    static class CHANGE_OWNER extends Request {
        int version;
        ByteBuf data;

        private CHANGE_OWNER() {
            super(MessageType.CHANGE_OWNER);
        }

        CHANGE_OWNER(short sender, long lineId, int version, ByteBuf data) {
            super(MessageType.CHANGE_OWNER);
            this.sender = sender;
            this.lineId = lineId;
            this.version = version;
            this.data = data;
        }

        @Override
        public int calcMessagesByteSize() {
            return super.calcMessagesByteSize()
                    + 4                                          // version number int
                    + 4                                          // bytebuf size
                    + ((data == null) ? 0 : data.capacity())     // buffer content
                    ;
        }

        @Override
        public void write(MessageOutput out) throws IOException {
            super.write(out);
            out.writeInt(version);
            writeByteBuf(out, data);
        }

        @Override
        public void read(MessageInput in) throws IOException {
            super.read(in);
            version = in.readInt();
            int bytesToRead = in.readInt();
            data = (bytesToRead == 0) ? null : in.readByteBuf(bytesToRead);
        }

        @Override
        public boolean equals(Object obj) {
            if (CHANGE_OWNER.class.isInstance(obj)) {
                CHANGE_OWNER that = (CHANGE_OWNER)obj;
                return super.equals(that)
                        && this.version == that.version
                        && equalsByteBuf(this.data, that.data);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Hashing.goodFastHash(32)
                    .hashObject(this, changeOwnerFunnel)
                    .asInt();
        }

        private static final Funnel<CHANGE_OWNER> changeOwnerFunnel = (Funnel<CHANGE_OWNER>) (msg, into) -> into
                .putByte(msg.type.ordinal)
                .putShort(msg.sender)
                .putLong(msg.lineId)
                .putInt(msg.version)
                .putInt((msg.data == null) ? 0 : msg.data.capacity())
                ;
    }

    static class DeserializationMessageFactory {
        Message createMessageShellForType(MessageType type) {
            switch (type) {
                case ACK:
                    return new ACK();
                case PUT:
                    return new PUT();
                case PUTX:
                    return new PUTX();
                case GET:
                    return new GET();
                case GETX:
                    return new GETX();
                case OWNER_CHANGED:
                    return new OWNER_CHANGED();
                case INV:
                    return new INV();
                case INVACK:
                    return new INVACK();
                case BACKUP:
                    return new BACKUP();
                case BACKUP_ACK:
                    return new BACKUP_ACK();
                case REMOVE_BACKUP:
                    return new REMOVE_BACKUP();
                case CHANGE_OWNER:
                    return new CHANGE_OWNER();
                default:
                    throw new IllegalArgumentException("Unknown type " + type);
            }
        }
    }
}
