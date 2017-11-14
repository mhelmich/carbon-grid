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

package org.carbon.grid;

import io.netty.buffer.ByteBuf;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class MessageInput extends InputStream implements DataInput {
    private final ByteBuf buffer;

    public MessageInput(ByteBuf buffer) {
        this.buffer = buffer;
    }

    @Override
    public int read() throws IOException {
        return !this.buffer.isReadable() ? -1 : this.buffer.readByte() & 255;
    }

    public void close() throws IOException {
        super.close();
    }

    public ByteBuf readByteBuf() throws IOException {
        return buffer.readRetainedSlice(available());
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        this.readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        this.checkAvailable(len);
        this.buffer.readBytes(b, off, len);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        int nBytes = Math.min(this.available(), n);
        this.buffer.skipBytes(nBytes);
        return nBytes;
    }

    @Override
    public boolean readBoolean() throws IOException {
        this.checkAvailable(1);
        return this.read() != 0;
    }

    @Override
    public byte readByte() throws IOException {
        if (!this.buffer.isReadable()) {
            throw new EOFException();
        } else {
            return this.buffer.readByte();
        }
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return this.readByte() & 255;
    }

    @Override
    public short readShort() throws IOException {
        this.checkAvailable(2);
        return this.buffer.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return this.readShort() & '\uffff';
    }

    @Override
    public char readChar() throws IOException {
        return (char)this.readShort();
    }

    @Override
    public int readInt() throws IOException {
        this.checkAvailable(4);
        return this.buffer.readInt();
    }

    @Override
    public long readLong() throws IOException {
        this.checkAvailable(8);
        return this.buffer.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(this.readInt());
    }

    @Override
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(this.readLong());
    }

    @Override
    public String readLine() throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public String readUTF() throws IOException {
        return DataInputStream.readUTF(this);
    }

    @Override
    public int available() throws IOException {
        return buffer.readableBytes();
    }

    private void checkAvailable(int fieldSize) throws IOException {
        if (fieldSize < 0) {
            throw new IndexOutOfBoundsException("fieldSize cannot be a negative number");
        } else if (fieldSize > this.available()) {
            throw new EOFException("fieldSize is too long! Length is " + fieldSize + ", but maximum is " + this.available());
        }
    }
}
