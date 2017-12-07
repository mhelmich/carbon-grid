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

import io.netty.buffer.ByteBuf;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

public class MessageOutput extends OutputStream implements DataOutput {
    private final ByteBuf buffer;

    public MessageOutput(ByteBuf buffer) {
        this.buffer = buffer;
    }

    public void writeByteBuf(ByteBuf bites) {
        buffer.writeBytes(bites);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        this.write(v ? 1 : 0);
    }

    @Override
    public void writeByte(int v) throws IOException {
        this.write(v);
    }

    @Override
    public void writeShort(int v) throws IOException {
        this.buffer.writeShort((short)v);
    }

    @Override
    public void writeChar(int v) throws IOException {
        this.writeShort((short)v);
    }

    @Override
    public void writeInt(int v) throws IOException {
        this.buffer.writeInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException {
        this.buffer.writeLong(v);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        this.writeInt(Float.floatToIntBits(v));
    }

    @Override
    public void writeDouble(double v) throws IOException {
        this.writeLong(Double.doubleToLongBits(v));
    }

    @Override
    public void writeBytes(String s) throws IOException {
        int len = s.length();
        for(int i = 0; i < len; ++i) {
            this.write((byte)s.charAt(i));
        }
    }

    @Override
    public void writeChars(String s) throws IOException {
        int len = s.length();
        for(int i = 0; i < len; ++i) {
            this.writeChar(s.charAt(i));
        }
    }

    @Override
    public void writeUTF(String s) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void write(int b) throws IOException {
        this.buffer.writeByte((byte)b);
    }
}
