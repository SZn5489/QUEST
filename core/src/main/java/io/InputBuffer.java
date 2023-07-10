/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import org.apache.trevni.Input;
import org.apache.trevni.TrevniRuntimeException;

import codec.metadata.misc.InputBytes;
import codec.metadata.misc.ValueType;

/**
 * Used to read values.
 */
public class InputBuffer {
    private Input in;

    private final long inLength;
    private long offset; // pos of next read from in 下一个pos

    protected byte[] buf; // data from input //存储数据
    protected int pos; // position within buffer 当前位置
    protected int limit; // end of valid buffer data //最后的有效的数据位置

    private static final CharsetDecoder UTF8 = Charset.forName("UTF-8").newDecoder();

    protected int bitCount; // position in booleans  在bitset中的位置

    protected int runLength; // length of run
    protected int runValue; // value of run

    public InputBuffer(Input in) throws IOException {
        this(in, 0);
    }

    public InputBuffer(Input in, long position) throws IOException {
        this.in = in;
        this.inLength = in.length();
        this.offset = position;

        if (in instanceof InputBytes) { // use buffer directly
            this.buf = ((InputBytes) in).getBuffer();
            this.limit = (int) in.length();
            this.offset = limit;
            this.pos = (int) position;
        } else { // create new buffer
            this.buf = new byte[8192]; // big enough for primitives
        }
    }

    /**
     * 下一次读取数据从全局index为position的位置开始
     * @param position 一个全局index
     * @throws IOException ...
     */
    public void seek(long position) throws IOException {
        runLength = 0;
        //如果这个全局index在buffer中，设置当前pos指向position指代的buffer的位置
        if (position >= (offset - limit) && position <= offset) {
            pos = (int) (limit - (offset - position)); // seek in buffer;
            return;
        }
        //否则设置buffer为空，并将下一次读取位置设置为position
        pos = 0;
        limit = 0;
        offset = position;
    }


    /**
     * offset是下一次读取起始位置的全局index
     * limit是当前缓冲区中的字节数
     * pos是当前缓冲区下一个读取的字节数
     * @description tell()方法获得当前位置的全局index
     * @return 返回当前位置的全局index
     */
    public long tell() {
        return (offset - limit) + pos;
    }

    public long length() {
        return inLength;
    }

    /**
     * 按照type确定读取缓冲区的方式
     * @param type 按照什么类型读数据
     * @return 返回读出的数据
     * @param <T> extends Comparable
     * @throws IOException ...
     */
    public <T extends Comparable> T readValue(ValueType type) throws IOException {
        switch (type) {
            case NULL:
                return (T) null;
            case BOOLEAN:
                return (T) Boolean.valueOf(readBoolean());
            case INT:
                return (T) Integer.valueOf(readFixed32());
            case LONG:
                return (T) Long.valueOf(readFixed64());
            case FIXED32:
                return (T) Integer.valueOf(readFixed32());
            case FIXED64:
                return (T) Long.valueOf(readFixed64());
            case FLOAT:
                return (T) Float.valueOf(readFloat());
            case DOUBLE:
                return (T) Double.valueOf(readDouble());
            case STRING:
                return (T) readString();
            case BYTES:
                return (T) readBytes(null);
            default:
                throw new TrevniRuntimeException("Unknown value type: " + type);
        }
    }

    /**
     * 根据type类型跳过不同的字节数
     * @param type 输入的type类型
     * @throws IOException ...
     */
    public void skipValue(ValueType type) throws IOException {
        switch (type) {
            case NULL:
                break;
            case BOOLEAN:
                readBoolean();
                break;
            case INT:
                skip(4);
                break;
            case LONG:
                skip(8);
                break;
            case FIXED32:
            case FLOAT:
                skip(4);
                break;
            case FIXED64:
            case DOUBLE:
                skip(8);
                break;
            case STRING:
            case BYTES:
                skipBytes();
                break;
            default:
                throw new TrevniRuntimeException("Unknown value type: " + type);
        }
    }

    /**
     * 读取当前pos下第bitCount位(从右往左)的boolean值 0 -> false 1-> true
     * @return 读出的boolean类型数据
     * @throws IOException ...
     */
    public boolean readBoolean() throws IOException {
        if (bitCount == 0)
            read();
        int bits = buf[pos - 1] & 0xff;
        int bit = (bits >> bitCount) & 1;
        bitCount++;
        if (bitCount == 8)
            bitCount = 0;
        return bit == 0 ? false : true;
    }

    /**
     *
     * @return
     * @throws IOException
     */
    public int readLength() throws IOException {
        bitCount = 0;
        if (runLength > 0) {
            runLength--; // in run
            return runValue;
        }

        int length = readFixed32();
        if (length >= 0) // not a run
            return length;

        runLength = (1 - length) >>> 1; // start of run
        runValue = (length + 1) & 1;
        return runValue;
    }

    /**
     * 读取buffer中的下一个整数
     * @return 返回读取的整数
     * @throws IOException ...
     */
    public int readInt() throws IOException {
        if ((limit - pos) < 5) { // maybe not in buffer
            int b = read();
            int n = b & 0x7f;
            for (int shift = 7; b > 0x7f; shift += 7) {
                b = read();
                n ^= (b & 0x7f) << shift;
            }
            return (n >>> 1) ^ -(n & 1); // back to two's-complement
        }
        int len = 1;
        int b = buf[pos] & 0xff;
        int n = b & 0x7f;
        if (b > 0x7f) {
            b = buf[pos + len++] & 0xff;
            n ^= (b & 0x7f) << 7;
            if (b > 0x7f) {
                b = buf[pos + len++] & 0xff;
                n ^= (b & 0x7f) << 14;
                if (b > 0x7f) {
                    b = buf[pos + len++] & 0xff;
                    n ^= (b & 0x7f) << 21;
                    if (b > 0x7f) {
                        b = buf[pos + len++] & 0xff;
                        n ^= (b & 0x7f) << 28;
                        if (b > 0x7f) {
                            throw new IOException("Invalid int encoding");
                        }
                    }
                }
            }
        }
        pos += len;
        if (pos > limit)
            throw new EOFException();
        return (n >>> 1) ^ -(n & 1); // back to two's-complement
    }

    public long readLong() throws IOException {
        if ((limit - pos) < 10) { // maybe not in buffer
            int b = read();
            long n = b & 0x7f;
            for (int shift = 7; b > 0x7f; shift += 7) {
                b = read();
                n ^= (b & 0x7fL) << shift;
            }
            return (n >>> 1) ^ -(n & 1); // back to two's-complement
        }

        int b = buf[pos++] & 0xff;
        int n = b & 0x7f;
        long l;
        if (b > 0x7f) {
            b = buf[pos++] & 0xff;
            n ^= (b & 0x7f) << 7;
            if (b > 0x7f) {
                b = buf[pos++] & 0xff;
                n ^= (b & 0x7f) << 14;
                if (b > 0x7f) {
                    b = buf[pos++] & 0xff;
                    n ^= (b & 0x7f) << 21;
                    if (b > 0x7f) {
                        // only the low 28 bits can be set, so this won't carry
                        // the sign bit to the long
                        l = innerLongDecode((long) n);
                    } else {
                        l = n;
                    }
                } else {
                    l = n;
                }
            } else {
                l = n;
            }
        } else {
            l = n;
        }
        if (pos > limit) {
            throw new EOFException();
        }
        return (l >>> 1) ^ -(l & 1); // back to two's-complement
    }

    // splitting readLong up makes it faster because of the JVM does more
    // optimizations on small methods
    private long innerLongDecode(long l) throws IOException {
        int len = 1;
        int b = buf[pos] & 0xff;
        l ^= (b & 0x7fL) << 28;
        if (b > 0x7f) {
            b = buf[pos + len++] & 0xff;
            l ^= (b & 0x7fL) << 35;
            if (b > 0x7f) {
                b = buf[pos + len++] & 0xff;
                l ^= (b & 0x7fL) << 42;
                if (b > 0x7f) {
                    b = buf[pos + len++] & 0xff;
                    l ^= (b & 0x7fL) << 49;
                    if (b > 0x7f) {
                        b = buf[pos + len++] & 0xff;
                        l ^= (b & 0x7fL) << 56;
                        if (b > 0x7f) {
                            b = buf[pos + len++] & 0xff;
                            l ^= (b & 0x7fL) << 63;
                            if (b > 0x7f) {
                                throw new IOException("Invalid long encoding");
                            }
                        }
                    }
                }
            }
        }
        pos += len;
        return l;
    }

    /**
     * 读取一个4位float型数
     * 按照固定32位读取并转为float类型
     * @return 返回读取的float类型数据
     * @throws IOException ...
     */
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readFixed32());
    }

    /**
     * 读取缓存区中固定4个字节即32位数据
     * 并修改pos为读取后的下一次读取的第一个位置
     * @return 读取的四个字节对应的整数
     * @throws IOException ...
     */
    public int readFixed32() throws IOException {
        if ((limit - pos) < 4) // maybe not in buffer
            return read() | (read() << 8) | (read() << 16) | (read() << 24);

        int len = 1;
        int n = (buf[pos] & 0xff) | ((buf[pos + len++] & 0xff) << 8) | ((buf[pos + len++] & 0xff) << 16)
                | ((buf[pos + len++] & 0xff) << 24);
        if ((pos + 4) > limit)
            throw new EOFException();
        pos += 4;
        return n;
    }

    /**
     * 从缓存区中读取一个double类型的数据
     * 按照long型读取并转为double类型
     * @return 返回读取的double类型的数据
     * @throws IOException ...
     */
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readFixed64());
    }

    /**
     * 读取固定的缓存区中的8个字节即64位
     * 读取两次32位数并拼接成64位返回
     * @return 返回读取得到的64位long型
     * @throws IOException ...
     */
    public long readFixed64() throws IOException {
        return (readFixed32() & 0xFFFFFFFFL) | (((long) readFixed32()) << 32);
    }

    /**
     * 读取缓存区的一个String类型数据
     * 首先读取缓冲区的一个整数类型，表示字符串长度，小于limit - pos说明字符串已经读入buf中，直接读取返回
     * 如果string没有全部在buf中，则调用readFully方法
     * @return 返回最后读取到的string
     * @throws IOException ...
     */
    public String readString() throws IOException {
        int length = readInt();
        if (length <= (limit - pos)) { // in buffer
            String result = UTF8.decode(ByteBuffer.wrap(buf, pos, length)).toString();
            pos += length;
            return result;
        }
        byte[] bytes = new byte[length];
        readFully(bytes, 0, length);
        return UTF8.decode(ByteBuffer.wrap(bytes, 0, length)).toString();
    }

    /**
     * ?????
     * 先读取一个整数开辟result空间，然后按照readString的方法填满result
     * @return 返回读取的byte[] result
     * @throws IOException ...
     */
    public byte[] readBytes() throws IOException {
        byte[] result = new byte[readInt()];
        readFully(result);
        return result;
    }

    /**
     * 读取一串字节存入到byteBuffer中
     * @param old 已经存在的buffer，如果长度够用则可以少开辟一段内存空间
     * @return 读取数据后的新的buffer
     * @throws IOException ...
     */
    public ByteBuffer readBytes(ByteBuffer old) throws IOException {
        int length = readInt();
        ByteBuffer result;
        //allocate 至少为 length长度的buffer
        if (old != null && length <= old.capacity()) {
            result = old;
            result.clear();
        } else {
            result = ByteBuffer.allocate(length);
        }
        readFully(result.array(), result.position(), length);
        result.limit(length);
        return result;
    }

    /**
     * 跳过下一个字段数据
     * @throws IOException ...
     */
    public void skipBytes() throws IOException {
        skip(readInt());
    }

    /**
     * 跳过length个字节长度的数据
     * 调用tell方法获取当前字节的全局index
     * 调用seek方法将读取的pos跳转到下一个应该读取的index上
     * @param length 要跳过的字节数
     * @throws IOException ...
     */
    public void skip(long length) throws IOException {
        seek(tell() + length);
    }

    /**
     * 读取buf中的当前字节(pos)的一个字节的数据
     * 如果缓冲区已经读完，则调用readInput方法装填缓冲区再进行数据读取
     * @return 返回读取的数据
     * @throws IOException ...
     */
    public int read() throws IOException {
        if (pos >= limit) {
            limit = readInput(buf, 0, buf.length);
            pos = 0;
        }
        return buf[pos++] & 0xFF;
    }

    public void readFully(byte[] bytes) throws IOException {
        readFully(bytes, 0, bytes.length);
    }

    /**
     * 读满bytes数组
     * @param bytes 需要装满的数组
     * @param start 开始装的第一个index
     * @param len 需要装的最后一个index
     * @throws IOException ...
     */
    public void readFully(byte[] bytes, int start, int len) throws IOException {
        int buffered = limit - pos; //buffered中还有多少字节
        //先把buffer中的内容都读到bytes中
        if (len > buffered) { // buffer is insufficient

            System.arraycopy(buf, pos, bytes, start, buffered); // consume buffer
            start += buffered;
            len -= buffered;
            pos += buffered;
            if (len > buf.length) { // bigger than buffer
                do {
                    int read = readInput(bytes, start, len); // read directly into result
                    len -= read;
                    start += read;
                } while (len > 0);
                return;
            }

            limit = readInput(buf, 0, buf.length); // refill buffer
            pos = 0;
        }

        System.arraycopy(buf, pos, bytes, start, len); // copy from buffer
        pos += len;
    }

    /**
     * 装填buffer缓冲区
     * @param b 缓冲区数组
     * @param start 开始装填的位置
     * @param len 装填的长度
     * @return 读取数据的字节数
     * @throws IOException ...
     */
    private int readInput(byte[] b, int start, int len) throws IOException {
        int read = in.read(offset, b, start, len);
        if (read < 0)
            throw new EOFException();
        offset += read;//更新offset，下一次加载数据时装填的起始位置
        return read;
    }

}
