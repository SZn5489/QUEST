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
package codec;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.trevni.TrevniRuntimeException;

import codec.metadata.MetaData;

/**
 * Interface for compression codecs.
 */
public abstract class Codec {

    /**
     * 获取编解码方式
     * @param meta 需要获取编解码方式的元数据
     * @return 返回编解码的方式 null || deflate || snappy || bzip2(不支持)
     */
    public static Codec get(MetaData meta) {
        String name = meta.getCodec();
        if (name == null || "null".equals(name))
            return new NullCodec();
        else if ("deflate".equals(name))
            return new DeflateCodec();
        else if ("snappy".equals(name))
            return new SnappyCodec();
        else if ("bzip2".equals(name))
            throw new TrevniRuntimeException("Bzip2 not supported: " + name);
        //return new BZip2Codec();
        else
            throw new TrevniRuntimeException("Unknown codec: " + name);
    }

    /**
     * Compress data
     */
    public abstract ByteBuffer compress(ByteBuffer uncompressedData) throws IOException;

    /**
     * Decompress data
     */
    public abstract ByteBuffer decompress(ByteBuffer compressedData) throws IOException;

}
