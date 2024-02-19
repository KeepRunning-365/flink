/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.myself;

import org.apache.flink.streaming.examples.socket.SocketWindowWordCount;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.fury.Fury;
import io.fury.ThreadSafeFury;
import io.fury.config.CompatibleMode;
import io.fury.config.Language;

import java.io.Serializable;

/**
 * 自定义序列化器.
 */
public class FurySerializer extends Serializer<SocketWindowWordCount.WordWithCount> implements Serializable {

    // 建议作为一个全局变量，避免重复创建
    final Fury fury = Fury.builder()
            .withLanguage(Language.JAVA)
            //开启共享引用/循环引用支持，不需要的话建议关闭，性能更快
            .withRefTracking(true)
            // 允许序列化未注册类型
            // .withClassRegistrationRequired(false)
            // 开启int/long压缩，减少序列化数据大小，无该类需求建议关闭，性能更好
            // .withNumberCompressed(true)
            .withCompatibleMode(CompatibleMode.SCHEMA_CONSISTENT)
            // 开启类型前后兼容，允许序列化和反序列化字段不一致，无该类需求建议关闭，性能更好
            // .withCompatibleMode(CompatibleMode.COMPATIBLE)
            // 开启异步多线程编译
            .withAsyncCompilation(true)
            .requireClassRegistration(false)
            .build();

    ThreadSafeFury threadSafeFury = Fury.builder()
            .withLanguage(Language.JAVA)
            //开启共享引用/循环引用支持，不需要的话建议关闭，性能更快
            .withRefTracking(true)
            // 允许序列化未注册类型
            // .withClassRegistrationRequired(false)
            // 开启int/long压缩，减少序列化数据大小，无该类需求建议关闭，性能更好
            // .withNumberCompressed(true)
            .withCompatibleMode(CompatibleMode.SCHEMA_CONSISTENT)
            // 开启类型前后兼容，允许序列化和反序列化字段不一致，无该类需求建议关闭，性能更好
            // .withCompatibleMode(CompatibleMode.COMPATIBLE)
            // 开启异步多线程编译
            .withAsyncCompilation(true)
            .buildThreadSafeFury();

    @Override
    public void write(Kryo kryo, Output output, SocketWindowWordCount.WordWithCount object) {
        byte[] bytes = fury.serialize(object);
        System.out.println("自定义序列化 - 写："+bytes.length);
        output.writeInt(bytes.length);
        output.writeBytes(bytes);
    }

    @Override
    public SocketWindowWordCount.WordWithCount read(
            Kryo kryo,
            Input input,
            Class<SocketWindowWordCount.WordWithCount> type) {
        int read = input.read();
        System.out.println("自定义序列化 - 读："+read );
        byte[] barr = new byte[read];
        input.readBytes(barr);
        return (SocketWindowWordCount.WordWithCount) fury.deserialize(barr);
    }
}
