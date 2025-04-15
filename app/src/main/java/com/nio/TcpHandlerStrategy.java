package com.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import com.google.common.collect.Multimap;
/*
Instead of using the Strategy pattern, a functional approach can be used if the app is not expected to grow.
* private static final Map<Integer, BiConsumer<SocketChannel, ByteBuffer>> handlerMap = Map.of(
*    5555, (client, buffer) -> handleUppercaseEcho(client, buffer),
*    4444, (client, buffer) -> handleCommandControl(client, buffer)
* );
* However, the Strategy pattern follows the Open/Closed and Single Responsibility Principles.
*/
public interface TcpHandlerStrategy {
    void handle(SocketChannel client, ByteBuffer buffer) throws IOException;
}