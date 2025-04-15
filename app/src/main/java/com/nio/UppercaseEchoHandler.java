package com.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

import com.google.common.collect.Multimap;

public class UppercaseEchoHandler implements TcpHandlerStrategy {

    @Override
    public void handle(SocketChannel client, ByteBuffer buffer) throws IOException {
        String input = StandardCharsets.UTF_8.decode(buffer).toString();
        String upper = input.toUpperCase();
        byte[] upperBytes = upper.getBytes(StandardCharsets.UTF_8);
        buffer.clear();
        buffer.put(upperBytes);
        buffer.flip();
        client.write(buffer);
        /* Previous approach  works for ASCII characters not multibyte characters, for example 'Café'
        	for (int i = 0; i < buffer.limit(); i++) {
								buffer.put(i, (byte) Character.toUpperCase(buffer.get(i)));
							}*/
    }
}

