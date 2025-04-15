package com.nio;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

@Slf4j
public final class TcpDataFlowExample {

    public static final String HOSTNAME = "0.0.0.0";
    public static final int[] PORTS = new int[]{5555, 4444};

    private TcpDataFlowExample() {
    }

    public  void start() throws Exception {
        log.info("Tcp Data Flow Example started at {}:{}", HOSTNAME, Arrays.toString(PORTS));

        final Selector selector = Selector.open();

        for (final int port : PORTS) {
            try {
                final ServerSocketChannel serverSocket = ServerSocketChannel.open();
                serverSocket.bind(new InetSocketAddress(HOSTNAME, port));
                serverSocket.configureBlocking(false);
                serverSocket.register(selector, SelectionKey.OP_ACCEPT);
            } catch (IOException e) {
                log.error("Failed to bind to port {}: {}", port, e.getMessage(), e);
            }
        }
        final Multimap<Integer, SocketChannel> portToSocketChannels  = ArrayListMultimap.create();

        final Map<Integer, TcpHandlerStrategy> strategyMap = Map.of(
                PORTS[0], new UppercaseEchoHandler(),
                PORTS[1], new CommandControlHandler(selector, portToSocketChannels )
        );
        while (!Thread.currentThread().isInterrupted()) {
            log.info("Wait new events..");
            selector.select();

            final Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
            while (iterator.hasNext()) {
                final SelectionKey selectionKey = iterator.next();
                iterator.remove();
                try {
                    if (selectionKey.isAcceptable()) {
                        handleAccept(selectionKey, portToSocketChannels, selector);
                    }

                    if (selectionKey.isReadable()) {
                        if (handleRead(selectionKey, portToSocketChannels, strategyMap)) continue;
                    }

                } catch (Exception e) {
                    log.error("An error occurred while processing the selection key: {}", e.getMessage(), e);
                    if (selectionKey.channel() instanceof SocketChannel) {
                        closeClientConnection((SocketChannel) selectionKey.channel(), selectionKey, portToSocketChannels );
                    }
                }
            };
        }
    }

    private boolean handleRead(SelectionKey selectionKey,
                               Multimap<Integer, SocketChannel> portToSocketChannels,
                               Map<Integer, TcpHandlerStrategy> strategyMap) throws IOException {
        final SocketChannel client = (SocketChannel) selectionKey.channel();
        final ByteBuffer buffer = allocateBuffer(client);
        final int read = client.read(buffer);

        if (read == -1) {
            closeClientConnection(client, selectionKey, portToSocketChannels);
            return true;
        }

        buffer.flip();
        int port = client.socket().getLocalPort();
        TcpHandlerStrategy handler = strategyMap.get(port);

        if (handler != null) {
            handler.handle(client, buffer);
        } else {
            log.warn("No handler for port: {}", port);

        }
        buffer.clear();
        return false;
    }

    private  void handleAccept(SelectionKey selectionKey,
                               Multimap<Integer, SocketChannel> portToSocketChannels,
                               Selector selector) throws IOException {
        final ServerSocketChannel server = (ServerSocketChannel) selectionKey.channel();
        final SocketChannel client = server.accept();
        client.configureBlocking(false);
        log.info("New connection accepted: {}", client);

        int port = server.socket().getLocalPort();
        portToSocketChannels.put(port, client);

        client.register(selector, SelectionKey.OP_READ);
    }

    private  ByteBuffer allocateBuffer(SocketChannel client) throws IOException {

        int bufferSize = 1024;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        int bytesRead = client.read(buffer);

        while (bytesRead > buffer.remaining()) {
            bufferSize = buffer.capacity() * 2;
            ByteBuffer newBuffer = ByteBuffer.allocate(bufferSize);

            buffer.flip();
            newBuffer.put(buffer);

            buffer = newBuffer;

            bytesRead = client.read(buffer);
        }

        return buffer;
    }

    private static void closeClientConnection(SocketChannel client, SelectionKey selectionKey, Multimap<Integer, SocketChannel> clients) {
        try {
            if (client != null) {
                client.close();
                log.info("Closed client connection: {}", client);
                int port = client.socket().getLocalPort();
                clients.get(port).remove(client);
            }
        } catch (IOException e) {
            log.error("Failed to close client channel: {}", client, e);
        } finally {
            if (selectionKey.isValid()) {
                selectionKey.cancel();
            }
        }
    }

    public static void main(final String... args) throws Exception {
        new TcpDataFlowExample().start();
    }

}
