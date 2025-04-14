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

    private static final Map<Integer, TcpHandlerStrategy> strategyMap = Map.of(
            5555, new UppercaseEchoHandler(),
            4444, new CommandControlHandler()
    );

    private TcpDataFlowExample() {
    }

    public static void main(final String... args) throws Exception {
        log.info("Tcp Data Flow Example started at {}:{}", HOSTNAME, Arrays.toString(PORTS));

        final Selector selector = Selector.open();

        for (final int port : PORTS) {
            try {
                final ServerSocketChannel serverSocket = ServerSocketChannel.open();
                serverSocket.bind(new InetSocketAddress(HOSTNAME, port));
                serverSocket.configureBlocking(false);
                serverSocket.register(selector, SelectionKey.OP_ACCEPT);
            }
            catch (IOException e) {
                log.error("Failed to bind to port {}: {}", port, e.getMessage(), e);
            }
        }

        final Multimap<Integer, SocketChannel> clients = ArrayListMultimap.create();

        while (!Thread.currentThread().isInterrupted()) {
            log.info("Wait new events..");
            selector.select();

            final Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
            iterator.forEachRemaining(selectionKey -> {
                try {
                    if (selectionKey.isAcceptable()) {
                        final ServerSocketChannel server = (ServerSocketChannel) selectionKey.channel();
                        final SocketChannel client = server.accept();
                        client.configureBlocking(false);
                        log.info("New connection accepted: {}", client);

                        int port = server.socket().getLocalPort();
                        clients.put(port, client);

                        client.register(selector, SelectionKey.OP_READ);
                    }

                    if (selectionKey.isReadable()) {
                        final SocketChannel client = (SocketChannel) selectionKey.channel();
                        final ByteBuffer buffer = allocateBuffer(client);
                        final int read = client.read(buffer);

                        if (read == -1) {
                            client.close();
                            int port = client.socket().getLocalPort();
                            clients.get(port).remove(client);
                            selectionKey.cancel();
                            log.info("The connection was closed: {}", client);
                            return;
                        }

                        buffer.flip();
                        int port = client.socket().getLocalPort();
                        TcpHandlerStrategy handler = strategyMap.get(port);

                        if (handler != null) {
                            handler.handle(client, buffer, selector, clients);
                        } else {
                            log.warn("No handler for port: {}", port);

                        }
                        buffer.clear();
                    }

                    iterator.remove();
                } catch (Exception e) {
                    log.error("An error occurred while processing the selection key: {}", e.getMessage(), e);
                }
            });
        }
    }

    private static ByteBuffer allocateBuffer(SocketChannel client) throws IOException {

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

}
