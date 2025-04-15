package com.nio;

import com.google.common.collect.Multimap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

@Slf4j
@RequiredArgsConstructor
public class CommandControlHandler implements TcpHandlerStrategy {
    private static final int READ_CHANNEL_PORT = 5555;
    private static final int NO_OPS = 0; // Represents no interest in any operation
    private final Selector selector;
    private final Multimap<Integer, SocketChannel> clients;

    @Override
    public void handle(SocketChannel client, ByteBuffer buffer) throws IOException {
        if (client == null || buffer == null || selector == null || clients == null) {
            log.warn("Received null input in handle method: client={}, buffer={}, selector={}, clients={}", client, buffer, selector, clients);
            return;
        }
            String commandText = StandardCharsets.UTF_8.decode(buffer).toString().trim().toLowerCase();

        ControlCommand.from(commandText).ifPresentOrElse(command -> {
            switch (command) {
                case STOP_READ:
                    log.info("Handle stop-read");
                    registerEvent(selector, clients, NO_OPS);
                    break;
                case START_READ:
                    log.info("Handle start-read");
                    registerEvent(selector, clients, SelectionKey.OP_READ);
                    break;
            }
        }, () -> {
            String help = "Supported commands:\n" + ControlCommand.supportedCommands() + "\n";
            try {
                client.write(ByteBuffer.wrap(help.getBytes(StandardCharsets.UTF_8)));
            } catch (IOException e) {
                log.warn("Failed to write help message to client: {}", client, e);
                closeClient(client);
            }
        });
    }

    private void closeClient(SocketChannel client) {
        try {
            client.close();
            log.info("Closed client connection: {}", client);
        } catch (IOException e) {
            log.error("Failed to close client channel: {}", client, e);
        }
    }

    private void registerEvent(final Selector selector, final Multimap<Integer, SocketChannel> clients, final int op) {
        clients.get(READ_CHANNEL_PORT).forEach(c -> {
            try {
                c.register(selector, op);
            } catch (Exception e) {
                log.error("Failed to register event for client: {}", c, e);
                closeClient(c);
                clients.remove(READ_CHANNEL_PORT, c);
            }
        });
    }
}
