package com.nio;

import com.google.common.collect.Multimap;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

@Slf4j
public class CommandControlHandler implements TcpHandlerStrategy {

    @Override
    public void handle(SocketChannel client, ByteBuffer buffer, Selector selector, Multimap<Integer, SocketChannel> clients) throws IOException {
        String commandText = StandardCharsets.UTF_8.decode(buffer).toString().trim().toLowerCase();

        ControlCommand.from(commandText).ifPresentOrElse(command -> {
            switch (command) {
                case STOP_READ:
                    log.info("Handle stop-read");
                    registerEvent(selector, clients, 0);
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
            }
        });
    }

    private void registerEvent(final Selector selector, final Multimap<Integer, SocketChannel> clients, final int op) {
        clients.get(5555).forEach(c -> {
            try {
                c.register(selector, op);
            } catch (Exception e) {
                log.error("Failed to register event for client: {}", c, e);
            }
        });
    }
}
