package com.nio;

import lombok.Getter;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

@Getter
public enum ControlCommand {
    START_READ("start-read"),
    STOP_READ("stop-read");

    private final String value;

    ControlCommand(String value) {
        this.value = value;
    }

    public static Optional<ControlCommand> from(String input) {
        return Arrays.stream(values())
                .filter(cmd -> cmd.value.equalsIgnoreCase(input.trim()))
                .findFirst();
    }

    public static String supportedCommands() {
        return Arrays.stream(values())
                .map(ControlCommand::getValue)
                .collect(Collectors.joining("\n"));
    }
}
