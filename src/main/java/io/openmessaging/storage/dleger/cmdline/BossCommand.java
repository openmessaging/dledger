package io.openmessaging.storage.dleger.cmdline;

import com.beust.jcommander.JCommander;
import io.openmessaging.storage.dleger.DLeger;
import io.openmessaging.storage.dleger.DLegerConfig;
import java.util.HashMap;
import java.util.Map;

public class BossCommand {

    public static void main(String args[]) {
        Map<String, BaseCommand> commands = new HashMap<>();
        commands.put("append", new AppendCommand());
        commands.put("get", new GetCommand());
        commands.put("readFile", new ReadFileCommand());

        JCommander.Builder builder = JCommander.newBuilder();
        builder.addCommand("server", new DLegerConfig());
        for (String cmd: commands.keySet()) {
            builder.addCommand(cmd, commands.get(cmd));
        }
        JCommander jc = builder.build();
        jc.parse(args);

        if (jc.getParsedCommand() == null) {
            jc.usage();
        } else if (jc.getParsedCommand().equals("server")) {
            String[] subArgs = new String[args.length - 1];
            System.arraycopy(args, 1, subArgs, 0, subArgs.length);
            DLeger.main(subArgs);
        } else {
            BaseCommand command = commands.get(jc.getParsedCommand());
            if (command != null) {
                command.doCommand();
            } else {
                jc.usage();
            }
        }
    }
}
