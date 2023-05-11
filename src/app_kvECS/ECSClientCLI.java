package app_kvECS;

import ecs.IECSNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

public class ECSClientCLI extends Thread {
    private final IECSClient ecs;
    private static final String PROMPT = "ECS> ";

    private boolean stop = false;
    public ECSClientCLI(IECSClient ecs) {
        this.ecs = ecs;
    }
    public void run() {

        final BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
        while (!stop) {
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                handleCommand(cmdLine);
            }
            catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }
        }

    }

    private void handleCommand(String cmdLine) {
        if (cmdLine == null) {
            try {
                Thread.sleep(1000);
                //ecs.start();
            }
            catch (InterruptedException e) {}
            catch (Exception e) {}
            return;
        }

        String[] tokens = cmdLine.split("\\s+");

        String command = tokens[0];
        if (command.equals("help")) {
            commandHelp();
        }
        else if (command.equals("nodes")) {
            commandNodes();
        }
        else {
            printError("Unknown command");
            printHelp();
        }
    }
    private void commandHelp()
    {
        printHelp();
    }

    private void commandNodes() {
        Map<String, IECSNode> nodes = ecs.getNodes();
        for (Map.Entry<String, IECSNode> entry : nodes.entrySet()) {
            IECSNode node = entry.getValue();
            printStdOut(node.getNodeName() + " [" + node.getNodeStatus() + "]");
        }
    }
    private void printHelp()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECS HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("put <key> <text message>");
        sb.append("\t\t puts a key value pair into the storage server. \n");
        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t Retrieves the value for the given key from the storage server. \n");
        sb.append(PROMPT).append("nodes");
        sb.append("\t\t\t List all nodes \n");

        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("help");
        sb.append("\t\t\t prints help information \n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t exits the program");
        System.out.println(sb.toString());
    }
    private void printError(String msg)
    {
        System.out.println("[ERROR] " + msg);
    }
    private void printStdOut(String msg)
    {
        System.out.println(msg);
    }
}
