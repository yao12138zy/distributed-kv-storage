package shared;

import java.util.ArrayList;
import java.util.List;

public class ServerInfo {
    private boolean isValid = false;
    private String address = "";
    private int port = -1;
    private String connectionString;
    public ServerInfo(String address, int port) {
        if (address != null && !address.trim().equals("")) {
            this.address = address;
            this.port = port;
            this.connectionString = address + ":" + port;
            this.isValid = true;
        }
    }
    public ServerInfo(String connectionString) {
        this.connectionString = connectionString;
        if (connectionString == null)
            return;
        String[] fields = connectionString.trim().split(":");
        if (fields.length == 2) {
            address = fields[0].trim();
            try {
                port = Integer.parseInt(fields[1].trim());
                isValid = true;
            }
            catch (NumberFormatException ignore) { }
        }
    }
    public String getConnectionString() {
        return connectionString;
    }
    public boolean getIsValid() {
        return isValid;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public static List<ServerInfo> hashRingToServerInfoList(String hashRing) {
        List<ServerInfo> serverList = new ArrayList<>();
        if (hashRing != null) {
            String[] serverStrList = hashRing.trim().split(";");
            for (String serverStr : serverStrList) {
                String[] fieldList = serverStr.trim().split(",");
                if (fieldList.length == 3) {
                    String connStr = fieldList[2];
                    ServerInfo serverInfo = new ServerInfo(connStr);
                    serverList.add(serverInfo);
                }
            }
        }
        return serverList;
    }
}
