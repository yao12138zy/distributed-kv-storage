package shared;

public class RaftPayload {
    private int termNumber;
    private String address;
    public RaftPayload(int termNumber, String address) {
        this.termNumber = termNumber;
        this.address = address;
    }
    public RaftPayload(String serialized) {
        String[] parts = serialized.split(",");
        termNumber = Integer.parseInt(parts[0]);
        address = parts[1];
    }
    public int getTermNumber() {
        return termNumber;
    }
    public String getAddress() {
        return address;
    }
    public String serialize() {
        return String.format("%d,%s", termNumber, address);
    }
}
