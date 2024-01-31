package co.nyzo.verifier;

import co.nyzo.verifier.util.IpUtil;

import java.nio.ByteBuffer;
import java.util.*;

public class Node implements MessageObject {

    private static final int communicationFailureInactiveThreshold = 6;

    private String nickname; 
    private boolean identifierIsKnown;
    private byte[] identifier;                    // wallet public key (32 bytes)
    private String identifierString;
    private byte[] ipAddress;                     // IPv4 address, stored as bytes to keep memory predictable (4 bytes)
    private String ipAddressString;
    private int portTcp;                          // TCP port number
    private int portUdp;                          // UDP port number, if available
    private long queueTimestamp;                  // this is the timestamp that determines queue placement -- it is
                                                  // when the verifier joined the mesh or when the verifier was last
                                                  // updated
    private boolean inCycle;
    private long inactiveTimestamp;               // when the verifier was marked as inactive; -1 for active verifiers
    private long communicationFailureCount;       // consecutive communication failures before marking inactive

    public Node(byte[] identifier, byte[] ipAddress, int portTcp, int portUdp) {

        // This identifier is used as a substitute when the identifier of a queue node is not known (DataDumper.getMeshParticipants)
        if(Arrays.equals(ByteUtil.byteArrayFromHexString("0000000000000000-0000000000000000-0000000000000000-0000000000000000", FieldByteSize.identifier), identifier)){
            this.identifierIsKnown = false;
        } else {
            this.identifierIsKnown = true;
        }

        this.setNickname(NicknameManager.get(identifier));
        this.identifier = Arrays.copyOf(identifier, FieldByteSize.identifier);
        this.identifierString = ByteUtil.arrayAsStringWithDashes(this.identifier);
        this.ipAddress = Arrays.copyOf(ipAddress, FieldByteSize.ipAddress);
        this.ipAddressString = IpUtil.addressAsString(this.ipAddress);
        this.portTcp = portTcp;
        this.portUdp = portUdp;
        this.queueTimestamp = System.currentTimeMillis();
        this.inactiveTimestamp = -1L;
        this.communicationFailureCount = 0;
    }

    public String getNickname(){
        return nickname;
    }

    public void setNickname(String nickname){
        this.nickname = nickname;
    }

    public boolean getIdentifierIsKnown(){
        return identifierIsKnown;
    }

    public byte[] getIdentifier() {
        return identifier;
    }

    public String getIdentifierString(){
        return identifierString;
    }

    public void setIdentifier(byte[] identifier) {
        this.identifier = identifier;
        this.identifierString = ByteUtil.arrayAsStringWithDashes(identifier);
    }

    public byte[] getIpAddress() {
        return ipAddress;
    }

    public String getIpAddressString() {
        return ipAddressString;
    }

    public int getPortTcp() {
        return portTcp;
    }

    public void setPortTcp(int portTcp) {
        this.portTcp = portTcp;
    }

    public int getPortUdp() {
        return portUdp;
    }

    public void setPortUdp(int portUdp) {
        this.portUdp = portUdp;
    }

    public long getQueueTimestamp() {
        return queueTimestamp;
    }

    public void setQueueTimestamp(long queueTimestamp) {
        this.queueTimestamp = queueTimestamp;
    }

    public long getInactiveTimestamp() {
        return inactiveTimestamp;
    }

    public void setInactiveTimestamp(long inactiveTimestamp) {
        this.inactiveTimestamp = inactiveTimestamp;
    }

    public void setInCycle(boolean inCycle){
        this.inCycle = inCycle;
    }

    public boolean getInCycle() {
        return this.inCycle;
    }

    public boolean isActive() {
        return inactiveTimestamp < 0;
    }

    public void markSuccessfulConnection() {
        communicationFailureCount = 0;
        inactiveTimestamp = -1L;
    }

    public void markFailedConnection() {
        if (++communicationFailureCount >= communicationFailureInactiveThreshold && inactiveTimestamp < 0) {
            inactiveTimestamp = System.currentTimeMillis();
        }
    }

    public static int getByteSizeStatic() {
        return FieldByteSize.identifier + FieldByteSize.ipAddress + FieldByteSize.port + FieldByteSize.timestamp;
    }

    @Override
    public int getByteSize() {
        return getByteSizeStatic();
    }

    @Override
    public byte[] getBytes() {

        byte[] result = new byte[getByteSize()];
        ByteBuffer buffer = ByteBuffer.wrap(result);
        buffer.put(identifier);
        buffer.put(ipAddress);
        buffer.putInt(portTcp);
        buffer.putLong(queueTimestamp);

        return result;
    }

    public static Node fromByteBuffer(ByteBuffer buffer) {

        byte[] identifier = new byte[FieldByteSize.identifier];
        buffer.get(identifier);
        byte[] ipAddress = new byte[FieldByteSize.ipAddress];
        buffer.get(ipAddress);
        int portTcp = buffer.getInt();
        long queueTimestamp = buffer.getLong();

        Node node = new Node(identifier, ipAddress, portTcp, -1);
        node.setQueueTimestamp(queueTimestamp);

        return node;
    }

    @Override
    public String toString() {
        return "[Node: " + IpUtil.addressAsString(getIpAddress()) + ":TCP=" + portTcp + ",UDP=" + portUdp + "]";
    }
}
