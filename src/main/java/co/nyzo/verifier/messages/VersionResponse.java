package co.nyzo.verifier.messages;

import java.nio.ByteBuffer;

import co.nyzo.verifier.FieldByteSize;
import co.nyzo.verifier.MessageObject;
import co.nyzo.verifier.Version;
import co.nyzo.verifier.util.LogUtil;

public class VersionResponse implements MessageObject {
    
    private int version;
    private int subVersion;

    public VersionResponse(){
        this.version = new Version().getVersion();
        this.subVersion = new Version().getSubVersion();
    }

    public VersionResponse(int version, int subVersion){
        this.version = version;
        this.subVersion = subVersion;
    }

    public int getVersion() {
        return this.version;
    }

    public int getSubVersion() {
        return this.subVersion;
    }

    @Override
    public int getByteSize(){
        return FieldByteSize.version;
    }

    @Override
    public byte[] getBytes(){
        int size = getByteSize();
        byte[] result = new byte[size];
        ByteBuffer buffer = ByteBuffer.wrap(result);

        buffer.putInt(this.version);
        buffer.putInt(this.subVersion);

        return result;
    }

    public static VersionResponse fromByteBuffer(ByteBuffer buffer){
        VersionResponse result = null;

        try {
            result = new VersionResponse(buffer.getInt(), buffer.getInt());
        } catch (Exception e){
            LogUtil.println("[1/2] Failed to construct a VersionResponse from a ByteBuffer");
            LogUtil.println("[2/2] " + e.getStackTrace().toString());
        }

        return result;
    }

    @Override
    public String toString(){
        return "[VersionResponse(version=" + this.version + ", subVersion=" + this.subVersion + ")]";
    }
}
