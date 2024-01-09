package co.nyzo.verifier.messages;

import java.nio.ByteBuffer;

import co.nyzo.verifier.FieldByteSize;
import co.nyzo.verifier.MessageObject;

public class VersionRequest implements MessageObject {
    private int requestReason;

    public VersionRequest(){
        // 0 : Initialization of NodeManager
        this.requestReason = 0;
    }

    public VersionRequest(int requestReason){
        // 1 : UnfrozenBlockManager through NodeManager
        this.requestReason = requestReason;
    }

    @Override
    public int getByteSize(){
        return FieldByteSize.versionRequestReason;
    }

    @Override
    public byte[] getBytes(){
        byte[] array = new byte[getByteSize()];
        ByteBuffer buffer = ByteBuffer.wrap(array);
        buffer.putInt(requestReason);

        return array;
    }
}
