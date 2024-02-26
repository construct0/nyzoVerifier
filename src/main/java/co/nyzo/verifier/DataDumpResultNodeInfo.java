package co.nyzo.verifier;

import java.nio.ByteBuffer;

public class DataDumpResultNodeInfo {
    public int majorVersion = -1;
    public int minorVersion = -1;
    public String version = "";
    public boolean isInCycle = false;

    public long frozenEdgeHeight = -1;
    public Block frozenEdgeBlock = null;
    public long genesisBlockStartTimestamp = -1;
    public long lastVerifierJoinHeight = -1;
    public long lastVerifierRemovalHeight = -1;
    public long retentionEdgeHeight = -1;
    public long trailingEdgeHeight = -1;
    public long openEdgeHeight = -1;
    public boolean isCycleComplete = false;
    

    public DataDumpResultNodeInfo(){
        majorVersion = Version.getVersion();
        minorVersion = Version.getSubVersion();
        version = "" + majorVersion + "." + minorVersion;
        isInCycle = BlockManager.verifierInCurrentCycle(ByteBuffer.wrap(Verifier.getIdentifier()));

        frozenEdgeHeight = BlockManager.getFrozenEdgeHeight();
        frozenEdgeBlock = BlockManager.getFrozenEdge();
        genesisBlockStartTimestamp = BlockManager.getGenesisBlockStartTimestamp();
        lastVerifierJoinHeight = BlockManager.getLastVerifierJoinHeight();
        lastVerifierRemovalHeight = BlockManager.getLastVerifierRemovalHeight();
        retentionEdgeHeight = BlockManager.getRetentionEdgeHeight();
        trailingEdgeHeight = BlockManager.getTrailingEdgeHeight();
        openEdgeHeight = BlockManager.openEdgeHeight(false);
        isCycleComplete = BlockManager.isCycleComplete();
    }
}
