package co.nyzo.verifier;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import co.nyzo.verifier.util.PreferencesUtil;
import co.nyzo.verifier.util.UpdateUtil;

// todo
public class HistoricalCycleDigestManager {
    public static final String startManagerKey = "start_historical_cycle_digest_manager";
    private static final boolean startManager = PreferencesUtil.getBoolean(startManagerKey, false);

    private static final AtomicBoolean alive = new AtomicBoolean(false);

    private static CycleDigest lastCycleDigest = null;

    public static void start(){
        if(startManager && !alive.getAndSet(true)){
            new Thread(new Runnable() {
                @Override
                public void run(){

                    while(!UpdateUtil.shouldTerminate()){
                        try {
                            Thread.sleep(10_000L);
                            createConsolidateCycleDigests();
                        } catch (Exception e){

                        } finally {

                        }
                    }
                }
            });
        }
    }

    private static void createConsolidateCycleDigests(){
        long frozenEdgeHeight = BlockManager.getFrozenEdgeHeight();

        // Uses the largest consolidation threshold as depicted in BlockFileConsolidator.consolidateFiles() whereby a
        // minimum interval of 1000 blocks, increased by the maximum amount of blocks which can be produced within the period 
        // since the BlockFileConsolidator last ran, increased by an arbitrary amount of 10 blocks; is enforced
        long consolidationThreshold = 
            frozenEdgeHeight 
            - (
                Math.max(BlockManager.blocksPerFile, (BlockManager.currentCycleLength() * 5)) 
                + (BlockFileConsolidator.runEverySeconds / (Block.minimumVerificationInterval / 1000)) 
                + 10
            );

        Long[] storedFileHeights = HistoricalCycleDigestManager.getStoredConsolidatedCycleDigestFileHeights(consolidationThreshold);
        long maxFileHeight = storedFileHeights.length > 0 ? storedFileHeights[storedFileHeights.length - 1] : -1L;

        if(lastCycleDigest == null){
            if(maxFileHeight != -1L){
                File lastFile = HistoricalCycleDigestManager.getConsolidatedCycleDigestFile(maxFileHeight);
                List<CycleDigest> lastCycleDigests = HistoricalCycleDigestManager.loadCycleDigestsInFile(lastFile);

                if(lastCycleDigests.size() != BlockManager.blocksPerFile){ // todo reuse of blocks per file is kinda jank

                }

                lastCycleDigest = lastCycleDigests.get(lastCycleDigests.size() - 1);
            }
        }

        long startFromFileHeight = (lastCycleDigest == null) ? 0 : lastCycleDigest.getBlockHeight() + 1;

        List<Block> relevantBlocks = new ArrayList<>();

        try {
            relevantBlocks = HistoricalCycleDigestManager.loadBlocksFromConsolidatedFile(BlockManager.consolidatedFileForBlockHeight(startFromFileHeight));
        } catch (Exception e){
            System.out.println();
        }

        List<CycleDigest> cycleDigestsToWrite = new ArrayList<>();
        CycleDigest rollingCycleDigest = null;

        for(int i=0; i<relevantBlocks.size(); i++){
            Block block = relevantBlocks.get(i);
            CycleDigest cycleDigest = null;

            if(lastCycleDigest == null){
                if(block.getBlockHeight() == 0){
                    cycleDigest = CycleDigest.digestForNextBlock(null, block.getVerifierIdentifier(), 0);
                }
            } else {
                if(rollingCycleDigest == null){
                    if((lastCycleDigest.getBlockHeight() + 1) == block.getBlockHeight()){
                        cycleDigest = CycleDigest.digestForNextBlock(lastCycleDigest, block.getVerifierIdentifier(), -1L); 
                    } 
                } else {
                    if((rollingCycleDigest.getBlockHeight() + 1) == block.getBlockHeight()){
                        cycleDigest = CycleDigest.digestForNextBlock(rollingCycleDigest, block.getVerifierIdentifier(), -1L);
                    }
                }
            }

            if(cycleDigest != null){
                cycleDigestsToWrite.add(cycleDigest);
                rollingCycleDigest = cycleDigest;
            } else {
                break;
            }
        }

        
        if(cycleDigestsToWrite.size() == BlockManager.blocksPerFile){ // todo reuse of blockPerFile is kinda jank
            HistoricalCycleDigestManager.writeCycleDigestsToFile(
                cycleDigestsToWrite,
                HistoricalCycleDigestManager.getConsolidatedCycleDigestFile(startFromFileHeight)                  
            );
            lastCycleDigest = cycleDigestsToWrite.get(cycleDigestsToWrite.size() - 1);
        }

        // todo cont.
    }

    private static List<CycleDigest> loadCycleDigestsInFile(File file){
        List<CycleDigest> cycleDigests = new ArrayList<>();
    
        if(file.exists()){
            Path path = Paths.get(file.getAbsolutePath());

            try {
                byte[] fileBytes = Files.readAllBytes(path);
                ByteBuffer buffer = ByteBuffer.wrap(fileBytes);
                int numberOfCycleDigests = buffer.getShort();

                for(int i=0; i<numberOfCycleDigests; i++){
                    CycleDigest cycleDigest = CycleDigest.fromByteBuffer(buffer);
                    cycleDigests.add(cycleDigest);
                }
            } catch (Exception e){
                cycleDigests = new ArrayList<>();
            }
        }

        return cycleDigests;
    }

    private static boolean writeCycleDigestsToFile(List<CycleDigest> cycleDigests, File file){
        // Determine the temporary file and ensure the location is available.
        File temporaryFile = new File(file.getAbsolutePath() + "_temp");
        temporaryFile.delete();

        boolean successful = true;

        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(temporaryFile, "rw");

            cycleDigests.sort(new Comparator<CycleDigest>(){
                @Override
                public int compare(CycleDigest c1, CycleDigest c2) {
                    return Long.compare(c1.getBlockHeight(), c2.getBlockHeight());
                }
            });

            randomAccessFile.writeShort((short)cycleDigests.size());

            for(int i=0;i<cycleDigests.size();i++){
                CycleDigest cycleDigest = cycleDigests.get(i);
                randomAccessFile.write(cycleDigest.getBytes());
            }

            randomAccessFile.close();
        } catch (Exception e) {
            successful = false;
        }

        if(successful){ 
            Path temporaryPath = Paths.get(temporaryFile.getAbsolutePath());
            Path path = Paths.get(file.getAbsolutePath());

            try {
                Files.move(temporaryPath, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (Exception ignored) {
                successful = false;
            }
        } else {
            temporaryFile.delete();
        }

        return successful;
    }

    private static List<Block> loadBlocksFromConsolidatedFile(File file) throws Exception {
        long fileHeight = Long.parseLong(
            file.toPath()
                .getFileName()
                .toString()
                .replace(".nyzoblock", "")
        );
        long minimumHeight = fileHeight * BlockManager.blocksPerFile;
        long maximumHeight = minimumHeight + BlockManager.blocksPerFile - 1;

        List<Block> blocks = BlockManager.loadBlocksInFile(file, minimumHeight, maximumHeight);

        if(blocks.size() == BlockManager.blocksPerFile){
            return blocks;
        }

        throw new Exception("Expected " + BlockManager.blocksPerFile + " blocks but got " + blocks.size() + " for file " + file.getAbsolutePath());
    }

    private static File getConsolidatedCycleDigestFile(long fileHeight) {
        if(fileHeight < 0) {
            return null;
        } 

        File consolidatedBlockFile = BlockManager.consolidatedFileForBlockHeight(fileHeight * BlockManager.blocksPerFile);
        return new File(consolidatedBlockFile.getAbsolutePath().replace("nyzoblock", "cycledigest"));
    }

    // Returns the consolidated cycle digest file heights up until the point of exhaustion or the occurrence of a gap in file heights
    private static Long[] getStoredConsolidatedCycleDigestFileHeights(long height) {
        if(height < BlockManager.blocksPerFile){
            return new Long[0];
        }

        int maxAmountOfFiles = (int) (height / BlockManager.blocksPerFile);
        Long[] consolidatedCycleDigestHeights = new Long[maxAmountOfFiles];

        for(int i=0; i<maxAmountOfFiles; i++){
            File file = HistoricalCycleDigestManager.getConsolidatedCycleDigestFile(i);

            if(file.exists()){
                long fileHeight = -1L;

                try {
                    fileHeight = Long.parseLong(
                        file.toPath()
                            .getFileName()
                            .toString()
                            .replace(".cycledigest", "")
                    );
                } catch (Exception ignored){

                }

                if(consolidatedCycleDigestHeights.length == 0){
                    // The cycle digests start off at block 0 and then move on forward, using the previous block as reference
                    // Should the file for this height not exist, all subsequent cycle digest file heights are considered invalid
                    if(fileHeight != 0){
                        return new Long[0];
                    }
                } else {
                    // Akin to the principle in regards to height 0 above, any and all cycle digest file heights subsequent to the first interrupt/gap are considered invalid
                    // This is a redundant check, a non existing file should lead to a break before this evaluates to true
                    if((consolidatedCycleDigestHeights[consolidatedCycleDigestHeights.length - 1] + 1) != fileHeight){
                        break;
                    }
                }

                consolidatedCycleDigestHeights[consolidatedCycleDigestHeights.length] = fileHeight;
            } else {
                // Cycle digest file does not exist
                break;
            }
        }

        return consolidatedCycleDigestHeights;
    }
}
