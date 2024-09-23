package co.nyzo.verifier;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import co.nyzo.verifier.util.PreferencesUtil;
import co.nyzo.verifier.util.UpdateUtil;

// todo
public class CycleDigestManager {
    public static final String startManagerKey = "start_cycle_digest_manager";
    private static final boolean startManager = PreferencesUtil.getBoolean(startManagerKey, false); // todo check

    public static final File individualCycleDigestDirectory = BlockManager.individualBlockDirectory;
    public static final long cycleDigestsPerFile = BlockManager.blocksPerFile;

    private static final AtomicBoolean alive = new AtomicBoolean(false);

    // private static CycleDigest lastCycleDigest = null;

    public static void start(){
        if(startManager && !alive.getAndSet(true)){
            new Thread(new Runnable() {
                @Override
                public void run(){
                    createCycleDigests();

                    while(!UpdateUtil.shouldTerminate()){
                        try {
                            for(int i=0; i<100 && !UpdateUtil.shouldTerminate(); i++){
                                Thread.sleep(2000L);
                            }
                        } catch (Exception e){

                        } finally {
                            createCycleDigests();    
                            CycleDigestFileConsolidator.consolidateCycleDigests();    
                        }
                    }
                }
            });
        }
    }

    

    private static void createCycleDigests(){
        try {
            CycleDigest lastCycleDigest = CycleDigestManager.getLastCycleDigestEntry();

            // Akin to block files, cycle digest files behind the retention edge are consolidated. If the retention edge is not available, step back 5 cycles.
            // This will prevent accumulation of excessive individual files due to discontinuities.
            // long consolidateBeforeBlockHeight;
            // if (BlockManager.getRetentionEdgeHeight() >= 0) {
            //     consolidateBeforeBlockHeight = BlockManager.getRetentionEdgeHeight();
            // } else {
            //     consolidateBeforeBlockHeight = BlockManager.getFrozenEdgeHeight() - BlockManager.currentCycleLength() * 5;
            // }
    
            Block frozenEdge = BlockManager.getFrozenEdge();
            long startAtBlockHeight = lastCycleDigest != null ? lastCycleDigest.getBlockHeight() + 1 : 0;
    
            // The block height beyond which trying to create cycle digests is not intended or supported in this call
            long stopBeforeBlockHeight = Math.min(startAtBlockHeight + 10_000L, frozenEdge.getBlockHeight() + 1);
    
            // List<CycleDigest> cycleDigestsToWrite = new ArrayList<>();
            CycleDigest rollingCycleDigest = null;
    
            Block block;
            for(long blockHeight=startAtBlockHeight; blockHeight < stopBeforeBlockHeight; blockHeight++){
                block = null;
    
                // Attempt to get the frozen block from memory
                block = BlockManager.frozenBlockForHeight(blockHeight);
    
                // Attempt to get the frozen block without writing individual files to disk in the process, this requires offset files to be present
                try {
                    if(block == null){
                        block = HistoricalBlockManager.blockForHeight(blockHeight);
                    }
                } catch(Exception e){
                    block = null;
                }
    
                // Attempt to get the frozen block by extracting the consolidated file for that height, this writes all individual blocks associated with that height to disk in the process
                // If the block file consolidator consolidates individual block files these will be consolidated again in the future
                if(block == null){
                    block = BlockManager.loadBlockFromFile(blockHeight);
                }
                
                // Could not load block, processing with and beyond this block height is not possible
                // todo - when building from block 0 onwards, without the presence of any stored cycle digests which would make this possible
                // todo - when considering building on a cycle digest > 0, for starters, the resource-managing aspect (avoid repetition, reliance on previous digest) needs to be taken into account 
                if(block == null){
                    break;
                }
    
                CycleDigest cycleDigest = null;
                
    
                if(lastCycleDigest == null || block.getBlockHeight() == 0){
                    cycleDigest = CycleDigest.digestForNextBlock(null, block.getVerifierIdentifier(), 0);
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
                    boolean writeSuccessful = CycleDigestManager.writeCycleDigestToIndividualFile(cycleDigest);
                    
                    if(!writeSuccessful){
                        // Writing failed, this is retried after the thread sleep at the calling location
                        break;
                    }
                    
                    rollingCycleDigest = cycleDigest;
                } else {
                    // None of the conditions above were met, no cycle digest was created & added to the list of cycle digests to write to disk
                    break;
                }
            }
        } catch (Exception e){

        }
       

        // get highest indiv cycle digest block height
        // get highest cycle digest block height stored within a consolidated file

        // if any != -1 OR -1
        //      get individual blocks AMOUNT beyond max(indivmax, consmax)
        //      get blocks AMOUNT in consolidated file(s) beyond max(indivmax, consmax)

        //      per N individual blocks: get individual blocks & AMOUNT-=N
        //      per N blocks : get blocks & AMOUNT-=N

        //      if any blocks
        //          get file->cycledigest for highest cycle digest block height, set as previousCycleDigest
        //          foreach indiv block, use previousCycleDigest to create cycleDigest, add to list, set previousCycleDigest
        //          save indiv cycle digest
        //
        //          get consfile->cycledigest for highest cycle digest block height, set as previousCycleDigest; if consfile not full, store as write target
        //          foreach cons block, " " " " " " " " " " " " " " "
        //              accum until write target limit reached, save; set new write target (next cons file)

        
    }

    // private static void consolidateCycleDigests(){
        // long frozenEdgeHeight = BlockManager.getFrozenEdgeHeight();

        // Uses the largest consolidation threshold as depicted in BlockFileConsolidator.consolidateFiles() whereby an additional
        // minimum interval of 1000 blocks, increased by the maximum amount of blocks which can be produced within the period 
        // since the BlockFileConsolidator last ran, increased by an arbitrary amount of 10 blocks; is enforced
        // long consolidationThreshold = 
        //     frozenEdgeHeight 
        //     - (
        //         Math.max(BlockManager.blocksPerFile, (BlockManager.currentCycleLength() * 5)) 
        //         + (BlockFileConsolidator.runEverySeconds / (Block.minimumVerificationInterval / 1000)) 
        //         + 10
        //     );

        // Block files behind the retention edge are consolidated. If the retention edge is not available, step back 5 cycles.
        // This will prevent accumulation of excessive individual files due to discontinuities.
        // long consolidationThreshold;
        // if (BlockManager.getRetentionEdgeHeight() >= 0) {
        //     consolidationThreshold = BlockManager.getRetentionEdgeHeight();
        // } else {
        //     consolidationThreshold = BlockManager.getFrozenEdgeHeight() - BlockManager.currentCycleLength() * 5;
        // }
        // long currentFileIndex = consolidationThreshold / BlockManager.blocksPerFile;

        // Long[] storedFileHeights = HistoricalCycleDigestManager.getStoredConsolidatedCycleDigestFileHeights(consolidationThreshold);
        // long maxFileHeight = storedFileHeights.length > 0 ? storedFileHeights[storedFileHeights.length - 1] : -1L;
        // File lastFile = HistoricalCycleDigestManager.getConsolidatedCycleDigestFile(maxFileHeight);
        
        // List<CycleDigest> lastConsolidatedCycleDigests = new ArrayList<>();
        // CycleDigest lastCycleDigest = null;

        // if(maxFileHeight != -1L){
        //     lastConsolidatedCycleDigests = HistoricalCycleDigestManager.loadCycleDigestsFromConsolidatedFile(
        //         lastFile,
        //         maxFileHeight * HistoricalCycleDigestManager.cycleDigestsPerFile,
        //         (maxFileHeight * HistoricalCycleDigestManager.cycleDigestsPerFile) + (HistoricalCycleDigestManager.cycleDigestsPerFile - 1)
        //     );

        //     if(lastConsolidatedCycleDigests.size() > 0){
        //         lastCycleDigest = lastConsolidatedCycleDigests.get(lastConsolidatedCycleDigests.size() - 1);
        //     }
        // }

        





        // long startFromFileHeight = (lastCycleDigest == null) ? 0 : lastCycleDigest.getBlockHeight() + 1;

        // List<Block> relevantBlocks = new ArrayList<>();

        // try {
        //     relevantBlocks = HistoricalCycleDigestManager.loadBlocksFromConsolidatedFile(BlockManager.consolidatedFileForBlockHeight(startFromFileHeight));
        // } catch (Exception e){
        //     System.out.println();
        // }

        // List<CycleDigest> cycleDigestsToWrite = new ArrayList<>();
        // CycleDigest rollingCycleDigest = null;

        // for(int i=0; i<relevantBlocks.size(); i++){
        //     Block block = relevantBlocks.get(i);
        //     CycleDigest cycleDigest = null;

        //     if(lastCycleDigest == null){
        //         if(block.getBlockHeight() == 0){
        //             cycleDigest = CycleDigest.digestForNextBlock(null, block.getVerifierIdentifier(), 0);
        //         }
        //     } else {
        //         if(rollingCycleDigest == null){
        //             if((lastCycleDigest.getBlockHeight() + 1) == block.getBlockHeight()){
        //                 cycleDigest = CycleDigest.digestForNextBlock(lastCycleDigest, block.getVerifierIdentifier(), -1L); 
        //             } 
        //         } else {
        //             if((rollingCycleDigest.getBlockHeight() + 1) == block.getBlockHeight()){
        //                 cycleDigest = CycleDigest.digestForNextBlock(rollingCycleDigest, block.getVerifierIdentifier(), -1L);
        //             }
        //         }
        //     }

        //     if(cycleDigest != null){
        //         cycleDigestsToWrite.add(cycleDigest);
        //         rollingCycleDigest = cycleDigest;
        //     } else {
        //         break;
        //     }
        // }

        
        // if(cycleDigestsToWrite.size() == HistoricalCycleDigestManager.cycleDigestsPerFile){
        //     HistoricalCycleDigestManager.writeCycleDigestsToConsolidatedFile(
        //         cycleDigestsToWrite,
        //         HistoricalCycleDigestManager.getConsolidatedCycleDigestFile(startFromFileHeight)                  
        //     );
        //     lastCycleDigest = cycleDigestsToWrite.get(cycleDigestsToWrite.size() - 1);
        // }

        // todo cont.
    // }

    

    private static boolean writeCycleDigestToIndividualFile(CycleDigest cycleDigest){
        File file = CycleDigestManager.getIndividualCycleDigestFile(cycleDigest.getBlockHeight());

        // Determine the temporary file and ensure the location is available.
        File temporaryFile = new File(file.getAbsolutePath() + "_temp");
        temporaryFile.delete();

        boolean successful = true;

        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(temporaryFile, "rw");

            randomAccessFile.write(cycleDigest.getBytes());
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

    public static boolean writeCycleDigestsToConsolidatedFile(List<CycleDigest> cycleDigests, File file){
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

    public static CycleDigest loadCycleDigestFromIndividualFile(File file) {
        return CycleDigest.fromFile(file);
    }

    public static List<CycleDigest> loadCycleDigestsFromConsolidatedFile(File file){
        long fileHeight = Long.parseLong(
            file.toPath()
                .getFileName()
                .toString()
                .replace(".cycledigest", "")
        );

        long minimumHeight = fileHeight * CycleDigestManager.cycleDigestsPerFile;
        long maximumHeight = minimumHeight + CycleDigestManager.cycleDigestsPerFile - 1;

        return CycleDigestManager.loadCycleDigestsFromConsolidatedFile(file, minimumHeight, maximumHeight);
    }
    
    private static List<CycleDigest> loadCycleDigestsFromConsolidatedFile(File file, long minimumBlockHeight, long maximumBlockHeight){
        List<CycleDigest> cycleDigests = new ArrayList<>();
    
        if(file.exists()){
            Path path = Paths.get(file.getAbsolutePath());

            try {
                byte[] fileBytes = Files.readAllBytes(path);
                ByteBuffer buffer = ByteBuffer.wrap(fileBytes);
                int numberOfCycleDigests = buffer.getShort();
                CycleDigest previousCycleDigest = null;

                for(int i=0; i<numberOfCycleDigests && (previousCycleDigest == null || previousCycleDigest.getBlockHeight() < maximumBlockHeight); i++){
                    CycleDigest cycleDigest = CycleDigest.fromByteBuffer(buffer);

                    if(cycleDigest.getBlockHeight() >= minimumBlockHeight && cycleDigest.getBlockHeight() <= maximumBlockHeight){
                        cycleDigests.add(cycleDigest);
                    }

                    previousCycleDigest = cycleDigest;
                }
            } catch (Exception ignored){ }
        }

        Collections.sort(cycleDigests, new Comparator<CycleDigest>(){
            @Override
            public int compare(CycleDigest c0, CycleDigest c1){
                return Long.compare(c0.getBlockHeight(), c1.getBlockHeight());
            }
        });

        return cycleDigests;
    }

    // private static List<Block> loadBlocksFromConsolidatedFile(File file) throws Exception {
    //     long fileHeight = Long.parseLong(
    //         file.toPath()
    //             .getFileName()
    //             .toString()
    //             .replace(".nyzoblock", "")
    //     );
    //     long minimumHeight = fileHeight * BlockManager.blocksPerFile;
    //     long maximumHeight = minimumHeight + BlockManager.blocksPerFile - 1;

    //     List<Block> blocks = BlockManager.loadBlocksInFile(file, minimumHeight, maximumHeight);

    //     Collections.sort(blocks, new Comparator<Block>(){
    //         @Override
    //         public int compare(Block b0, Block b1){
    //             return Long.compare(b0.getBlockHeight(), b1.getBlockHeight());
    //         }
    //     });

    //     return blocks;
    //     // if(blocks.size() == BlockManager.blocksPerFile){
    //     //     return blocks;
    //     // }

    //     // throw new Exception("Expected " + BlockManager.blocksPerFile + " blocks but got " + blocks.size() + " for file " + file.getAbsolutePath());
    // }


    private static File getIndividualCycleDigestFile(long blockHeight){
        return CycleDigest.fileForHeight(blockHeight);
    }

    public static File getConsolidatedCycleDigestFile(long fileHeight) {
        File consolidatedBlockFile = BlockManager.consolidatedFileForBlockHeight(fileHeight * BlockManager.blocksPerFile);
        return new File(consolidatedBlockFile.getAbsolutePath().replace("nyzoblock", "cycledigest"));
    }

    private static CycleDigest getLastCycleDigestEntry(){
        CycleDigest lastIndividualCycleDigestEntry = CycleDigestManager.findHighestIndividualCycleDigestEntry();
        CycleDigest lastConsolidatedCycleDigestEntry = CycleDigestManager.findHighestConsolidatedCycleDigestEntry();
        boolean bothExist = lastIndividualCycleDigestEntry != null && lastConsolidatedCycleDigestEntry != null;

        CycleDigest lastCycleDigestEntry = null;

        if(bothExist){
            lastCycleDigestEntry = lastIndividualCycleDigestEntry.getBlockHeight() > lastConsolidatedCycleDigestEntry.getBlockHeight() 
                                 ? lastIndividualCycleDigestEntry : lastConsolidatedCycleDigestEntry;
        } else {
            lastCycleDigestEntry = lastIndividualCycleDigestEntry != null ? lastIndividualCycleDigestEntry : lastConsolidatedCycleDigestEntry;
        }

        return lastCycleDigestEntry;
    }

    private static CycleDigest findHighestIndividualCycleDigestEntry(){
        long height = -1L;
        
        try {
            List<File> files = Arrays.asList(CycleDigestManager.getStoredIndividualCycleDigestFiles());
            
            Collections.sort(files, new Comparator<File>() {
                @Override
                public int compare(File file1, File file2) {
                    return file2.getName().compareTo(file1.getName());
                }
            });

            for (int i = 0; i < files.size() && height < 0; i++) {
                try {
                    height = Long.parseLong(files.get(i).getName().replace("i_", "").replace(".cycledigest", ""));
                } catch (Exception ignored) {

                }
            }
        } catch (Exception ignored){

        }

        if(height > -1L) {
            return CycleDigestManager.loadCycleDigestFromIndividualFile(
                CycleDigestManager.getIndividualCycleDigestFile(height)
            );
        }

        return null;
    }

    private static File[] getStoredIndividualCycleDigestFiles(){
        return CycleDigestManager.individualCycleDigestDirectory.listFiles(f -> f.getAbsolutePath().endsWith("cycledigest"));
    }

    private static CycleDigest findHighestConsolidatedCycleDigestEntry(){
        long fileHeight = -1L;

        Long[] fileHeights = CycleDigestManager.getStoredConsolidatedCycleDigestFileHeights(Long.MAX_VALUE);
        
        if(fileHeights.length > 0){
            List<Long> fileHeightList = Arrays.asList(fileHeights);

            Collections.sort(fileHeightList, new Comparator<Long>(){
                @Override
                public int compare(Long i0, Long i1){
                    return Long.compare(i0, i1);
                }
            });

            fileHeight = fileHeightList.get(fileHeightList.size() - 1);
        }

        if(fileHeight > -1L) {
            List<CycleDigest> cycleDigests = CycleDigestManager.loadCycleDigestsFromConsolidatedFile(
                CycleDigestManager.getConsolidatedCycleDigestFile(fileHeight)
            );

            return cycleDigests.get(cycleDigests.size() - 1);
        }

        return null;
    }

    // Returns the consolidated cycle digest file heights up until the point of exhaustion or the occurrence of a gap in file heights
    private static Long[] getStoredConsolidatedCycleDigestFileHeights(long blockHeight) {
        int maxAmountOfFiles = (int) (blockHeight / BlockManager.blocksPerFile);
        Long[] consolidatedCycleDigestHeights = new Long[maxAmountOfFiles];

        for(int i=0; i<maxAmountOfFiles; i++){
            File file = CycleDigestManager.getConsolidatedCycleDigestFile(i);

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
