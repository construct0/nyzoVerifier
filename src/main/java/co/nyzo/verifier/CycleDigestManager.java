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

// todo catch blocks & determine how block file consolidator behaves/should behave when disabled, no offset or individual block files exist and the consolidated block files are unpacked to get the block necessary for creating that cycle digest
// todo add remaining [].start() statements beyond client, perhaps require opt-in preference 
public class CycleDigestManager {
    public static final String startManagerKey = "start_cycle_digest_manager";
    private static final boolean startManager = PreferencesUtil.getBoolean(startManagerKey, true);

    public static final File individualCycleDigestDirectory = BlockManager.individualBlockDirectory;
    public static final long cycleDigestsPerFile = BlockManager.blocksPerFile;

    private static final AtomicBoolean alive = new AtomicBoolean(false);

    // The maximum amount of individual cycle digests which are created before a consolidation attempt is made
    private static final long maxCreateBatchSize = 5000L;

    // Delay between consolidate & create runs
    private static final long delayForSeconds = 30L;
    public static void start(){
        if(startManager && !alive.getAndSet(true)){
            new Thread(new Runnable() {
                @Override
                public void run(){
                    while(!UpdateUtil.shouldTerminate()){
                        try {
                            CycleDigestFileConsolidator.consolidateCycleDigests();    
                            CycleDigestManager.createCycleDigests();    

                            for(int i=0; i<(delayForSeconds / 2) && !UpdateUtil.shouldTerminate(); i++){
                                Thread.sleep(delayForSeconds * 10);
                            }
                        } catch (InterruptedException e){

                        } catch (Exception e) {

                        }
                    }
                }
            }).start();
        }
    }

    

    private static void createCycleDigests(){
        try {
            CycleDigest lastCycleDigest = CycleDigestManager.getLastCycleDigestEntry();
    
            Block frozenEdge = BlockManager.getFrozenEdge();
            long startAtBlockHeight = lastCycleDigest != null ? lastCycleDigest.getBlockHeight() + 1 : 0;
    
            // The block height beyond which trying to create cycle digests is not intended or supported in this call
            long stopBeforeBlockHeight = Math.min(startAtBlockHeight + CycleDigestManager.maxCreateBatchSize, frozenEdge.getBlockHeight() + 1);
    
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
    }

    // Writes 1 (one) cycle digest to the appropriate individual file
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

    // Writes cycle digests to a consolidated file
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

    // Loads all cycle digests from a consolidated file
    // Wraps the function below
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
    
    // Loads a specific set of cycle digests from a consolidated file
    // Akin to how blocks residing in a consolidated block file are loaded in
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


    private static File getIndividualCycleDigestFile(long blockHeight){
        return CycleDigest.fileForHeight(blockHeight);
    }

    public static File getConsolidatedCycleDigestFile(long fileHeight) {
        File consolidatedBlockFile = BlockManager.consolidatedFileForBlockHeight(fileHeight * BlockManager.blocksPerFile);
        return new File(consolidatedBlockFile.getAbsolutePath().replace("nyzoblock", "cycledigest"));
    }

    // Returns the last cycle digest stored on this system
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

    // Returns the last cycle digest stored in an individual cycle digest file, on this system
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

    // Returns the last cycle digest stored in a consolidated cycle digest file, on this system
    private static CycleDigest findHighestConsolidatedCycleDigestEntry(){
        long fileHeight = -1L;

        Long[] fileHeights = CycleDigestManager.getStoredConsolidatedCycleDigestFileHeights((Integer.MAX_VALUE * CycleDigestManager.cycleDigestsPerFile));
        
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
        List<Long> consolidatedCycleDigestHeights = new ArrayList<Long>();

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

                if(consolidatedCycleDigestHeights.size() == 0){
                    // The cycle digests start off at block 0 and then move on forward, using the previous block as reference
                    // Should the file for this height not exist, all subsequent cycle digest file heights are considered invalid
                    if(fileHeight != 0){
                        return new Long[0];
                    }
                } else {
                    // Akin to the principle in regards to height 0 above, any and all cycle digest file heights subsequent to the first interrupt/gap are considered invalid
                    // This is a redundant check, a non existing file should lead to a break before this evaluates to true
                    if((consolidatedCycleDigestHeights.get(consolidatedCycleDigestHeights.size() - 1) + 1) != fileHeight){
                        break;
                    }
                }

                consolidatedCycleDigestHeights.add(fileHeight);
            } else {
                // Cycle digest file does not exist
                break;
            }
        }

        return consolidatedCycleDigestHeights.toArray(new Long[consolidatedCycleDigestHeights.size()]);
    }
}
