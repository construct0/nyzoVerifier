package co.nyzo.verifier;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import co.nyzo.verifier.util.LogUtil;
import co.nyzo.verifier.util.PreferencesUtil;
import co.nyzo.verifier.util.UpdateUtil;

// todo should the cycle digest in the future be subject to blockchain version-related changes, consider/check whether attempting to read >1 cycledigest from 1 indiv cycle digest file makes sense 

public class CycleDigestFileConsolidator {
    // To align with the run modes of the BlockFileConsolidator;
    // The consolidator has 3 run options:
    // - consolidate: create consolidated files and delete individual files (normal operation)
    // - delete: delete individual files only (do not create consolidated files)
    // - disable: do not run (do not create consolidated files, do not delete individual files)
    public static final String runOptionKey = "cycle_digest_file_consolidator";
    public static final String runOptionValueConsolidate = "consolidate";
    public static final String runOptionValueDeleteOnly = "delete";
    public static final String runOptionValueDisable = "disable";
    private static String runOption = PreferencesUtil.get(runOptionKey).toLowerCase();

    // public static final int runEverySeconds = 300;

    //  todo fileIndex -> fileHeight to be in concordance w term in manager

    static {
        // Ensure the value is explicitly set to one of the enumerated values. The default is "delete" for the verifier
        // and the sentinel. This conserves disk space, which reduces maintenance and improves robustness of these run
        // modes. The default is "consolidate" for the client. The client never participates in blockchain creation,
        // which weighs its emphasis more toward utility.
        if (!runOption.equals(runOptionValueConsolidate) && !runOption.equals(runOptionValueDeleteOnly) && !runOption.equals(runOptionValueDisable)) {
            if (RunMode.getRunMode() == RunMode.Client) {
                runOption = runOptionValueConsolidate;
            } else {
                runOption = runOptionValueDeleteOnly;
            }
        }
    }

    // public static void start(){
    //     // Ensure the value is explicitly set to one of the enumerated values. The default is "delete" for the verifier
    //     // and the sentinel. This conserves disk space, which reduces maintenance and improves robustness of these run
    //     // modes. The default is "consolidate" for the client. The client never participates in blockchain creation,
    //     // which weighs its emphasis more toward utility.
    //     if (!runOption.equals(runOptionValueConsolidate) && !runOption.equals(runOptionValueDeleteOnly) &&
    //             !runOption.equals(runOptionValueDisable)) {
    //         if (RunMode.getRunMode() == RunMode.Client) {
    //             runOption = runOptionValueConsolidate;
    //         } else {
    //             runOption = runOptionValueDeleteOnly;
    //         }
    //     }
    //     LogUtil.println("CycleDigestFileConsolidator setting: " + runOptionKey + "=" + runOption);
    
    //     if(!runOption.equals(runOptionValueDisable)){
    //         new Thread(new Runnable(){
    //             @Override
    //             public void run(){
    //                 consolidateCycleDigests();

    //                 while(!UpdateUtil.shouldTerminate()){
    //                     try {
    //                         // Sleep for 5 minutes (300 seconds) in 3-second intervals
    //                         for(int i=0; i< (runEverySeconds / 3) && !UpdateUtil.shouldTerminate(); i++){
    //                             Thread.sleep(runEverySeconds * 10);
    //                         }
    //                     } catch (Exception e){

    //                     } finally {
    //                         consolidateCycleDigests();
    //                     }
    //                 }
    //             }
    //         })
    //     }
    // }

    public static void consolidateCycleDigests(){
        

        if(runOption.equals(CycleDigestFileConsolidator.runOptionValueDisable)){
            return;
        }

        try {
            // Get all cycle digest files in the individual directory
            File[] individualFiles = CycleDigestManager.individualCycleDigestDirectory.listFiles(f -> f.getAbsolutePath().endsWith("cycledigest"));

            // Files behind the retention edge are consolidated. If the retention edge is not available, step back 5 cycles.
            // This will prevent accumulation of excessive individual files due to discontinuities.
            long consolidationThreshold;
            if (BlockManager.getRetentionEdgeHeight() >= 0) {
                consolidationThreshold = BlockManager.getRetentionEdgeHeight();
            } else {
                consolidationThreshold = BlockManager.getFrozenEdgeHeight() - BlockManager.currentCycleLength() * 5;
            }
            
            long currentFileHeight = consolidationThreshold / CycleDigestManager.cycleDigestsPerFile;

            // Build a map of all files that need to be consolidated
            // file height : individual cycle digest files within the scope of said file height
            Map<Long, List<File>> fileMap = new HashMap<>();
            if(individualFiles != null){
                for(File file : individualFiles){
                    long blockHeight = CycleDigestFileConsolidator.blockHeightForFile(file);
                    if(blockHeight > 0){
                        long fileHeight = blockHeight / CycleDigestManager.cycleDigestsPerFile;

                        if(fileHeight < currentFileHeight){
                            List<File> filesForFileHeight = fileMap.get(fileHeight);

                            if(filesForFileHeight == null){
                                filesForFileHeight = new ArrayList<>();
                                fileMap.put(fileHeight, filesForFileHeight);
                            }

                            filesForFileHeight.add(file);
                        }
                    }
                }
            }

            // Process each file height
            for(Long fileHeight : fileMap.keySet()){
                boolean performDelete = false;

                // If the delete-only option is set, skip consolidation
                if (!runOption.equals(runOptionValueDeleteOnly)) {
                    performDelete = consolidateFiles(fileHeight, fileMap.get(fileHeight));
                } else {
                    performDelete = true;
                }

                if(performDelete){
                    // The safeguard which applies to the block isn't enforced for the cycle digest with block height 0
                    for(File file : fileMap.get(fileHeight)){
                        file.delete();
                    }
                }
                
                // todo
                System.out.println();
            }

            // , , - , ,

        } catch (Exception e){

        }
    }

    private static boolean consolidateFiles(long fileHeight, List<File> individualFiles) {
        boolean successful;

        try {
            File consolidatedFile = CycleDigestManager.getConsolidatedCycleDigestFile(fileHeight);

            // Get the blocks from the existing consolidated file for this file height
            List<CycleDigest> cycleDigests = CycleDigestManager.loadCycleDigestsFromConsolidatedFile(consolidatedFile);

            // Add the blocks from the individual files
            for(File file : individualFiles){
                CycleDigest digest = CycleDigestManager.loadCycleDigestFromIndividualFile(file);

                if(digest == null){
                    throw new Exception(); // todo 
                }

                cycleDigests.add(
                    digest
                );
            }

            // Sort the cycle digests on block height ascending
            Collections.sort(cycleDigests, new Comparator<CycleDigest>() {
                @Override
                public int compare(CycleDigest digest0, CycleDigest digest1) {
                    return ((Long) digest0.getBlockHeight()).compareTo(digest1.getBlockHeight());
                }
            });

            // Dedupe cycle digests
            for (int i = cycleDigests.size() - 1; i > 0; i--) {
                if (cycleDigests.get(i).getBlockHeight() == cycleDigests.get(i - 1).getBlockHeight()) {
                    cycleDigests.remove(i);
                }
            }

            Long previousBlockHeight = -1L;
            for(int i=0; i<cycleDigests.size(); i++){
                if(previousBlockHeight == -1L){
                    previousBlockHeight = cycleDigests.get(i).getBlockHeight();
                    continue;
                }

                if(previousBlockHeight != cycleDigests.get(i).getBlockHeight() - 1){
                    throw new Exception(); // todo
                }

                previousBlockHeight = cycleDigests.get(i).getBlockHeight();
            }

            // Write the consolidated file
            consolidatedFile.getParentFile().mkdirs();
            CycleDigestManager.writeCycleDigestsToConsolidatedFile(cycleDigests, consolidatedFile);
        } catch (Exception e){
            return successful = false;
        } 

        return successful = true;
    }

    private static long blockHeightForFile(File file) {

        long height = -1;
        try {
            String filename = file.getName().replace("i_", "").replace(".cycledigest", "");
            height = Long.parseLong(filename);
        } catch (Exception ignored) { }

        return height;
    }
}
