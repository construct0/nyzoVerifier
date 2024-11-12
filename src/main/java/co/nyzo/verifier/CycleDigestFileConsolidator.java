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

public class CycleDigestFileConsolidator {
    // While distinct, the run options align with the run options of the BlockFileConsolidator;
    // The consolidator has 3 run options:
    // - consolidate: create consolidated files and delete individual files (normal operation)
    // - delete: delete individual files only (do not create consolidated files)
    // - disable: do not run (do not create consolidated files, do not delete individual files)
    private static final String runOptionKey = "cycle_digest_file_consolidator";
    private static final String runOptionValueConsolidate = "consolidate";
    private static final String runOptionValueDeleteOnly = "delete";
    private static final String runOptionValueDisable = "disable";
    private static String runOption = PreferencesUtil.get(runOptionKey).toLowerCase(); // default is assigned in static constructor below

    static {
        // Ensure the value is explicitly set to one of the enumerated values. The default is "delete" for the verifier
        // and the sentinel. This conserves disk space, which reduces maintenance and improves robustness of these run
        // modes. The default is "delete" for the client as well. The client never participates in blockchain creation,
        // but is likely to store many or all blocks on-disk, which can result in an unnecessarily exuberant amount of disk space required.
        if (!runOption.equals(runOptionValueConsolidate) && !runOption.equals(runOptionValueDeleteOnly) && !runOption.equals(runOptionValueDisable)) {
            runOption = runOptionValueDeleteOnly;
        }
    }

    public static void consolidateCycleDigests(){
        // Consolidation & deletion is disabled
        if(runOption.equals(CycleDigestFileConsolidator.runOptionValueDisable)){
            return;
        }

        try {
            // Get all cycle digest files in the individual directory
            File[] individualFiles = CycleDigestManager.individualCycleDigestDirectory.listFiles(f -> f.getAbsolutePath().endsWith(".cycledigest"));

            // There are no individual cycle digests to consolidate
            if(individualFiles.length == 0){
                return;
            }

            // While distinct, the consolidation threshold aligns with the consolidation threshold of the BlockFileConsolidator;
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
                    long blockHeight = CycleDigestFileConsolidator.blockHeightForIndividualFile(file);
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

            // Process each file height
            for(Long fileHeight : fileMap.keySet()){
                boolean performDelete = false;
                
                if (!runOption.equals(runOptionValueDeleteOnly)) {
                    // The run option is neither disable or delete-only
                    // Consolidate & mark the files for deletion if successful
                    performDelete = CycleDigestFileConsolidator.consolidateFiles(fileHeight, fileMap.get(fileHeight));
                } else {
                    // If the delete-only option is set, skip consolidation
                    performDelete = true;
                }

                if(performDelete){
                    // The safeguard in the BlockFileConsolidator which prevents the deletion of block with height 0, isn't enforced for the cycle digest for block height 0
                    for(File file : fileMap.get(fileHeight)){
                        file.delete();
                    }
                }
            }
        } catch (Exception e){
            LogUtil.println("[CycleDigestFileConsolidator][consolidateCycleDigests]: " + e.getMessage() + "\r\n" + e.getStackTrace().toString());
        }
    }

    // Consolidates the given individual files into a consolidated file, whereby the consolidated file to write to is determined by the provided file height
    private static boolean consolidateFiles(long fileHeight, List<File> individualFiles) throws Exception {
        boolean successful = true;

        File consolidatedFile = CycleDigestManager.getConsolidatedCycleDigestFile(fileHeight);

        // Get the cycle digests residing in the existing consolidated file for this file height
        List<CycleDigest> cycleDigests = CycleDigestManager.loadCycleDigestsFromConsolidatedFile(consolidatedFile);

        // Add the cycle digests from the individual files
        for(File file : individualFiles){
            CycleDigest digest = CycleDigestManager.loadCycleDigestFromIndividualFile(file);

            if(digest == null){
                throw new Exception("While loading cycle digest from individual file " + file.getAbsolutePath() + " the resulting instance is null");
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

        // Check for block height gaps
        Long previousBlockHeight = -1L;
        for(int i=0; i<cycleDigests.size(); i++){
            if(previousBlockHeight == -1L){
                previousBlockHeight = cycleDigests.get(i).getBlockHeight();
                continue;
            }

            if(previousBlockHeight != cycleDigests.get(i).getBlockHeight() - 1){
                throw new Exception("An iterative block height gap greater than 1 is present in the list of cycle digests (pre-existing consolidated + additional individual), previous=" + previousBlockHeight + ", current=" + cycleDigests.get(i).getBlockHeight());
            }

            previousBlockHeight = cycleDigests.get(i).getBlockHeight();
        }

        // Write the consolidated file
        consolidatedFile.getParentFile().mkdirs();
        CycleDigestManager.writeCycleDigestsToConsolidatedFile(cycleDigests, consolidatedFile);

        return successful;
    }

    // Returns the block height associated with the given cycle digest file 
    private static long blockHeightForIndividualFile(File file) {

        long height = -1;
        try {
            String filename = file.getName().replace("i_", "").replace(".cycledigest", "");
            height = Long.parseLong(filename);
        } catch (Exception ignored) { }

        return height;
    }
}
