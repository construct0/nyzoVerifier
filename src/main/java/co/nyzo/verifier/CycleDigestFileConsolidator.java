package co.nyzo.verifier;

import java.io.File;
import java.util.List;

import co.nyzo.verifier.util.LogUtil;
import co.nyzo.verifier.util.PreferencesUtil;
import co.nyzo.verifier.util.UpdateUtil;

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

    public static final int runEverySeconds = 300;

    public static void start(){
        // Ensure the value is explicitly set to one of the enumerated values. The default is "delete" for the verifier
        // and the sentinel. This conserves disk space, which reduces maintenance and improves robustness of these run
        // modes. The default is "consolidate" for the client. The client never participates in blockchain creation,
        // which weighs its emphasis more toward utility.
        if (!runOption.equals(runOptionValueConsolidate) && !runOption.equals(runOptionValueDeleteOnly) &&
                !runOption.equals(runOptionValueDisable)) {
            if (RunMode.getRunMode() == RunMode.Client) {
                runOption = runOptionValueConsolidate;
            } else {
                runOption = runOptionValueDeleteOnly;
            }
        }
        LogUtil.println("CycleDigestFileConsolidator setting: " + runOptionKey + "=" + runOption);
    
        if(!runOption.equals(runOptionValueDisable)){
            new Thread(new Runnable(){
                @Override
                public void run(){
                    consolidateCycleDigests();

                    while(!UpdateUtil.shouldTerminate()){
                        try {
                            // Sleep for 5 minutes (300 seconds) in 3-second intervals
                            for(int i=0; i< (runEverySeconds / 3) && !UpdateUtil.shouldTerminate(); i++){
                                Thread.sleep(runEverySeconds * 10);
                            }
                        } catch (Exception e){

                        } finally {
                            consolidateCycleDigests();
                        }
                    }
                }
            })
        }
    }

    private static void consolidateCycleDigests(){
        try {
            // Get all cycle digest files in the individual directory
            File[] individualFiles = CycleDigestManager.individualCycleDigestDirectory.listFiles(f -> f.getAbsolutePath().endsWith("cycledigest"));

            

        } catch (Exception e){

        }
    }

    public static synchronized writeCycleDigestsToConsolidatedFile(List<CycleDigest> cycleDigests, File file){

    }
}
