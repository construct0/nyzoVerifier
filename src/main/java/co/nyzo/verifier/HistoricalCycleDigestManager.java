package co.nyzo.verifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import co.nyzo.verifier.util.PreferencesUtil;
import co.nyzo.verifier.util.UpdateUtil;

// todo construct calls icm flags
public class HistoricalCycleDigestManager {
    public static final String startManagerKey = "start_historical_cycle_digest_manager";
    private static final boolean startManager = PreferencesUtil.getBoolean(startManagerKey, false);

    public static final String cycleDigestBuildThrottlingKey = "cycle_digest_build_throttling";
    private static final boolean cycleDigestBuildThrottling = PreferencesUtil.getBoolean(cycleDigestBuildThrottlingKey, true);

    private static final boolean historicalBlockManagerEnabled = HistoricalBlockManager.startManager;
    private static final boolean historicalBlockManagerThrottling = HistoricalBlockManager.offsetBuildThrottling;

    private static final AtomicBoolean alive = new AtomicBoolean(false);

    // cd /var/lib/nyzo/production/blocks/individual && ls -l -a | grep *.cycledigest
    public static void start(){

        // Start the manager if the preference flag indicates so. Starts from block 0 onwards until the frozen edge height, keeps tracking frozen edge height and creating cycle digests.
        if(startManager && !alive.getAndSet(true)){
            if(!historicalBlockManagerEnabled){
                System.out.println("[WARN][HistoricalCycleDigestManager]: the historical block manager is not enabled, ensure the necessary individual block files and/or consolidated block offset files are available on this system");
            }

            new Thread(new Runnable() {
                @Override
                public void run(){

                    while(!UpdateUtil.shouldTerminate()){
                        try {
                            Long[] sortedStoredCycleDigestHeights = getStoredCycleDigestHeights();
                            long latestAvailableCycleDigestHeight = getLatestAvailableCycleDigestHeight(sortedStoredCycleDigestHeights);
                        } catch (IOException ioException){

                        } catch (Exception e){

                        } finally {
                            if(cycleDigestBuildThrottling){

                            }
                        }


                        

                    }
                }
            });

        }
        
    }



    // Returns the last block height for which a cycle digest is stored, or the latest block height before an incremental gap has occurred between stored cycle digests
    protected static long getLatestAvailableCycleDigestHeight(Long[] storedCycleDigestHeights){
        if(storedCycleDigestHeights.length == 0){
            return -1L;
        }

        long first = storedCycleDigestHeights[0];
        long last = storedCycleDigestHeights[storedCycleDigestHeights.length - 1];

        // The cycle digests start off at block 0 and then move on forward, using the previous block as reference
        // Should this block not exist, all cycle digests are considered invalid
        if(first != 0L){
            return 0L;
        }

        // A lazy check for gaps
        boolean containsGaps = last > storedCycleDigestHeights.length;

        if(containsGaps){
            // A cycle digest file has been removed, now to identify the last height before the incremental sequence is first interrupted
            // Akin to the principle in regards to block 0 above, any and all cycle digests subsequent to the first interrupt/gap are considered invalid

            long c = -1L;

            for(long i : storedCycleDigestHeights){
                c++;
                if(c != i){
                    return c;
                }
            }

            return 0L;
        }

        return last;
    }

    protected static Long[] getStoredCycleDigestHeights() throws IOException {
        return Files.list(BlockManager.individualBlockDirectory.toPath())
                    .filter(file -> file.getFileName().endsWith("cycledigest"))
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .map(fn -> fn.replace("i_", "").replace(".cycleDigest", ""))
                    .map(heightString -> {
                        try {
                            return Long.parseLong(heightString);
                        } catch (Exception ignored) { 
                            return -1L;
                        }
                    })
                    .sorted()
                    .toArray(Long[]::new)
                    ;

    }

}
