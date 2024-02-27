package co.nyzo.verifier;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import co.nyzo.verifier.util.IpUtil;
import co.nyzo.verifier.util.LogUtil;
import co.nyzo.verifier.web.elements.P;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

// todo/remarks 
// - parse the version strings from the existing messages and account for any subversions to be included, don't rely on version requests at this point since it's a 643 codebase
// - decide whether the init timestamp should be provided or present in accumulator and speaks for the entire instance, none the less all data dumps procedes should be initiated on launch, just a sidenote 
// - provide a simple version endpoint with version : {identifier: incycle boolean} map, if possible add the version string to the nodes dump as well
// - node health/status insight from a 3rd party perspective was very useful 
// - fix sd nodes, probably enough to import blocks, probably already 1 dropped (?)
// - data accumulator for "foreign" class initiated calls to which some logic is applied, data dumper for datadumper initiated calls to which some logic is applied
// -  perhaps some interplay with the consolidated blocks stored and a validator output pertaining to those files, incl shasum for each file for reference 
// - the consolidated block storage should be accompanied by the last 1000 individual blocks up until the point of consolidation (ideally)
// -  

public class DataDumper {
    public static final long dataDumperInitialisationTimestamp = System.currentTimeMillis();
    public static final File dataDumpDirectory = new File("/var/www/dumps");

    public static final File meshParticipantsFile = new File(DataDumper.dataDumpDirectory, "nodes.json");
    public static final File meshInCycleIdentifiers = new File(DataDumper.dataDumpDirectory, "incycleIdentifiers.json");
    public static final File meshVersionsFile = new File(DataDumper.dataDumpDirectory, "versions.json");

    private DataAccumulator _dataAccumulator = null;

    // private static Integer _c = 0;

    public DataDumper(){
        _dataAccumulator = new DataAccumulator();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    dump();
                } catch(Exception e) {
                    LogUtil.println("[DataDumper]: unexpected exception, retrying..");
                }
                
                try {
					Thread.sleep(5000); // this doesn't need to be further adjusted due to dump() running sequential; ergo self adjusting 
				} catch (InterruptedException e) {
                    LogUtil.println("[DataDumper]: Thread.sleep InterruptedException");
                }

                run();
            }
        }).start();
    }

    public static void dump(){
        LogUtil.println("[DataDumper][dump]: Starting dump...");


        if(!NodeManager.connectedToMeshAndReadyForDataDump()){
            LogUtil.println("[DataDumper][dump]: Skipping dump due to insufficient amount of data available");
            return;
        }

        Map<String, Map<String, Node>> meshParticipants = DataDumper.getMeshParticipants();
        _persist(meshParticipantsFile, meshParticipants, "");

        // A QOL endpoint, the meshParticipants map has the inCycle boolean for each ip address / node, making it unclear if none can be found for an identifier
        List<String> inCycleIdentifiers = DataDumper.getInCycleIdentifiers();
        _persist(meshInCycleIdentifiers, inCycleIdentifiers, "");

        KeyValuePair<DataAccumulatorPingVersionInfoResult, Set<DataAccumulatorPingVersionInfoResult>> availableVersionInfos = DataAccumulator.getAvailableVersionInfo();
        _persist(meshVersionsFile, availableVersionInfos, "");

        LogUtil.println("[DataDumper][dump]: Completed dump...");
    }


    public static List<String> getInCycleIdentifiers(){
        List<String> result = new ArrayList<>();

        List<ByteBuffer> verifiersInCurrentCycleList = BlockManager.verifiersInCurrentCycleList();

        for(ByteBuffer i : verifiersInCurrentCycleList){
            result.add(ByteUtil.arrayAsStringWithDashes(i.array()));
        }

        LogUtil.println("[DataDumper][getInCycleIdentifiers]: " + result.size() + " incycle identifiers");

        return result;
    }

    // version string : {ip address : node}
    // public static Map<String, Map<String, Node>> getMeshVersions() {

    // }

    // identifier : {ip address : node}
    public static Map<String, Map<String, Node>> getMeshParticipants() {
        Map<String, Map<String, Node>> result = new ConcurrentHashMap<>();

        List<ByteBuffer> verifiersInCurrentCycleList = BlockManager.verifiersInCurrentCycleList();

        // identifier byte[], is in cycle : identifier string with dashes
        Map<KeyValuePair<byte[], Boolean>, String> identifierMap = new ConcurrentHashMap<>();

        // incycle identifiers 
        Set<ByteBuffer> activeInCycleVerifiers = NodeManager.getActiveCycleIdentifiers();

        LogUtil.println("[DataDumper][getMeshParticipants]: " + activeInCycleVerifiers.size() + " active in cycle verifiers");
        
        activeInCycleVerifiers.forEach(i -> {
            KeyValuePair<byte[], Boolean> k = new KeyValuePair<byte[], Boolean>(i.array(), verifiersInCurrentCycleList.contains(i));

            identifierMap.put(
                k, 
                ByteUtil.arrayAsStringWithDashes(k.getKey())  
            );
        });

        LogUtil.println("[DataDumper][getMeshParticipants]: " + identifierMap.keySet().size() + " entries in identifier map");

        // missing incycle identifiers 
        Set<ByteBuffer> missingInCycleVerifiersSet = NodeManager.getMissingInCycleVerifiersSet();
        
        LogUtil.println("[DataDumper][getMeshParticipants]: " + missingInCycleVerifiersSet.size() + " missing in cycle verifiers");

        missingInCycleVerifiersSet.forEach(i -> {
            for(KeyValuePair<byte[], Boolean> k : identifierMap.keySet()){
                if(Arrays.equals(k.getKey(), i.array())){
                    return;
                }
            }

            KeyValuePair<byte[], Boolean> k = new KeyValuePair<byte[], Boolean>(i.array(), verifiersInCurrentCycleList.contains(i));

            identifierMap.put(
                k, 
                ByteUtil.arrayAsStringWithDashes(k.getKey())
            );
        });

        LogUtil.println("[DataDumper][getMeshParticipants]: " + identifierMap.keySet().size() + " entries in identifier map");

        // ip address node map
        Map<ByteBuffer, Node> ipAddressNodeMap = NodeManager.getIpAddressToNodeMap();
        
        LogUtil.println("[DataDumper][getMeshParticipants]: " + ipAddressNodeMap.keySet().size() + " entries in ip address node map");

        ipAddressNodeMap.values().forEach(n -> {
            for(KeyValuePair<byte[], Boolean> k : identifierMap.keySet()){
                if(Arrays.equals(k.getKey(), n.getIdentifier())) {
                    // we are aware of this identifier already
                    return;
                }
            }

            // the identifier was not present as active or missing incycle verifier, that means the identifier is not incycle 
            KeyValuePair<byte[], Boolean> k = new KeyValuePair<>(n.getIdentifier(), false);

            identifierMap.put(
                k,
                ByteUtil.arrayAsStringWithDashes(k.getKey())
            );
        });
        
        LogUtil.println("[DataDumper][getMeshParticipants]: " + identifierMap.keySet().size() + " entries in identifier map");

        // // new node ip to port map
        // Map<ByteBuffer, Integer> newNodeIpToPortMap = NodeManager.getNewNodeIpToPortMap();

        // LogUtil.println("[DataDumper][getMeshParticipants]: " + newNodeIpToPortMap.keySet().size() + " entries in new node ip to port queue map");

        // // node join request queue map
        // Map<ByteBuffer, Integer> nodeJoinRequestQueueMap = NodeManager.getNodeJoinRequestQueueMap();

        // LogUtil.println("[DataDumper][getMeshParticipants]: " + nodeJoinRequestQueueMap.keySet().size() + " entries in nodejoin requests queue map");

        // Map<ByteBuffer, Integer> dedupedQueueMap = _getDeduplicatedMap(newNodeIpToPortMap, nodeJoinRequestQueueMap);

        // LogUtil.println("[DataDumper][getMeshParticipants]: removed " + ((newNodeIpToPortMap.keySet().size() + nodeJoinRequestQueueMap.keySet().size()) - dedupedQueueMap.keySet().size()) + " duplicate keys to produce a deduped queue map");

        // creating the result
        // result.put("queue", new ConcurrentHashMap<String, Node>());

        // _c = 0;

        // dedupedQueueMap.keySet().forEach(k -> {
        //     for(ByteBuffer b : ipAddressNodeMap.keySet()) {
        //         if(_areByteBufferContentsEqual(k, b)) {
        //             _c++;
        //             return;
        //         }
        //     }

        //     // the node manager data does not provide the correct identifier, a placeholder identifier is used in order to be able to create a new node 
        //     // users of the data should refer to the new Node.identifierIsKnown boolean before using the value of the identifier 
        //     Node n = new Node(ByteUtil.byteArrayFromHexString("0000000000000000-0000000000000000-0000000000000000-0000000000000000", FieldByteSize.identifier), k.array(), 0, 0);
        //     n.setInCycle(false);

        //     result.get("queue").put(IpUtil.addressAsString(k.array()), n);
        // });

        // LogUtil.println("[DataDumper][getMeshParticipants]: skipped " + _c + " queue map entries due to them being present in the ip address node map");
        // _c = 0;

        identifierMap.keySet().forEach(i -> {
            // the string representation of the current identifier (value in the map)
            String iv = identifierMap.get(i);

            // ip address bytebuffer
            for(ByteBuffer k : ipAddressNodeMap.keySet()){
                // ip address node
                Node v = ipAddressNodeMap.get(k);
                

                // set incycle boolean
                v.setInCycle(i.getValue());

                // node identifier
                byte[] vi = v.getIdentifier();
                v.setNickname();

                // the kvp aligns with the identifier from the identifierMap
                if(Arrays.equals(i.getKey(), vi)) {
                    if(!result.keySet().contains(iv)) {
                        // there is no entry yet, we add a new kvp to the result
                        result.put(iv, new ConcurrentHashMap<String, Node>());
                    }

                    // we add the ip adress node map kvp to the result
                    result.get(iv).put(v.getIpAddressString(), v);
                }
            }

            // the identifier is known, but we don't have any nodes to include in the result
            if(!result.keySet().contains(iv)) {
                result.put(iv, new ConcurrentHashMap<>());
            }
        }); 

        // ret
        return result;
    }

    private static Map<ByteBuffer, Integer> _getDeduplicatedMap(Map<ByteBuffer, Integer> map1, Map<ByteBuffer, Integer> map2) {
        Map<ByteBuffer, Integer> deduplicatedMap = new ConcurrentHashMap<>();

        for (Map.Entry<ByteBuffer, Integer> entry : map1.entrySet()) {
            if (!_containsKeyWithSameContent(map2, entry.getKey())) {
                deduplicatedMap.put(entry.getKey(), entry.getValue());
            }
        }

        for (Map.Entry<ByteBuffer, Integer> entry : map2.entrySet()) {
            if (!_containsKeyWithSameContent(map1, entry.getKey())) {
                deduplicatedMap.put(entry.getKey(), entry.getValue());
            }
        }

        return deduplicatedMap;
    }

    private static boolean _containsKeyWithSameContent(Map<ByteBuffer, Integer> map, ByteBuffer key) {
        for (ByteBuffer mapKey : map.keySet()) {
            if (_areByteBufferContentsEqual(mapKey, key)) {
                return true;
            }
        }
        return false;
    }

    private static boolean _areByteBufferContentsEqual(ByteBuffer buffer1, ByteBuffer buffer2) {
        if (buffer1.remaining() != buffer2.remaining()) {
            return false;
        }

        int startPosition1 = buffer1.position();
        int startPosition2 = buffer2.position();

        while (buffer1.hasRemaining() && buffer2.hasRemaining()) {
            if (buffer1.get() != buffer2.get()) {
                return false;
            }
        }

        buffer1.position(startPosition1);
        buffer2.position(startPosition2);

        return true;
    }

    private static void _persist(File file, Object object, String message){
        ObjectMapper objectMapper = new ObjectMapper();
        DataDumpResult result = new DataDumpResult(object, message, dataDumperInitialisationTimestamp);

        try {
            _persist(file, objectMapper.writeValueAsString(result));
        } catch (JsonProcessingException e){
            LogUtil.println("[DataDumper][_persist]: failed to convert to json " + e.toString() + "\r\n" + e.getStackTrace());
        }
    }

    private static void _persist(File file, String json){
        File tempFile = new File(file.getAbsolutePath() + "_temp");
        tempFile.delete();
        BufferedWriter writer = null;

        try {
            writer = new BufferedWriter(new FileWriter(tempFile));
            writer.write(json);
        } catch (Exception e){
            LogUtil.println("[DataDumper][_persist]: Failed to write json to temp file with path " + tempFile.getAbsolutePath());
        }

        try {
            if(writer != null){
                writer.close();
            }
        } catch (Exception e){
            LogUtil.println("[DataDumper][_persist]: Failed to close file writer for temp file with path " + tempFile.getAbsolutePath());
        }

        try {
            Files.move(
                Paths.get(tempFile.getAbsolutePath()), 
                Paths.get(file.getAbsolutePath()), 
                StandardCopyOption.ATOMIC_MOVE, 
                StandardCopyOption.REPLACE_EXISTING
            );
        } catch (Exception e){
            LogUtil.println("[DataDumper][_persist]: Failed to move temp file to final file path " + file.getAbsolutePath());
        }
    }
}
