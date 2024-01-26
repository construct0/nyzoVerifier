package co.nyzo.verifier;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import co.nyzo.verifier.json.JsonRenderer;
import co.nyzo.verifier.util.LogUtil;

public class DataDumper {
    public static final File dataDumpDirectory = new File("/var/www/dumps");

    public static final File meshParticipantsFile = new File(DataDumper.dataDumpDirectory, "nodes");


    public static void dump(){
        if(!NodeManager.connectedToMesh()){
            LogUtil.println("[DataDumper][dump]: Skipping dump due to not being connected to the mesh yet");
            return;
        }

        Map<String, Map<ByteBuffer, Node>> meshParticipants = DataDumper.getMeshParticipants();
        
        _persist(meshParticipantsFile, meshParticipants);
    }

    // identifier : [ip address : node]
    public static Map<String, Map<ByteBuffer, Node>> getMeshParticipants() {
        Map<String, Map<ByteBuffer, Node>> result = new HashMap<>();

        // identifier byte[] : identifier string with dashes
        Map<byte[], String> identifierMap = new HashMap<>();

        // incycle identifiers 
        Set<ByteBuffer> activeInCycleVerifiers = NodeManager.getActiveCycleIdentifiers();
        
        activeInCycleVerifiers.forEach(i -> identifierMap.put(
            i.array(), 
            ByteUtil.arrayAsStringWithDashes(i.array())  
        ));

        // missing incycle identifiers 
        Set<ByteBuffer> missingInCycleVerifiersSet = NodeManager.getMissingInCycleVerifiersSet();
        
        missingInCycleVerifiersSet.forEach(i -> {
            for(byte[] k : identifierMap.keySet()){
                if(Arrays.equals(k, i.array())){
                    return;
                }
            }

            identifierMap.put(
                i.array(), 
                ByteUtil.arrayAsStringWithDashes(i.array())
            );
        });

        // ip address node map
        Map<ByteBuffer, Node> ipAddressNodeMap = NodeManager.getIpAddressToNodeMap();
        
        ipAddressNodeMap.values().forEach(n -> {
            for(byte[] k : identifierMap.keySet()){
                if(Arrays.equals(k, n.getIdentifier())) {
                    return;
                }
            
                identifierMap.put(
                    n.getIdentifier(),
                    ByteUtil.arrayAsStringWithDashes(n.getIdentifier())
                );
            }
        });
        

        // creating the result
        identifierMap.keySet().forEach(i -> {
            // the string representation of the current identifier (value in the map)
            String iv = identifierMap.get(i);

            // ip address bytebuffer
            for(ByteBuffer k : ipAddressNodeMap.keySet()){
                // ip address node
                Node v = ipAddressNodeMap.get(k);

                // node identifier
                byte[] vi = v.getIdentifier();

                // the kvp aligns with the identifier from the identifierMap
                if(Arrays.equals(i, vi)) {
                    if(!result.keySet().contains(iv)) {
                        // there is no entry yet, we add a new kvp to the result
                        result.put(iv, new HashMap<ByteBuffer, Node>());
                    }

                    // we add the ip adress node map kvp to the result
                    result.get(iv).put(k, v);
                }
            }
        }); 

        // ret
        return result;
    }

    private static void _persist(File file, Object object){
        _persist(file, JsonRenderer.toJson(object));
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
