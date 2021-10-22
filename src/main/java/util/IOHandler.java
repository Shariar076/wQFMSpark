package util;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author mahim
 */
public class IOHandler {

    public static void runSystemCommand(String cmd) {
        try {
            Process p = Runtime.getRuntime().exec(cmd);
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
            BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

            String s;

            while ((s = stdInput.readLine()) != null) {
                System.out.println(s);
            }

            while ((s = stdError.readLine()) != null) {
                System.out.println(s);
            }

        } catch (IOException ex) {
            System.out.println("Exception while running system command <" + cmd + "> ... Exiting.");
            System.exit(-1);
        }
    }

    // handles gene trees by producing weighted quartets
    public static void generateWeightedQuartets(String inputFileName, String outputFileName) {
//        String cmd = App.PYTHON_ENGINE + " " + Scripts.GENERATE_WQRTS + " " + inputFileName + " " + outputFileName;
        String cmd = "/home/himel/Documents/Academic/Kaggle/venv/bin/python" + " generate_wqrts.py " + inputFileName + " " + outputFileName;
        runSystemCommand(cmd);
    }

    public static void writeToFile(String tree, String outputfileName) {
        File file = new File(outputfileName);
        FileWriter fr = null;
        try {
            fr = new FileWriter(file);
            fr.write(tree);
        } catch (IOException e) {
            System.out.println("Error in writingFile to "
                    + outputfileName + ", [Helper.writeToFile]. Exiting system.");
            System.out.println("Tree:\n" + tree);
            System.exit(-1);
        } finally {
            //close resources
            try {
                fr.close();
            } catch (IOException e) {
                System.out.println("Error in closing file resource in [Helper.writeToFile]. to outputfile = " + outputfileName);
            }
        }
        System.out.println(">-> Successfully written to output-file " + outputfileName);
    }

    public static boolean checkAllValuesIFSame(Map<Integer, Boolean> map, boolean val) {
        if (map.isEmpty()) {
            return true;
        }
        return map.keySet().stream().noneMatch((key) -> (map.get(key) != val));
    }

    public static String getFinalTreeFromMap(String finalTree, Map<Integer, String> map_of_int_vs_str) {

        String decodedTree = "";
        for (int i = 0; i < finalTree.length(); i++) {
            char c = finalTree.charAt(i);
            if (c != '(' && c != ')' && c != ',' && c != ';') {
                String key = "";
                int j;
                for (j = i + 1; j < finalTree.length(); j++) {
                    char c1 = finalTree.charAt(j);
                    if (c1 == ')' || c1 == '(' || c1 == ',' || c1 == ';') {
                        break;
                    }
                }
                // System.out.println(j);
                key = finalTree.substring(i, j);
                // System.out.println("i: "+i+ " j: "+j);
                // System.out.println("Key: "+ key);
                String val = map_of_int_vs_str.get(Integer.parseInt(key.trim()));
                //System.out.println(val);
                decodedTree += val;
                i += (j - 1 - i);
            } else {
                decodedTree += c;
            }
            //  System.out.println(finalTree.charAt(i));

        }
//        for(int key: map_of_int_vs_str.keySet()){
//            System.out.println("<<REPLACING key=" + key + ", with val=" + map_of_int_vs_str.get(key) + ">>");
//            replaced = replaced.replace(String.valueOf(key), map_of_int_vs_str.get(key));
//        }
        return decodedTree;
    }

    public static boolean areEqualBipartition(Map<Integer, Integer> map1, Map<Integer, Integer> map2,
                                              int leftPartition, int rightPartition, int unassignedPartition) {

        // check if normally equal.
        if (map1.equals(map2)) {
            return true;
        }

        Map<Integer, Integer> newFinalMap = new HashMap<>();
        map2.keySet().forEach((key) -> {
            newFinalMap.put(key, (map2.get(key) == unassignedPartition) ? unassignedPartition : ((map2.get(key) == leftPartition) ? rightPartition : leftPartition)); // only non-zeros will be flipped
        });


//        System.out.println("COMPARING two maps function\nmap1 = " + map1 + "\nmap2 = " + map2 + "\nnewFinalMap = " + newFinalMap);


        return (map1.equals(newFinalMap));
    }
    public static int sumArray(int[] arr) {
        int sum = 0;
        for (int i = 0; i < arr.length; i++) {
            sum += arr[i];
        }
        return sum;
    }

    public static int sumMapValuesInteger(Map<Integer, Integer> mapInitialBip) {
        int sum = 0;
        sum = mapInitialBip.keySet().stream().map((key) -> mapInitialBip.get(key)).reduce(sum, Integer::sum);
        return sum;
    }
    private static String getDummyName(int x) {
        return "DUM_" + Integer.toString(x);
    }
    private static String getKeysWithSpecifiedValue(Map<Integer, Integer> map, int val, Map<Integer, String> reverse_mapping) {
        return map.keySet()
                .stream()
                .filter((t) -> {
                    return map.get(t) == val;
                })
                .map(x -> (reverse_mapping.get(x) == null) ? IOHandler.getDummyName(x) : reverse_mapping.get(x)) // x -> reverse_mapping.get(x)
                .collect(Collectors.joining(", "));
    }
    public static String getPartition(Map<Integer, Integer> partition_map,
                                      int left_partition, int right_partition,
                                      Map<Integer, String> reverse_mapping) {

        StringBuilder bld = new StringBuilder();

        bld
                .append("LEFT: ")
                .append(getKeysWithSpecifiedValue(partition_map, left_partition, reverse_mapping))
                .append("\n")
                .append("RIGHT: ")
                .append(getKeysWithSpecifiedValue(partition_map, right_partition, reverse_mapping))
                .append("\n");

        return bld.toString();
    }
    public static void printPartition(Map<Integer, Integer> partition_map,
                                      int left_partition, int right_partition,
                                      Map<Integer, String> reverse_mapping) {

        System.out.println(getPartition(partition_map, left_partition, right_partition, reverse_mapping));
    }
}

