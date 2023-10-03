/*
 * the MapReduce functionality implemented in this program takes a single large text file to map i.e. split it into small chunks and then assign 1 to all the found words
 * then reduces by adding count values to each unique words
 */

package io.grpc.filesystem.task2;

import java.util.*;
import java.util.stream.Collectors;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Map.Entry;

import io.grpc.filesystem.task2.Mapper;

public class MapReduce {

    public static String makeChunks(String inputFilePath) throws IOException {
        int count = 1;
        int size = 500;
        File f = new File(inputFilePath);
        try (BufferedReader br = new BufferedReader(new FileReader(inputFilePath))) {
            String l = br.readLine();

            while (l != null) {
                File newFile = new File(f.getParent() + "/temp", "chunk"
                        + String.format("%03d", count++) + ".txt");
                try (OutputStream out = new BufferedOutputStream(new FileOutputStream(newFile))) {
                    int fileSize = 0;
                    while (l != null) {
                        byte[] bytes = (l + System.lineSeparator()).getBytes(Charset.defaultCharset());
                        if (fileSize + bytes.length > size)
                            break;
                        out.write(bytes);
                        fileSize += bytes.length;
                        l = br.readLine();
                    }
                }
            }
        }
        return f.getParent() + "/temp";

    }

    /**
     * @param inputfilepath
     * @throws IOException
     */
    public static void map(String inputfilepath) throws IOException {

        File f = new File(inputfilepath);
        String content = "";
        BufferedReader br = new BufferedReader(new FileReader(inputfilepath));
        String l = br.readLine();

        while (l != null) {
            content += " " + l;
            l = br.readLine();
        }
        content = content.trim();
        content = content.replaceAll("\\p{Punct}", "");
        content = content.replaceAll("[^a-zA-Z0-9\\s]", "");
        String[] splitContent = content.split(" ");

        File outputFile = new File(f.getParent() + "/map", "map-" + f.getName());
        FileWriter out = new FileWriter(outputFile, true);
        FileWriter clear = new FileWriter(outputFile, false);
        clear.write("");

            for (String w : splitContent) {
                if (!w.isEmpty() && !w.equals(" "))
                    out.write(w + ":1 ");
            }


        out.close();
        br.close();


    }


    /*
     * Insert your code here
     * Take a chunk and filter words (you could use "\\p{Punct}" for filtering punctuations and "^[a-zA-Z0-9]"
     * together for filtering the words), then split the sentences to take out words and assign "1" as the initial count.
     * Use the given mapper class to create the unsorted key-value pair.
     * Save the map output in a file named "map-chunk001", for example, in folder
     * path input/temp/map
     */

    /**
     * @param inputfilepath
     * @param outputfilepath
     * @return
     * @throws IOException
     */
    public static void reduce(String inputfilepath, String outputfilepath) throws IOException {
        System.out.println("Starting reduce");

        TreeMap<String, Integer> table = new TreeMap<>();

        File dir = new File(inputfilepath + "/map");
        File[] directoyListing = dir.listFiles();
        if (directoyListing != null) {
            for (File f : directoyListing) {
                if (f.isFile()) {

                    String content = "";
                    BufferedReader br = new BufferedReader(new FileReader(f));
                    String l = br.readLine();

                    while (l != null) {
                        content += " " + l;
                        l = br.readLine();
                    }

                    br.close();
                    String[] splitContent = content.split(" ");
                    for (String w : splitContent) {
                        if (w.length() != 0 && !w.equals(" ") && !w.equals(null)) {
                            String[] item = w.split(":");
                            String word = item[0].toLowerCase();
                            if (table.containsKey(word)) {
                                int value = table.get(word) + 1;
                                table.put(word, value);
                            } else {
                                table.put(word, 1);

                            }
                        }
                    }
                }
            }
        }

        //I generated this comparator using ChatGPT
        Comparator<Map.Entry<String, Integer>> valueComparator = new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> entry1, Map.Entry<String, Integer> entry2) {
                return entry2.getValue().compareTo(entry1.getValue()); // Compare values in descending order
            }
        };

        // Create a list of map entries
        List<Map.Entry<String, Integer>> entryList = new ArrayList<>(table.entrySet());

        // Sort the list of entries based on values using the custom comparator
        Collections.sort(entryList, valueComparator);


        File outputFile = new File(outputfilepath);
        System.out.println("Creating file: " + outputFile.getName());
        FileWriter fw = new FileWriter(outputFile, false);
        fw.write("");
        System.out.println("File clear done");
        FileWriter fwa = new FileWriter(outputFile, true);

        for (Map.Entry<String, Integer> entry : entryList) {
            fwa.write(entry.getKey() + ":" + entry.getValue() + "\n" );
        }

        fw.close();
        fwa.close();
    }





        /*
             * Insert your code here
             * Take all the files in the map folder and reduce them to one file that shows
             * unique words with their counts as "the:64", for example.
             * Save the output of reduce function as output-task2.txt
             */




        /**
         * Takes a text file as an input and returns counts of each word in a text file
         * "output-task2.txt"
         *
         * @param args
         * @throws IOException
         */
        public static void main (String[]args) throws IOException { // update the main function if required
            String inputFilePath = args[0];
            String outputFilePath = args[1];
            String chunkpath = makeChunks(inputFilePath);
            File dir = new File(chunkpath);
            File[] directoyListing = dir.listFiles();
            if (directoyListing != null) {
                for (File f : directoyListing) {
                    if (f.isFile()) {

                        map(f.getPath());

                    }

                }

                reduce(chunkpath, outputFilePath);

            }

        }
    }