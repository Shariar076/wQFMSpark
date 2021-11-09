package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import properties.ConfigValues;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class HDFSWriter {
    public static void writeToHDFS(ArrayList<String> data, String fileName) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", ConfigValues.HDFS);
        FileSystem fileSystem = FileSystem.get(configuration);
        Path hdfsWritePath = new Path("/user/"+ConfigValues.HDFS_USER+"/exception/" + fileName);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath,true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
        bufferedWriter.write(String.join("\n", data));
        bufferedWriter.newLine();
        bufferedWriter.close();
        fileSystem.close();
    }

    public static void writeToHDFS(String data, String fileName) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", ConfigValues.HDFS);
        FileSystem fileSystem = FileSystem.get(configuration);
        Path hdfsWritePath = new Path("/user/"+ConfigValues.HDFS_USER+"/exception/" + fileName);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath,true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
        bufferedWriter.write(data);
        bufferedWriter.newLine();
        bufferedWriter.close();
        fileSystem.close();
    }

    public static void appendToHDFS(String data, String fileName) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", ConfigValues.HDFS);
        FileSystem fileSystem = FileSystem.get(configuration);
        Path hdfsWritePath = new Path("/user/"+ConfigValues.HDFS_USER+"/exception/" + fileName);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath,true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
        bufferedWriter.write(data);
        bufferedWriter.newLine();
        bufferedWriter.close();
        fileSystem.close();
    }
}
