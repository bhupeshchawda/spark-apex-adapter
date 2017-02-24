package org.apache.apex.adapters.spark.io;

import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import org.apache.apex.adapters.spark.properties.PathProperties;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Created by anurag on 24/2/17.
 */
public class WriteToFS {
    public static void write(String path, Object o){
        PathProperties properties = new PathProperties();
        String fs =properties.getProperty("fs").toLowerCase();

        switch (fs){
            case "alluxio":
                writeFileToAlluxio(path,o);
                break;
            case "local":
                writeFileToLocal(path, o);
                break;
            case "hdfs":
                writeFileToHDFS(path, o);
                break;
        }
    }
    public static synchronized void createSuccessFile() {
        FileSystem fs = FileSystem.Factory.get();
        AlluxioURI pathURI = new AlluxioURI("/user/anurag/spark-apex/_SUCCESS");
        try {
            FileOutStream outStream = fs.createFile(pathURI);
            DataOutputStream ds = new DataOutputStream(outStream);
            ds.write("Success File Created".getBytes());
            ds.flush();
            ds.close();
            outStream.flush();
            outStream.close();
        }
        catch (AlluxioException | IOException e) {
            e.printStackTrace();
        }

    }
    public static synchronized  void writeFileToAlluxio(String path,Object o) {
        try {
            FileSystem fs = FileSystem.Factory.get();
            AlluxioURI pathURI = new AlluxioURI("/user/anurag/spark-apex/" + path);
            if (fs.exists(pathURI))
                fs.delete(pathURI);
            FileOutStream outStream = fs.createFile(pathURI);
            ObjectOutputStream oos = new ObjectOutputStream(outStream);
            oos.writeObject(o);
            oos.flush();
            oos.close();
            outStream.flush();
            outStream.close();
            createSuccessFile();
        }
        catch (AlluxioException | IOException e) {
            throw new RuntimeException(e);
        }
    }
    public synchronized static void writeFileToHDFS(String path,Object o){
    }
    public synchronized  static void writeFileToLocal(String path,Object o) {
        try {
            FileOutputStream fos = new FileOutputStream(path);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(o);
            oos.flush();
            oos.close();

            FileOutputStream fileWriter = new FileOutputStream("/tmp/spark-apex/_SUCCESS");
            fileWriter.write("Writing Data to file".getBytes());
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
