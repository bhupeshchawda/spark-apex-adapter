package org.apache.apex.adapters.spark.io;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.exception.AlluxioException;
import org.apache.apex.adapters.spark.properties.PathProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

/**
 * Created by anurag on 24/2/17.
 */
public class ReadFromFS {
    public static Object read(String path)  {
        PathProperties properties = new PathProperties();
        String fs = properties.getProperty("fs").toLowerCase();
        switch (fs){
            case "alluxio":
                return readFromAlluxio(path);
            case "local":
                return readFromLocal(path);
            case "hdfs":
                return readFromHDFS(path);
        }
        return null;
    }
    public synchronized  static Object readFromAlluxio(String path)  {
        try {
            alluxio.client.file.FileSystem fs = alluxio.client.file.FileSystem.Factory.get();
            AlluxioURI pathURI = new AlluxioURI("/user/anurag/spark-apex/"+path);
            FileInStream inStream = fs.openFile(pathURI);
            ObjectInputStream ois = new ObjectInputStream(inStream);
            return ois.readObject();
        }
        catch (IOException | AlluxioException | ClassNotFoundException e){
            throw new RuntimeException(e);
        }
    }
    public static Object readFromLocal(String path) {
        try {
            FileInputStream fis = new FileInputStream(path);
            ObjectInputStream ois = new ObjectInputStream(fis);
            Object result = ois.readObject();
            ois.close();
            return result;
        }
        catch (IOException |ClassNotFoundException e){
            throw new RuntimeException(e);
        }

    }
    public static Object readFromHDFS(String path){
        Configuration conf = new Configuration();
        Path pt=new Path(path);
        try {
            FileSystem hdfs = FileSystem.get(pt.toUri(), conf);
            InputStream inputStream = hdfs.open(pt);
            ObjectInputStream ois= new ObjectInputStream(inputStream);
            return ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

    }
}
