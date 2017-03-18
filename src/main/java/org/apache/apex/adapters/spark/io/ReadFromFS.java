package org.apache.apex.adapters.spark.io;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.exception.AlluxioException;
import org.apache.apex.adapters.spark.properties.PathProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

/**
 * Created by anurag on 24/2/17.
 */
public class ReadFromFS implements Serializable {

    public  String getAppBasePath () {
        PathProperties properties = new PathProperties();
        String fs = properties.getProperty("fs").toLowerCase();
        switch (fs) {
            case "alluxio":
                return properties.getProperty("alluxio_app_path");
            case "hdfs":
                return properties.getProperty("hdfs_app_path");
        }
        return null;
    }

    public  Object read(String path)  {
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
    public  boolean successFileExists(){
        PathProperties properties = new PathProperties();
        String fs = properties.getProperty("fs").toLowerCase();
        switch (fs){
            case "alluxio":
                return successFileAlluxio();
            case "local":
                return successFileLocal();
            case "hdfs":
                return successFileHDFS();
            default:
                return false;
        }
    }


    public  boolean successFileLocal() {
        return false;
    }
    public synchronized  boolean successFileAlluxio(){

        alluxio.client.file.FileSystem fs = alluxio.client.file.FileSystem.Factory.get();
        AlluxioURI pathURI=new AlluxioURI(getAppBasePath()+"/_SUCCESS");
        try {
            return fs.exists(pathURI);
        } catch (IOException | AlluxioException e) {
            throw new RuntimeException(e);
        }

    }
    public synchronized  boolean successFileHDFS() {

        Configuration conf = new Configuration(true);

        Path pt=new Path(getAppBasePath()+"/_SUCCESS");
        try {
            FileSystem hdfs = FileSystem.get(pt.toUri(), conf);
            if(hdfs.exists(pt)){
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
    public synchronized   Object readFromAlluxio(String path)  {
        try {
            alluxio.client.file.FileSystem fs = alluxio.client.file.FileSystem.Factory.get();
            AlluxioURI pathURI = new AlluxioURI(getAppBasePath()+"/"+path);
            FileInStream inStream = fs.openFile(pathURI);
            ObjectInputStream ois = new ObjectInputStream(inStream);
            return ois.readObject();
        }
        catch (IOException | AlluxioException | ClassNotFoundException e){
            throw new RuntimeException(e);
        }
    }

    public  Object readFromLocal(String path) {
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
    public synchronized Object readFromHDFS(String path){
        Configuration conf = new Configuration(true);
        Path pt=new Path(getAppBasePath()+"/"+path);
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
