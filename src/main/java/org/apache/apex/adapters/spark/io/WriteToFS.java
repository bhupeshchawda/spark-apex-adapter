package org.apache.apex.adapters.spark.io;

import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import org.apache.apex.adapters.spark.properties.PathProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.*;

/**
 * Created by anurag on 24/2/17.
 */
public class WriteToFS implements Serializable{
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

    public synchronized void write(String path, Object o){
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
    public  void deleteSUCCESSFile(){
        PathProperties properties = new PathProperties();
        String fs =properties.getProperty("fs").toLowerCase();

        switch (fs){
            case "alluxio":
                 deleteSUCCESSFileAlluxio();
                break;
            case "local":
                deleteSUCCESSFileLocal();
                break;
            case "hdfs":
                deleteSUCCESSFileHDFS();
                break;
            default:return ;
        }
    }

    public synchronized  void deleteSUCCESSFileLocal() {
    }



    public  synchronized void createSuccessFileAlluxio() {
        FileSystem fs = FileSystem.Factory.get();
        AlluxioURI pathURI = new AlluxioURI(getAppBasePath()+"/_SUCCESS");
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

    public  synchronized void createHDFSSuccessFile() {
        Configuration configuration = new Configuration(true);
        try {
            Path file = new Path(getAppBasePath()+"/_SUCCESS");
            org.apache.hadoop.fs.FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(configuration);
            if (hdfs.exists(file)) {
                hdfs.delete(file, true);
            }
            hdfs.create(file);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public  synchronized  void writeFileToAlluxio(String path,Object o) {
        try {
            FileSystem fs = FileSystem.Factory.get();
            AlluxioURI pathURI = new AlluxioURI(getAppBasePath()+"/" + path);
            if (fs.exists(pathURI))
                fs.delete(pathURI);
            FileOutStream outStream = fs.createFile(pathURI);
            ObjectOutputStream oos = new ObjectOutputStream(outStream);
            oos.writeObject(o);
            oos.flush();
            oos.close();
            outStream.flush();
            outStream.close();
            createSuccessFileAlluxio();
        }
        catch (AlluxioException | IOException e) {
            throw new RuntimeException(e);
        }
    }


    public synchronized  void writeFileToHDFS(String path,Object o){
        Configuration configuration = new Configuration();
        try {
            Path file = new Path(getAppBasePath()+"/"+path);
            org.apache.hadoop.fs.FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(file.toUri(),configuration);
            if (hdfs.exists(file)) {
                hdfs.delete(file, false);
            }
            OutputStream os = hdfs.create(file);
            ObjectOutputStream oos=new ObjectOutputStream(os);
            oos.writeObject(o);
            oos.flush();
            oos.close();
            createHDFSSuccessFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized   void writeFileToLocal(String path,Object o) {
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
    public synchronized  void deleteSUCCESSFileAlluxio() {
        try {
            alluxio.client.file.FileSystem fs = alluxio.client.file.FileSystem.Factory.get();
            AlluxioURI pathURI=new AlluxioURI(getAppBasePath()+"/_SUCCESS");
            if(fs.exists(pathURI)) fs.delete(pathURI);

        } catch (IOException | AlluxioException e) {
            e.printStackTrace();
        }

    }
    public synchronized  void deleteSUCCESSFileHDFS() {
        Configuration conf = new Configuration(true);
        Path pt=new Path(getAppBasePath()+"/_SUCCESS");
        try {
            org.apache.hadoop.fs.FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(pt.toUri(),conf);
            if(hdfs.exists(pt)){
                hdfs.delete(pt,true);

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
