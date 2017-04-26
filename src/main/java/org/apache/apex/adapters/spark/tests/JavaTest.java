package org.apache.apex.adapters.spark.tests;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.exception.AlluxioException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by anurag on 10/1/17.
 */
public class JavaTest<T> {
    public T[] take() throws AlluxioException, IOException, ClassNotFoundException {
        ArrayList<T> array  = (ArrayList<T>) readFromAlluxio("/user/anurag/spark-apex/selectedData");
        return (T[]) array.toArray();
    }
    static class Person implements Serializable{
        int age;
        String name;

        public Person(String name, int age) {
            this.name=name;
            this.age=age;
        }


        public String getName() {
            return name;
        }

        public int getAge() {
            return age;
        }
    }
    public static void copyJars(String path) throws IOException {
        Pattern pattern =Pattern.compile("application.*");
        Matcher m =pattern.matcher(path);
        m.find();
        String oldId =m.group();
        Configuration conf = new Configuration();
        Path pt=new Path(path);
        FileSystem hdfs = FileSystem.get(pt.toUri(), conf);
        String s=path.replace(oldId,"application_1484098060070_0079");
        System.out.println(s+"\n"+path);
//        hdfs.rename()
    }

    public static void readFile(String path) throws IOException {
        Configuration conf = new Configuration();
        Path pt=new Path(path);
        FileSystem hdfs = FileSystem.get(pt.toUri(), conf);
        BufferedReader br=new BufferedReader(new InputStreamReader(hdfs.open(pt)));
        String line =null;
        System.out.println(br.readLine());
        System.out.println(br.readLine());
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

    public static void writeFileToAlluxio(String path,Object o) throws IOException, AlluxioException {
        alluxio.client.file.FileSystem fs = alluxio.client.file.FileSystem.Factory.get();
        AlluxioURI pathURI=new AlluxioURI(path);
        if(fs.exists(pathURI))
            fs.delete(pathURI);
        FileOutStream outStream = fs.createFile(pathURI);
//        DataOutputStream ds = new DataOutputStream(outStream);
        ObjectOutputStream oos = new ObjectOutputStream(outStream);
        oos.writeObject(o);
        oos.close();
        outStream.close();
    }
    public static Object readFromAlluxio(String path) throws IOException, AlluxioException, ClassNotFoundException {
        alluxio.client.file.FileSystem fs = alluxio.client.file.FileSystem.Factory.get();
        AlluxioURI pathURI=new AlluxioURI(path);
        FileInStream inStream = fs.openFile(pathURI);
        ObjectInputStream ois= new ObjectInputStream(inStream);
        return ois.readObject();
    }
    public static void main(String []args) throws IOException, AlluxioException, ClassNotFoundException {
//        Person p =new Person("Anurag",22);
//        writeFileToAlluxio("/user/anurag/p.ser",p);
//
//        String path ="datatorrent/apps/application_1484098060070_0078";
////        copyJars("hdfs://localhost:54310/user/anurag/datatorrent/apps/application_1484098060070_0078");
//        readFile("hdfs://localhost:54310/user/anurag/diabetes.txt");
        String path = System.getProperty("user.dir")+"/"+"path.properties";
        System.out.println(path);
    }
}
