package org.apache.apex.adapters.spark.properties;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by anurag on 24/2/17.
 */
public class PathProperties {
    public static Properties properties = new Properties();

    public  void load(String prop_path) {
        try {
            String baseDir = System.getProperty("user.dir");
            String path = baseDir+"/src/main/java/org/apache/apex/adapters/spark/"+prop_path;
            InputStream input = new FileInputStream(path);
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public  String getProperty(String s){
        return properties.getProperty(s);
    }
}
