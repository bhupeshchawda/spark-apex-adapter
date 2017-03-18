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

    public PathProperties() {
        try {
            String baseDir = System.getProperty("user.dir");
            String path = baseDir+"/src/main/resources/config.properties";
            InputStream input = new FileInputStream("/home/anurag/spark-apex-adapter/src/main/resources/config.properties");
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public  String getProperty(String s){
        return properties.getProperty(s);
    }
}
