package org.apache.apex.adapters.spark.operators;

import com.datatorrent.stram.client.StramAppLauncher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;

/**
 * Created by anurag on 12/1/17.
 */
public class ExtendedStramAppLauncher extends StramAppLauncher {
    public ExtendedStramAppLauncher(File appJarFile, Configuration conf) throws Exception {
        super(appJarFile, conf);
    }

    public ExtendedStramAppLauncher(FileSystem fs, Path path, Configuration conf) throws Exception {
        super(fs, path, conf);
    }

    public ExtendedStramAppLauncher(String name, Configuration conf) throws Exception {
        super(name, conf);
    }

    public ExtendedStramAppLauncher(FileSystem fs, Configuration conf) throws Exception {
        super(fs, conf);
    }
}
