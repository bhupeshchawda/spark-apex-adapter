package org.apache.apex.adapters.spark.operators;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import org.apache.apex.adapters.spark.io.WriteToFS;

import java.io.BufferedWriter;

//import org.apache.hadoop.fs.FileSystem;

public class FileWriterOperator extends BaseOperator
{
    private BufferedWriter bw;
    private String absoluteFilePath;
    public String successFilePath;
    public FileWriterOperator()
    {
    }

    @Override
    public void beginWindow(long windowId) {

    }

    @Override
    public void setup(OperatorContext context)
    {
        isSerialized =false;
    }
    private static boolean isSerialized;
    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
    {
        @Override
        public void process(Object tuple)
        {
            if(!isSerialized) {
                WriteToFS.write(absoluteFilePath, tuple);
                isSerialized=true;
            }
        }
    };

    @Override
    public void endWindow() {

    }
    public void setSuccessFilePath(String path){
        this.successFilePath=path;
    }
    public void setAbsoluteFilePath(String absoluteFilePath)
    {
        this.absoluteFilePath = absoluteFilePath;
    }
}
