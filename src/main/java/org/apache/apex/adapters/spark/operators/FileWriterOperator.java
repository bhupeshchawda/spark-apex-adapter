package org.apache.apex.adapters.spark.operators;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import org.apache.apex.adapters.spark.io.WriteToFS;

public class FileWriterOperator extends BaseOperator
{
    private String absoluteFilePath;
    public WriteToFS writeToFS;
    public FileWriterOperator()
    {
        writeToFS = new WriteToFS();
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
                writeToFS.write(absoluteFilePath, tuple);
                isSerialized=true;
            }
        }
    };

    public void setAbsoluteFilePath(String absoluteFilePath)
    {
        this.absoluteFilePath = absoluteFilePath;
    }
}
