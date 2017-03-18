package org.apache.apex.adapters.spark.operators;

import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by harsh on 25/2/17.
 */
@DefaultSerializer(JavaSerializer.class)
public class InputSplitOperator<T> extends BaseOperatorSerializable<T> implements InputOperator,Serializable {

    public String path;
    public boolean shutApp=false;
    public String appName="";
    public static int minPartitions;
    public transient InputSplit splits[];
    public transient FileInputFormat fileInputFormat;
    public transient Configuration conf;
    public transient RecordReader recordReader;
    public transient JobConf jobConf;
    public transient LongWritable longWritable;
    public transient Text text;
    private int operatorId;
    public boolean sent=false;

    public InputSplitOperator(){}

    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        operatorId = context.getId();
        try{
            conf = new Configuration(true);
            jobConf = new JobConf(conf);
            fileInputFormat = new TextInputFormat();
            ((TextInputFormat)fileInputFormat).configure(jobConf);
            text = new Text();
            longWritable = new LongWritable();
            splits=splitFileRecorder(path,minPartitions);
            recordReader = fileInputFormat.getRecordReader(splits[operatorId-1],jobConf,Reporter.NULL);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    public void emitTuples() {

        try {
            if(recordReader.next(longWritable,text)) {
                output.emit(text.toString());
            }
            else {
                sent=true;
            }
        }
        catch (Exception o){

        }
    }
    public InputSplit[] splitFileRecorder(String path, int minPartitions){
        FileInputFormat fileInputFormat = new TextInputFormat();
        ((TextInputFormat)fileInputFormat).configure(jobConf);
        fileInputFormat.addInputPaths(jobConf,path);
        try {
            return  fileInputFormat.getSplits(jobConf,minPartitions);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void endWindow() {
        super.endWindow();
        /*if(shutApp) {
            shutApp = false;
            try {
                if(checkSucess("hdfs://localhost:54310/harsh/chi/success/Chi"+appName+"Success"))
                    throw new ShutdownException();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }*/
    }


    @Override
    public void beginWindow(long windowId) {
        super.beginWindow(windowId);
        if(sent) {
            controlOut.emit(true);
            shutApp=true;
        }
    }

/*
    public boolean checkSucess(String path) throws IOException {
        Path pt=new Path(path);
        FileSystem hdfs = FileSystem.get(pt.toUri(), conf);
        if(hdfs.exists(pt))
        {
            //hdfs.delete(pt,false);
            return true;
        }
        else
            return false;

    }
*/


    public final  DefaultOutputPortSerializable<Object> output = new DefaultOutputPortSerializable<Object>();
    public final  DefaultOutputPortSerializable<Boolean> controlOut = new DefaultOutputPortSerializable<Boolean>();
    @Override
    public DefaultInputPortSerializable getInputPort() {
        return null;
    }

    @Override
    public DefaultOutputPortSerializable getOutputPort() {
        return output;
    }

    @Override
    public DefaultInputPortSerializable getControlPort() {
        return null;
    }

    @Override
    public DefaultOutputPortSerializable<Boolean> getControlOut() {
        return controlOut;
    }
}
