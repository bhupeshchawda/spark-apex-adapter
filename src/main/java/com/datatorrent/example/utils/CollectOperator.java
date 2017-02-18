package com.datatorrent.example.utils;

import com.datatorrent.api.Context;
import com.datatorrent.common.util.BaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by anurag on 12/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class CollectOperator<T> extends BaseOperator implements Serializable {
    Logger log = LoggerFactory.getLogger(CollectOperator.class);
    public CollectOperator(){}
    public static ArrayList<Object> dataList;
    int count;
    boolean emitted;
    @Override
    public void beginWindow(long windowId) {
        count=0;

    }

    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple) {
            log.info("Type of tuple: {}, ",tuple.getClass());
            dataList.add(tuple);
            count++;
        }
    };

    @Override
    public void endWindow() {
        if(count==0 && !emitted){
            emitted=true;
            output.emit(dataList);
        }
    }

    public DefaultOutputPortSerializable output =new DefaultOutputPortSerializable<>();

    @Override
    public void setup(Context.OperatorContext context) {
        dataList=new ArrayList<>();
        emitted=false;
    }

}
