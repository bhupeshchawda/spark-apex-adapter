package org.apache.apex.adapters.spark;

import com.datatorrent.common.util.BaseOperator;
import org.apache.apex.adapters.spark.operators.DefaultInputPortSerializable;
import org.apache.apex.adapters.spark.operators.DefaultOutputPortSerializable;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.io.Serializable;

/**
 * Created by harsh on 2/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public abstract class MyBaseOperator<T> extends BaseOperator implements  Serializable{
    public MyBaseOperator(){

    }
    public abstract DefaultInputPortSerializable<T> getInputPort();
    public abstract DefaultOutputPortSerializable getOutputPort();
    public  abstract DefaultInputPortSerializable getControlPort();
    public  abstract DefaultOutputPortSerializable<Boolean> getControlOut();


}
