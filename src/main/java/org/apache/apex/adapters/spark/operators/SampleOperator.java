package org.apache.apex.adapters.spark.operators;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.io.Serializable;

/**
 * Created by anurag on 27/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class SampleOperator<T> extends BaseOperatorSerializable implements Serializable {
    public SampleOperator(){}
    public static  double fraction;
    public DefaultOutputPortSerializable<T> output= new DefaultOutputPortSerializable();
    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {
        @Override
        public void process(T tuple) {
            if(Math.random()<fraction){
                output.emit(tuple);
            }
        }
    };


    @Override
    public DefaultInputPortSerializable getInputPort() {
        return this.input;
    }

    @Override
    public DefaultOutputPortSerializable getOutputPort() {
        return this.output;
    }

    @Override
    public DefaultInputPortSerializable getControlPort() {
        return null;
    }

    @Override
    public DefaultOutputPortSerializable<Boolean> getControlOut() {
        return null;
    }
}
