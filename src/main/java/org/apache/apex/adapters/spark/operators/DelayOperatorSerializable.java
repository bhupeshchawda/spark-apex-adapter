package org.apache.apex.adapters.spark.operators;

import com.datatorrent.common.util.DefaultDelayOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.io.Serializable;

/**
 * Created by anurag on 10/1/17.
 */
@DefaultSerializer(JavaSerializer.class)
public class DelayOperatorSerializable<T> extends DefaultDelayOperator<T> implements Serializable {

    private static final long serialVersionUID = -2972537213450678368L;

    public final DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {

        private static final long serialVersionUID = 6830919916828325819L;

        @Override
        public void process(T tuple) {
            processTuple(tuple);
        }
    };

    public final DefaultOutputPortSerializable<T> output = new DefaultOutputPortSerializable<>();

    protected void processTuple(T tuple) {
        lastWindowTuples.add(tuple);
        output.emit(tuple);
    }

    @Override
    public void firstWindow() {
        for (T tuple : lastWindowTuples) {
            output.emit(tuple);
        }
    }
}