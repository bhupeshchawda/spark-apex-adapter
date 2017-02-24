package org.apache.apex.adapters.spark.operators;

import com.datatorrent.api.Context;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by harsh on 21/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class CountByValueOperatorSerializable<K,V> extends BaseOperatorSerializable implements Serializable {
    private boolean done = false;

    public CountByValueOperatorSerializable() {

    }
    @Override
    public void beginWindow(long windowId) {
        super.beginWindow(windowId);
        if(done)
        {
            log.info("control done is here");
            output.emit(hashMap);
        }
    }

    public final DefaultInputPortSerializable<Boolean> controlDone = new DefaultInputPortSerializable<Boolean>() {
        @Override
        public void process(Boolean tuple)
        {
            log.info("control done is true");
            done = true;
        }
    };

    Logger log = LoggerFactory.getLogger(CountByValueOperatorSerializable.class);
    public static transient HashMap<Object, Long> hashMap;
    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        hashMap= new HashMap<>();
    }
    public DefaultInputPortSerializable input = new DefaultInputPortSerializable() {
        @Override
        public void process(Object tuple) {
            {
                if(hashMap.containsKey(tuple)) {
                    long x= hashMap.get(tuple).longValue();
                    hashMap.put(tuple, new Long(x+1));
                }
                else {
                    hashMap.put(tuple, (long) 1.0);
                }
            }
        }
    };

    @Override
    public void endWindow() {
        super.endWindow();
    }

    public DefaultOutputPortSerializable<HashMap> output = new DefaultOutputPortSerializable<HashMap>();
    @Override
    public DefaultInputPortSerializable<Object> getInputPort() {
        return input;
    }



    @Override
    public DefaultOutputPortSerializable getOutputPort() {
        return output;
    }

    @Override
    public DefaultInputPortSerializable getControlPort() {
        return controlDone;
    }

    @Override
    public DefaultOutputPortSerializable<Boolean> getControlOut() {
        return null;
    }
}
