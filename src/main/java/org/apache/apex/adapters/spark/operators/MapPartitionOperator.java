package org.apache.apex.adapters.spark.operators;

import com.datatorrent.api.Context;
import org.apache.apex.adapters.spark.MyBaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.spark.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.collection.Iterator;
import scala.collection.JavaConversions;

import java.io.Serializable;
import java.util.ArrayList;

@DefaultSerializer(JavaSerializer.class)
public class MapPartitionOperator<T,U> extends MyBaseOperator implements Serializable {
    int id=0;
    ArrayList<T> rddData = new ArrayList<>();
    public TaskContext taskContext;
    public Object object;
    public boolean emitted;
    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        id=context.getId();
        emitted=false;
    }
    int count=0;
    @Override
    public void beginWindow(long windowId) {
        count=0;

    }

    Logger log = LoggerFactory.getLogger(MapPartitionOperator.class);
    public Function1<Iterator<T>, Iterator<U>> f;
    public DefaultOutputPortSerializable<U> output = new DefaultOutputPortSerializable();
    public DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {

        @Override
        public void process(T tuple) {
            count++;
            try {
                rddData.add(tuple);

            } catch ( Exception e){
                log.debug("Exception Occured Due to {} ",tuple.getClass());
                e.printStackTrace();
//                output.emit(tuple);
            }
        }
    };

    @Override
    public void endWindow() {
        if(count==0 && !emitted){
            Iterator<U> result = f.apply(JavaConversions.asScalaIterator(rddData.listIterator()));
            while (result.hasNext()) {
                output.emit(result.next());
            }
            emitted=true;
        }

    }

    @Override
    public DefaultInputPortSerializable<T> getInputPort() {
        return null;
    }

    public DefaultOutputPortSerializable getOutputPort() {
        return this.output;
    }

    public DefaultInputPortSerializable getControlPort() {
        return null;
    }

    public DefaultOutputPortSerializable<Boolean> getControlOut() {
        return null;
    }


    public boolean isInputPortOpen = true;
    public boolean isOutputPortOpen = true;
}
