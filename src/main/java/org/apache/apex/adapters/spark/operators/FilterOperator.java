package org.apache.apex.adapters.spark.operators;

import com.datatorrent.api.Context;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;

import java.io.Serializable;
@DefaultSerializer(JavaSerializer.class)
public class FilterOperator<T> extends BaseOperatorSerializable implements Serializable
{
    int id=0;
    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        id=context.getId();
        log.info("Partition ID {}",id);
    }
    Logger log = LoggerFactory.getLogger(FilterOperator.class);
    public Function1 f;
  public final  DefaultInputPortSerializable<Object> input = new DefaultInputPortSerializable<Object>() {
    @Override
    public void process(Object tuple)
    {
      if((Boolean) f.apply(tuple)) {
        output.emit(tuple);
      }
    }
  };
  public final  DefaultOutputPortSerializable<Object> output = new DefaultOutputPortSerializable<Object>();

    @Override
    public DefaultInputPortSerializable<T> getInputPort() {
        return (DefaultInputPortSerializable<T>) this.input;
    }

    public DefaultOutputPortSerializable<Object> getOutputPort(){
    return this.output;
  }

  public DefaultInputPortSerializable getControlPort() {
    return null;
  }

  public DefaultOutputPortSerializable<Boolean> getControlOut() {
    return null;
  }

    public DefaultOutputPortSerializable<Integer> getCountOutputPort() {
        return null;
    }

}

