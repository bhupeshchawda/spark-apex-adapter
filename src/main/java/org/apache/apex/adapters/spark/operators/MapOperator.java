package org.apache.apex.adapters.spark.operators;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

import scala.Function1;

public class MapOperator extends BaseOperator
{
  public Function1 f;
  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object tuple)
    {
      output.emit(f.apply(tuple));
    }
  };
  public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<>();

}
