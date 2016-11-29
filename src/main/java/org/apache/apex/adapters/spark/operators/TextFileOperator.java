package org.apache.apex.adapters.spark.operators;

import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

public class TextFileOperator extends BaseOperator implements InputOperator
{
  private long tuplesInWindow = 0;
  private String path;

  @Override
  public void beginWindow(long windowId)
  {
    tuplesInWindow = 0;
  }

  @Override
  public void emitTuples()
  {

  }

}
