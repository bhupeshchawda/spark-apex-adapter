package org.apache.apex.adapters.spark.operators;

import com.datatorrent.lib.codec.KryoSerializableStreamCodec;

/**
 * Created by harsh on 12/12/16.
 */
public class ApexRDDStreamCodec extends KryoSerializableStreamCodec {
    @Override
    public int getPartition(Object o) {
        return o.hashCode();
    }
}
