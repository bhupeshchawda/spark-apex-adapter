package org.apache.apex.adapters.spark.operators;

import com.datatorrent.api.DefaultOutputPort;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.io.Serializable;

/**
 * Created by anurag on 2/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class DefaultOutputPortSerializable<U> extends DefaultOutputPort<U> implements Serializable {
}
