package org.apache.apex.adapters.spark.operators;

import com.datatorrent.api.Operator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.io.Serializable;
import java.util.Stack;

/**
 * Created by anurag on 3/12/16.
 */

@DefaultSerializer(JavaSerializer.class)
public class SerializableDAG extends LogicalPlan implements Serializable{
    public SerializableDAG(){}

    public  String getLastOperatorName() {
        return lastOperatorName;
    }

    public  String getFirstOperatorName() {
        return firstOperatorName;
    }

    public  String lastOperatorName;
    public  String firstOperatorName;

    public Stack<String> stackName = new Stack<String>();

    @Override
    public <T extends Operator> T addOperator(String name, T operator) {
        stackName.push(name);
        lastOperatorName =name;
        firstOperatorName =stackName.firstElement();
        return super.addOperator(name,operator);
    }

}
