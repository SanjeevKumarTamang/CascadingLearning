package implementations;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Created by sktamang on 10/15/15.
 */
public class RaiseCalculation extends BaseOperation implements Function {
    static Fields fieldOutput = new Fields(
            "name", "raiseAge");
    public RaiseCalculation(){
        super(fieldOutput);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry entry=functionCall.getArguments();
        System.out.println("entry ius----------"+entry);
    }
}
