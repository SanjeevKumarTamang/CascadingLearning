package implementations;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.FilterCall;
import cascading.tuple.Fields;
import cascading.operation.Filter;
import cascading.tuple.TupleEntry;

/**
 * Created by sktamang on 10/14/15.
 */
public class Raise extends BaseOperation implements Filter {
    String field;

    public Raise(String fieldTobeRemoved){
        this.field=fieldTobeRemoved;
    }

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
        TupleEntry tupleEntry=filterCall.getArguments();
        return tupleEntry.getString(5).contains(field);
    }
}
