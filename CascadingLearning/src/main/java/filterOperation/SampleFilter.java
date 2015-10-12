package filterOperation;

import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.flow.FlowProcess;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import java.io.File;


/**
 * Created by sktamang on 10/12/15.
 */
public class SampleFilter extends BaseOperation implements Filter {
    /* Note that fieldInputs not used. It is there just to
    show What the input looks like */
    static Fields fieldInputs = new Fields("line_num",
            "line");
    String filterWord = "";

    public SampleFilter(String word) { // expects 2 arguments, fail otherwise
        super(2);
        filterWord = word;
    }

    public boolean isRemove(FlowProcess flowProcess,
                            FilterCall call) { // get the arguments TupleEntry
        TupleEntry arguments = call.getArguments(); // filter out the current Tuple if the sentence contains a specific word
        return arguments.getString(1).contains(filterWord);
    }

}
