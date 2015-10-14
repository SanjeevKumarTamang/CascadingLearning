package functionOperation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Created by sanjeev on 10/12/15.
 */
public class SentFunc extends BaseOperation<Tuple> implements Function<Tuple> {
    static Fields fieldDeclaration = new Fields(
            "document", "text");
    static Fields fieldOutput = new Fields(
            "document", "sentnum", "sentence");

    public SentFunc() {
        super(2, fieldOutput);
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Tuple> call) {
// create a reusable Tuple of size 3
        call.setContext(Tuple.size(3));
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Tuple> call) {
        // get the arguments TupleEntry
        TupleEntry arguments = call.getArguments();
        String sentences[] = NLPUtils.getSentences
                (arguments.getString(1));
        int sentCounter = 0;
        for (String sent : sentences) {
        // get our previously created Tuple
            Tuple result = call.getContext();
        // First field is document name
            result.set(0, arguments.getString(0));
        // Second field is sentence number
            result.set(1, sentCounter);
        // Third field is the sentence
            result.set(2, sent);
        // return the result tuple
            call.getOutputCollector().add(result);
            sentCounter++;
            System.out.println("res = " + result.print());
        }
    }

    @Override
    public void cleanup( FlowProcess flowProcess,
                         OperationCall<Tuple> call ) {
        call.setContext( null );
    }
}
