package aggregatoOperation;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Created by sanjeev on 10/12/15.
 */
public class SampleAggregator extends BaseOperation<SampleAggregator.Context> implements Aggregator<SampleAggregator.Context> {

    public SampleAggregator()
    {// expects 2 argument, fail otherwise
        super(2, new Fields("total_kept"));
    }
    public SampleAggregator( Fields calcFields )
    {// expects 2 argument, fail otherwise
        super( 2, calcFields );
    }

    @Override
    public void start(FlowProcess flowProcess, AggregatorCall<SampleAggregator.Context> aggregatorCall) {
        aggregatorCall.setContext(new Context());
    }

    @Override
    public void aggregate(FlowProcess flowProcess, AggregatorCall<SampleAggregator.Context> aggregatorCall) {
        TupleEntry arguments = aggregatorCall.getArguments();
        Context context = aggregatorCall.getContext();
        // add the current argument value to the current sum
        context.value += (arguments.getInteger(0)-
                arguments.getInteger(1));
    }

    @Override
    public void complete(FlowProcess flowProcess, AggregatorCall<SampleAggregator.Context> aggregatorCall) {
        Context context = aggregatorCall.getContext();
        // create a Tuple to hold our result values
        Tuple result = new Tuple();
        // set the sum
        result.add( context.value );
        // return the result Tuple
        aggregatorCall.getOutputCollector().add( result );

    }

    public class Context
    {
        long value = 0;
    }
}
