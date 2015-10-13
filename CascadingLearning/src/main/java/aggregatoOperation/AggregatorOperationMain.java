package aggregatoOperation;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

/**
 * Created by sanjeev on 10/12/15.
 */
public class AggregatorOperationMain {

    public static void main(String[] args) {

        // define source and sink Taps.
        Scheme sourceScheme = new TextDelimited(new
                Fields("store_name", "product_name", "number_sold",
                "number_returned"), true, ",");

        Tap source = new FileTap(sourceScheme, args[0]);
        Scheme sinkScheme = new TextDelimited(new Fields(
                "product_name", "total_kept"));
        Tap sink = new FileTap(sinkScheme, args[1],
                SinkMode.REPLACE);

        // the 'head' of the pipe assembly
        Pipe assembly = new Pipe("total");

        //
        assembly = new GroupBy(assembly, new Fields("product_name"));
        assembly = new Every(assembly, new Fields("number_sold", "number_returned"), new SampleAggregator(new Fields("total_kept")), Fields.ALL);
        FlowConnector flowConnector = new LocalFlowConnector();
        Flow flow = flowConnector.connect(source, sink, assembly);
        flow.complete();
    }
}
