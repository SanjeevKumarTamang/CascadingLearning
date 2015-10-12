import cascading.pipe.CoGroup;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.flow.FlowDef;
import cascading.flow.Flow;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Debug;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.InnerJoin;


/**
 * Created by sktamang on 10/12/15.
 */
public class Application {
    public static void main(String[] args) {
//            FilterOperationMain.main(new String[]{"a","b"});
//        Note that source1Scheme represents the tuple layout
//        of the first file, taking into account that the file has a
//        header (true as the second parameter) and "," as the
//        delimiter (third parameter). Similarly, source2Scheme
//        represents the tuple layout of the second file.
        Scheme source1Scheme = new TextDelimited(new Fields("author", "organization", "document", "keyword"), true, ",");
        Scheme source2Scheme = new TextDelimited(new Fields("department", "employee"), true, ",");

        String file1 ="authorInfo";
        String file2 = "employeeDepartment";

        Tap source1 = new FileTap(source1Scheme, file1);
        Tap source2 = new FileTap(source2Scheme, file2);

        String outputFile ="output";
        Scheme sinkScheme = new TextDelimited(new Fields(
                "organization", "department", "employee", "document",
                "keyword"), true, ",");

        Tap sink = new FileTap(sinkScheme, outputFile,
                SinkMode.REPLACE);

        Pipe lhs = new Pipe("lhs");
        lhs = new Each(lhs, new Debug());
        Pipe rhs = new Pipe("rhs");
        rhs = new Each(rhs, new Debug());

        Fields common1 = new Fields( "author" );
        Fields common2 = new Fields( "employee" );
        Pipe join = new CoGroup(lhs, common1, rhs, common2,
                new InnerJoin());

        join = new Each(join, new Fields( "organization",
                "department", "employee", "document", "keyword"),
                new Identity());

        FlowDef flowDef = FlowDef.flowDef()
                .addSource(lhs, source1)
                .addSource(rhs, source2)
                .addTailSink(join, sink);

        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();

    }

}
