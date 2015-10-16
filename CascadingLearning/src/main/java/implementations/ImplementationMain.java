package implementations;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

/**
 * Created by sanjeev on 10/13/15.
 */
public class ImplementationMain {

    public static void main(String[] args){

        String inputFile="memberInfo";
        String outputFile="memberInfoOutput";

        Fields outputFields=new Fields("name","ageRaise");

        Scheme sourceScheme=new TextDelimited(outputFields,true,",");
        Tap sourceTap=new FileTap(sourceScheme,inputFile);

        Scheme sinkScheme=new TextDelimited(true,",");
        Tap sinkTap=new FileTap(sinkScheme,outputFile, SinkMode.REPLACE);

        Pipe filter=new Pipe("filter");
        filter=new Each(filter,new Raise("mechanic"));
        filter= new Each(filter,new RaiseCalculation());

        FlowConnector flowConnector= new LocalFlowConnector();
        Flow flow=flowConnector.connect(sourceTap,sinkTap,filter);
        flow.complete();

    }
}
