package implementations;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Identity;
import cascading.pipe.*;
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

        String memberInfoInputFile="memberInfo";
        String expenseInfoInputFile="memberInfoExpenseOnEducation";

        String outputFile="memberInfoOutput";

        Fields memberInfoFields=new Fields("id","firstName","lastName","address","age","phone","occupation","mbrRegion","mbrCode");
        Fields expenseInfoFields=new Fields("id","firstname","lastname","schoolName","privateOrGovFlag","governmentEqu","expense","numbers");

        Fields outputFields=new Fields("id","firstName","lastName","address","age","phone","occupation","mbrRegion","mbrCode","idNext","firstname","lastname","schoolName","privateOrGovFlag","governmentEqu","expense","numbers");

        Scheme memberInfoSourceScheme=new TextDelimited(memberInfoFields,true,",");
        Scheme expenseInfoSourceScheme=new TextDelimited(expenseInfoFields,true,",");
        Tap memberInfoSourceTap=new FileTap(memberInfoSourceScheme,memberInfoInputFile);
        Tap expenseInfoSourceTap=new FileTap(expenseInfoSourceScheme,expenseInfoInputFile);

        Scheme sinkScheme=new TextDelimited(outputFields,true,",");
        Tap sinkTap=new FileTap(sinkScheme,outputFile, SinkMode.REPLACE);

        Pipe lhs = new Each("memberInfo", memberInfoFields, new Identity());
        Pipe rhs = new Each("expenseInfo", expenseInfoFields, new Identity());
        Fields common = new Fields( "id" );
        Fields declared_fields = outputFields;
        Pipe join = new CoGroup( rhs, common, lhs, common, declared_fields);

        Pipe groupBy=new Pipe("groupBy");
        groupBy=new GroupBy(groupBy,new Fields("schoolName"));
        groupBy = new Every(groupBy, new Fields("number_sold", "number_returned"), new GroupAggregaotr(new Fields("total_kept")), Fields.ALL);


        FlowDef flowDef = FlowDef.flowDef()
                .addSource(lhs, memberInfoSourceTap)
                .addSource(rhs, expenseInfoSourceTap)
                .addTailSink(join,sinkTap);

        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();

    }
}
