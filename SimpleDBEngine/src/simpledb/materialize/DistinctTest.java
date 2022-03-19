// package simpledb.materialize;

// import simpledb.metadata.MetadataMgr;
// import simpledb.opt.HeuristicQueryPlanner;
// import simpledb.parse.QueryData;
// import simpledb.plan.Plan;
// import simpledb.query.*;
// import simpledb.server.SimpleDB;
// import simpledb.tx.Transaction;

// import java.util.ArrayList;
// import java.util.Collection;
// import java.util.LinkedHashMap;
// import java.util.List;

// public class DistinctTest {
//     public static void main(String[] args) {
//         SimpleDB db = new SimpleDB("studentdb");
//         MetadataMgr mdm = db.mdMgr();
//         Transaction tx = db.newTx();

//         List<String> fields = new ArrayList<>();
//         fields.add("majorid");
//         List<AggregationFn> aggregations = new ArrayList<>();
//         Collection<String> tables = new ArrayList<>();
//         tables.add("student");
//         Predicate pred = new Predicate();
//         List<String> orderfields = new ArrayList<>();
//         orderfields.add("majorid");
//         List<String> groupfields = new ArrayList<>();
//         QueryData data = new QueryData(fields, tables, pred, orderfields, true, aggregations, groupfields);

//         HeuristicQueryPlanner planner = new HeuristicQueryPlanner(mdm);
//         Plan p = planner.createPlan(data, tx);
//         Scan s = p.open();

//         System.out.println("Majorid");

//         while(s.next()) {
//             int majorid = s.getInt("majorid");
//             System.out.println(majorid);
//         }
//         System.out.println("Finished");
//     }
// }
