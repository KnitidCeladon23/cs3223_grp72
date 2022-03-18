package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class AggregationTest {

	public static void main(String[] args) {
        try {
            // analogous to the driver
            SimpleDB db = new SimpleDB("studentdb");

            // analogous to the connection
            Transaction tx = db.newTx();
            Planner planner = db.planner();

            String qry1 = "select sid, majorid from student";
            Plan p1 = planner.createQueryPlan(qry1, tx);

            // analogous to the result set
            Scan s1 = p1.open();

            System.out.println("majorid\t\tsid");

            while (s1.next()) {
                int sid = s1.getInt("sid");
                int majorid = s1.getInt("majorid"); //SimpleDB stores field names
                System.out.println(majorid + "\t\t" + sid);
            }
            System.out.println("\n----------------------------------------------\n");
            String qry2 = "select majorid, MAX(sid), MIN(sid), AVG(sid), COUNT(sid), SUM(sid) from student group by majorid";
            Plan p = planner.createQueryPlan(qry2, tx);
            Scan s = p.open();

            System.out.println("majorid\t        maxofsid\tminofsid\tavgofsid\tcoutofsid\tsumofsid");
            while (s.next()) {
                int minofsid = s.getInt("minofsid");
                int maxofsid = s.getInt("maxofsid");
                int avgofsid = s.getInt("avgofsid");
                int countofsid = s.getInt("countofsid");
                int sumofsid = s.getInt("sumofsid");
                int majorid = s.getInt("majorid"); //SimpleDB stores field names
                System.out.println(majorid + "\t\t" + maxofsid + "\t\t" + minofsid + "\t\t" + avgofsid + "\t\t" + countofsid + "\t\t" + sumofsid);
            }
            s.close();
            s1.close();
            tx.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
	
}
