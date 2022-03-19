package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class GroupByTest {
	
	/**
	 * Generates a Group By plan and commits the transaction.
	 * @param args
	 */
   public static void main(String[] args) {
      try {
    	 // analogous to the driver
		 SimpleDB db = new SimpleDB("studentdb");
		
		 // analogous to the connection
		 Transaction tx  = db.newTx();
		 Planner planner = db.planner();

         // analogous to the statement
		 String qry1 = "select majorid, sname, sid, gradyear" +
                 " from STUDENT";
         String qry2 = "select majorid, count(sname), sum(sname), avg(sid), max(gradyear), min(gradyear)" +
                      " from STUDENT group by majorid";
         Plan p1 = planner.createQueryPlan(qry1, tx);
         Scan s1 = p1.open();

         System.out.println("majorid\t\tsname\t\tsid\t\tgradyear");

         while (s1.next()) {
             int sid = s1.getInt("sid");
             int majorid = s1.getInt("majorid"); //SimpleDB stores field names
             int sname = s1.getInt("sname");
             int gradyear = s1.getInt("gradyear");
             System.out.println(majorid + "\t\t" + sname + "\t\t" + sid + "\t\t" + gradyear);
         }
         
         Plan p2 = planner.createQueryPlan(qry2, tx);

         // analogous to the result set
         Scan s = p2.open();

         System.out.println("MajorID\t\tCountofsname\tSumofsname\tAvgofsid\tMaxofgradyear\tMinofgradyear");
         while (s.next()) {
            int count = s.getInt("countofsname"); //SimpleDB stores field names
            int sum = s.getInt("sumofsname"); //SimpleDB stores field names
            int avg = s.getInt("avgofsid"); //SimpleDB stores field names
            int max = s.getInt("maxofgradyear"); //SimpleDB stores field names
            int min = s.getInt("minofgradyear"); //SimpleDB stores field names
            int majorid = s.getInt("majorid"); //in lower case
            System.out.println(majorid + "\t\t" + count + "\t\t" + sum + "\t\t" + avg + "\t\t" + max + "\t\t" + min + "\t\t");
         }
         s.close();
         tx.commit();
         
      }
      catch(Exception e) {
         e.printStackTrace();
      }
   }
}
