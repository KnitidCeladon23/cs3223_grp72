package simpledb.opt;

import java.util.Map;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.metadata.*;
import simpledb.index.planner.*;
import simpledb.materialize.HashJoinPlan;
import simpledb.materialize.MergeJoinPlan;
import simpledb.materialize.NestedLoopJoinPlan;
import simpledb.multibuffer.MultibufferProductPlan;
import simpledb.plan.*;

/**
 * This class contains methods for planning a single table.
 * @author Edward Sciore
 */
class TablePlanner {
   private TablePlan myplan;
   private Predicate mypred;
   private Schema myschema;
   private String tblname;
   private Map<String,IndexInfo> indexes;
   private Transaction tx;
   
   /**
    * Creates a new table planner.
    * The specified predicate applies to the entire query.
    * The table planner is responsible for determining
    * which portion of the predicate is useful to the table,
    * and when indexes are useful.
    * @param tblname the name of the table
    * @param mypred the query predicate
    * @param tx the calling transaction
    */
   public TablePlanner(String tblname, Predicate mypred, Transaction tx, MetadataMgr mdm) {
      this.mypred  = mypred;
      this.tx  = tx;
      this.tblname = tblname;
      myplan   = new TablePlan(tx, tblname, mdm);
      myschema = myplan.schema();
      indexes  = mdm.getIndexInfo(tblname, tx);
   }
   
   /**
    * Constructs a select plan for the table.
    * The plan will use an indexselect, if possible.
    * @return a select plan for the table.
    */
   public Plan makeSelectPlan() {
      Plan p = makeIndexSelect();
      if (p == null)
         p = myplan;
      return addSelectPred(p);
   }
   
   /**
    * Constructs a join plan of the specified plan
    * and the table.  The plan will use an indexjoin, if possible.
    * (Which means that if an indexselect is also possible,
    * the indexjoin operator takes precedence.)
    * The method returns null if no join is possible.
    * @param current the specified plan
    * @return a join plan of the plan and this table
    */
   public Plan makeJoinPlan(Plan current) {
      Schema sch = current.schema();
      Predicate joinpred = mypred.joinSubPred(myschema, sch);
      if (joinpred == null)
         return null;
   		 Plan outputPlan = makeProductJoin(current, sch);  
   		 
   		 Plan mergePlan = makeMergeJoin(current, sch, joinpred);
   		 Plan indexPlan = makeIndexJoin(current, sch);
   		 Plan nestedPlan = makeNestedLoopJoin(current, sch, joinpred);
   		 Plan hashPlan = makeHashJoin(current, sch, joinpred);
   		 
   		 /**
   		  Commented out for future fixes
   		 
   		// if block accessed by each individual plan is cheaper than the current cheapest, override
//   		if (outputPlan != null)
//   			System.out.println("Product: " + outputPlan.blocksAccessed());
//   		
//   		if (mergePlan != null) {
//   			System.out.println("Sort Merge: " + mergePlan.blocksAccessed());
//   			if (mergePlan.blocksAccessed() < outputPlan.blocksAccessed()) {
//   				outputPlan = mergePlan;
//   			}
//   		}
//   		
//   		if (indexPlan != null) {
//   			System.out.println("Indexed: " + indexPlan.blocksAccessed());
//   			if (indexPlan.blocksAccessed() < outputPlan.blocksAccessed()) {
//   				outputPlan = indexPlan;
//   			}
//   		}
//   		
//   		if (nestedPlan != null) {
//   			System.out.println("Nested Loops: " + nestedPlan.blocksAccessed());
//   			if (nestedPlan.blocksAccessed() < outputPlan.blocksAccessed()) {
//   				outputPlan = nestedPlan;
//   			}
//   		}
//   		
//   		if (hashPlan != null) {
//   			System.out.println("Hashed: " + hashPlan.blocksAccessed());
//   			if (hashPlan.blocksAccessed() < outputPlan.blocksAccessed()) {
//   				outputPlan = hashPlan;
//   			}
//   		}
   		
   		*/
   		
   		return outputPlan;
   }
   
   /**
    * Constructs a product plan of the specified plan and
    * this table.
    * @param current the specified plan
    * @return a product plan of the specified plan and this table
    */
   public Plan makeProductPlan(Plan current) {
      Plan p = addSelectPred(myplan);
      return new MultibufferProductPlan(tx, current, p);
   }
   
   private Plan makeIndexSelect() {
      for (String fldname : indexes.keySet()) {
         Constant val = mypred.equatesWithConstant(fldname);
         if (val != null) {
            IndexInfo ii = indexes.get(fldname);
            if(ii.getStructName().equals("hash")) {
               String comparatorType = mypred.fieldComparator(fldname);
               if(comparatorType != null && !comparatorType.equals("=")) 
                  return null;
            }
            System.out.println("index on " + fldname + " used");
            return new IndexSelectPlan(myplan, ii, val, tblname);
         }
      }
      return null;
   }
   
   private Plan makeIndexJoin(Plan curr_p, Schema sch) {
      for (String fldname : indexes.keySet()) {
         String outerfield = mypred.equatesWithField(fldname);
         if (outerfield != null && sch.hasField(outerfield)) {
            IndexInfo ii = indexes.get(fldname);
            Plan p = new IndexJoinPlan(curr_p, myplan, ii, outerfield);
            p = addSelectPred(p);
            return addJoinPred(p, sch);
         }
      }
      return null;
   }
   
   private Plan makeProductJoin(Plan curr_p, Schema sch) {
      Plan p = makeProductPlan(curr_p);
      return addJoinPred(p, sch);
   }
   
   private Plan makeMergeJoin(Plan curr_p, Schema sch, Predicate pred) {
		Plan p = null;
		String[] fields = pred.toString().split("=");
		
		if (curr_p.schema().hasField(fields[0]) && myplan.schema().hasField(fields[1])) {			
			p = new MergeJoinPlan(tx, curr_p, myplan, fields[0], fields[1]);
		} else if (curr_p.schema().hasField(fields[1]) && myplan.schema().hasField(fields[0])) {
			p = new MergeJoinPlan(tx, curr_p, myplan, fields[1], fields[0]);
		} else {
			return null;
		}
		p = addSelectPred(p);
		return addJoinPred(p, sch);
	}
   
   private Plan makeNestedLoopJoin(Plan curr_p, Schema sch, Predicate pred) {

		Plan p = null;
		String[] fields = pred.toString().split("=");
		
		if (curr_p.schema().hasField(fields[0]) && myplan.schema().hasField(fields[1])) {			
			p = new NestedLoopJoinPlan(tx, curr_p, myplan, fields[0], fields[1]);
		} else if(curr_p.schema().hasField(fields[1]) && myplan.schema().hasField(fields[0])) {
			p = new NestedLoopJoinPlan(tx, curr_p, myplan, fields[1], fields[0]);
		} else {
			return null;
		}
		p = addSelectPred(p);
		return addJoinPred(p, sch);
	}
   
   private Plan makeHashJoin(Plan curr_p, Schema sch, Predicate pred) {
	   
		Plan p = null;
		String[] fields = pred.toString().split("=");
		if (curr_p.schema().hasField(fields[0]) && myplan.schema().hasField(fields[1])) {			
			p = new HashJoinPlan(tx, curr_p, myplan, fields[0], fields[1]);
		} else if(curr_p.schema().hasField(fields[1]) && myplan.schema().hasField(fields[0])) {
			p = new HashJoinPlan(tx, curr_p, myplan, fields[1], fields[0]);
		} else {
			return null;
		}
		p = addSelectPred(p);
		return addJoinPred(p, sch);
	}
   
   private Plan addSelectPred(Plan p) {
      Predicate selectpred = mypred.selectSubPred(myschema);
      if (selectpred != null)
         return new SelectPlan(p, selectpred);
      else
         return p;
   }
   
   private Plan addJoinPred(Plan p, Schema sch) {
      Predicate joinpred = mypred.joinSubPred(sch, myschema);
      if (joinpred != null)
         return new SelectPlan(p, joinpred);
      else
         return p;
   }
   
   
}
