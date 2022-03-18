package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

/**
 * The <i>min</i> aggregation function.
 * @author Edward Sciore
 */
public class MinFn implements AggregationFn {
   private String fldname;
   private boolean isDistinct;
   private Constant val;

   /**
    * Create a min aggregation function for the specified field.
    * @param fldname the name of the aggregated field
    */
   public MinFn(String fldname, boolean isDistinct) {
      this.fldname = fldname;
      this.isDistinct = isDistinct;
   }
   
   /**
    * Start a new minimum to be the
    * field value in the current record.
    * @see simpledb.materialize.AggregationFn#processFirst(simpledb.query.Scan)
    */
   public void processFirst(Scan s) {
      val = s.getVal(fldname);
   }
   
   /**
    * Replace the current minimum by the field value
    * in the current record, if it is lower.
    * @see simpledb.materialize.AggregationFn#processNext(simpledb.query.Scan)
    */
   public void processNext(Scan s) {
      Constant newval = s.getVal(fldname);
      if (newval.compareTo(val) < 0)
         val = newval;
   }
   
   public String fieldName() {
      return "minof" + fldname;
   }
   
   public String field() {
      return fldname;
   }
   
   public Constant value() {
      return val;
   }
   
   public boolean isAlwaysInteger() {
	   return false;
   }
}
