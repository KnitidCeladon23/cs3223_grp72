package simpledb.materialize;

import java.util.*;

import simpledb.query.*;

/**
 * A comparator for scans.
 * @author Edward Sciore
 */
public class RecordComparator implements Comparator<Scan> {
   private LinkedHashMap<String, Boolean> fields;
   private List<String> fields_lst;
   
   /**
    * Create a comparator using the specified fields,
    * using the ordering implied by its iterator.
    * @param fields a list of field names
    */
   public RecordComparator(LinkedHashMap<String, Boolean> fields) {
      this.fields = fields;
   }

   public RecordComparator(List<String> fields) {
      this.fields = new LinkedHashMap<>();
      this.fields_lst = fields;
      for (String field : fields) {
         this.fields.put(field, true);
      }
   }
   
   /**
    * Compare the current records of the two specified scans.
    * The sort fields are considered in turn.
    * When a field is encountered for which the records have
    * different values, those values are used as the result
    * of the comparison.
    * If the two records have the same values for all
    * sort fields, then the method returns 0.
    * @param s1 the first scan
    * @param s2 the second scan
    * @return the result of comparing each scan's current record according to the field list
    */
   public int compare(Scan s1, Scan s2) {

      if (this.fields_lst != null) {
			for (String fldname : this.fields_lst) {
				Constant val1 = s1.getVal(fldname);
				Constant val2 = s2.getVal(fldname);
				int result = val1.compareTo(val2);
				if (result != 0)
					return result;
			}
			return 0;
		}

	   for (Map.Entry<String, Boolean> entry : fields.entrySet()) {
		   Constant val1 = s1.getVal(entry.getKey());
	      Constant val2 = s2.getVal(entry.getKey());
	      int result = val1.compareTo(val2);
	      if (result != 0) {
	    	if (entry.getValue()) {
	    		return result;
	    	} else {
	    		return -result;
	    	}
	      }
	  }
      return 0;
   }

   public boolean compareDistinct(Scan s1, Scan s2) {
      for (String fldname : this.fields_lst) {
			Constant val1 = s1.getVal(fldname);
			Constant val2 = s2.getVal(fldname);
			int result = val1.compareTo(val2);
			if (result == 0)
				return false;
		}
		return true;
   }
}
