package simpledb.parse;

import java.util.*;

import simpledb.materialize.AggregationFn;
import simpledb.query.*;

/**
 * Data for the SQL <i>select</i> statement.
 * @author Edward Sciore
 */
public class QueryData {
   private List<String> fields;
   private Collection<String> tables;
   private Predicate pred;
   private LinkedHashMap<String, Boolean> orderByAttributesList;
   private boolean distinct;
   private List<AggregationFn> aggregations;
   private List<String> groupfields;
   
   /**
    * Saves the field and table list and predicate.
    */
   public QueryData(List<String> fields, Collection<String> tables, Predicate pred, LinkedHashMap<String, Boolean> orderByAttributesList,
		   boolean distinct, List<AggregationFn> aggregations, List<String> groupfields) {
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;
      this.orderByAttributesList = orderByAttributesList;
      this.distinct = distinct;
      this.aggregations = aggregations;
      this.groupfields = groupfields;
   }
   
   /**
    * Returns the fields mentioned in the select clause.
    * @return a list of field names
    */
   public List<String> fields() {
      return fields;
   }
   
   /**
    * Returns the tables mentioned in the from clause.
    * @return a collection of table names
    */
   public Collection<String> tables() {
      return tables;
   }
   
   /**
    * Returns the predicate that describes which
    * records should be in the output table.
    * @return the query predicate
    */
   public Predicate pred() {
      return pred;
   }
   
   /**
    * Returns the list of 'order by' attributes that describes which
    * records should be in the output table.
    * @return the query predicate
    */
   public LinkedHashMap<String, Boolean> orderByAttributeList() {
      return orderByAttributesList;
   }
   
   /**
    * Returns whether the given select query specifies 'distinct' or not.
    * @return boolean indicating whether distinct
    */
   public boolean isDistinct() {
	   return distinct;
   }
   
   /**
    * Returns the aggregation functions in the select clause.
    * @return a list of aggregation functions
    */
   public List<AggregationFn> aggregations() {
       return aggregations;
   }

   /**
    * Returns the group fields in the group by clause.
    * @return a list of group fields
    */
   public List<String> groupfields() {
       return groupfields;
   }
   
   public String toString() {
      String result = "select ";
      for (String fldname : fields)
         result += fldname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      result += " from ";
      for (String tblname : tables)
         result += tblname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      String predstring = pred.toString();
      if (!predstring.equals(""))
         result += " where " + predstring;
      return result;
   }
}
