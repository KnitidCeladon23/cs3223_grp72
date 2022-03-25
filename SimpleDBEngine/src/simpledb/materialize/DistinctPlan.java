package simpledb.materialize;

import java.util.*;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.plan.Plan;
import simpledb.query.*;

/**
 * The Plan class for the <i>sort</i> operator.
 *
 * @author Edward Sciore
 */
public class DistinctPlan implements Plan {
    private Transaction tx;
    private Plan p;
    private Schema sch;
    private RecordComparator comp;

    private Map<String, Map<Constant, Boolean>> duplicateTracker;
    private List<String> distinct;

    /**
     * Create a sort plan for the specified query.
     *
     * @param tx         the calling transaction
     * @param p          the plan for the underlying query
     * @param sortfields the fields to sort by
     */

    public DistinctPlan(Transaction tx, Plan p, List<String> distinct) {
        this.tx = tx;
        this.p = p;
        sch = p.schema();
        this.distinct = distinct;
        this.duplicateTracker = new HashMap<String, Map<Constant, Boolean>>();
        comp = new RecordComparator(distinct);
    }

    /**
     * This method is where most of the action is. Up to 2 sorted temporary tables
     * are created, and are passed into SortScan for final merging.
     *
     * @see simpledb.plan.Plan#open()
     */
    public Scan open() {
        Scan s = p.open();
        Scan s_out = p.open();
        List<TempTable> runs = splitIntoRuns(s);
        s.close();

        while (runs.size() > 1) {
            runs = distinctMergeIter(runs);
        }
        if (runs.size() >= 1) {
            s_out = runs.get(0).open();
        }

        return s_out;
    }

    private List<TempTable> distinctMergeIter(List<TempTable> runs) {
        List<TempTable> result = new ArrayList<>();
        TempTable p1 = runs.remove(0);
        TempTable p2 = runs.remove(0);
        while (runs.size() > 1) {
            result.add(mergeDistinctRuns(p1, p2));
        }
        if (runs.size() == 1) {
            result.add(runs.get(0));
        }
        return result;
    }

    /**
     * @param s1 first scan
     * @param s2 second scan
     * @return boolean result of whether the scan is different
     */
    private boolean isDifferent(Scan s1, Scan s2) {
        if (s1 == null || s2 == null) {
            return false;
        }
        return comp.compare(s1, s2) != 0;
    }

    /**
     * @param p1 first run
     * @param p2 second run
     * @return merged run
     */
    private TempTable mergeDistinctRuns(TempTable p1, TempTable p2) {
        Scan s1 = p1.open();
        Scan s2 = p2.open();
        boolean hasMore1 = s1.next();
        boolean hasMore2 = s2.next();
        TempTable result = new TempTable(tx, sch);
        UpdateScan us = result.open();

        if (hasMore1 && hasMore2) {
            if (comp.compare(s1, s2) < 0) {
                hasMore1 = copy(s1, us);
            } else {
                hasMore2 = copy(s2, us);
            }
        }

        // iter through compare until one run is completed
        while (hasMore1 && hasMore2) {
            System.out.println("loop1");
            if (comp.compare(s1, s2) < 0) {
                if (isDifferent(s1, us)) {
                    hasMore1 = copy(s1, us);
                } else {
                    hasMore1 = s1.next();
                }
            } else if (isDifferent(s2, us)) {
                hasMore2 = copy(s2, us);
            } else {
                hasMore2 = s2.next();
            }
        }

        if (hasMore1) {
            while (hasMore1) {
                System.out.println("loop2");
                if (isDifferent(s1, us)) {
                    hasMore1 = copy(s1, us);
                } else {
                    hasMore1 = s1.next();
                }
            }
        } else {
            while (hasMore2) {
                System.out.println("loop3");
                if (isDifferent(s2, us)) {
                    hasMore2 = copy(s2, us);
                } else {
                    hasMore2 = s2.next();
                }
            }
        }

        s1.close();
        s2.close();
        us.close();
        return result;
    }

    /**
     * Return the number of blocks in the sorted table, which is the same as it
     * would be in a materialized table. It does <i>not</i> include the one-time
     * cost of materializing and sorting the records.
     *
     * @see simpledb.plan.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        // does not include the one-time cost of sorting
        Plan mp = new MaterializePlan(tx, p); // not opened; just for analysis
        return mp.blocksAccessed();
    }

    /**
     * Return the number of records in the sorted table, which is the same as in the
     * underlying query.
     *
     * @see simpledb.plan.Plan#recordsOutput()
     */
    public int recordsOutput() {
        return p.recordsOutput();
    }

    /**
     * Return the number of distinct field values in the sorted table, which is the
     * same as in the underlying query.
     *
     * @see simpledb.plan.Plan#distinctValues(java.lang.String)
     */
    public int distinctValues(String fldname) {
        return p.distinctValues(fldname);
    }

    /**
     * Return the schema of the sorted table, which is the same as in the underlying
     * query.
     *
     * @see simpledb.plan.Plan#schema()
     */
    public Schema schema() {
        return sch;
    }

    private List<TempTable> splitIntoRuns(Scan s) {
        List<TempTable> temps = new ArrayList<>();
        s.beforeFirst();
        if (!s.next())
            return temps;
        TempTable currenttemp = new TempTable(tx, sch);
        temps.add(currenttemp);
        UpdateScan currentscan = currenttemp.open();
        while (copy(s, currentscan))
            System.out.println("loop4");
            if (comp.compare(s, currentscan) < 0) {
                currentscan.close();
                currenttemp = new TempTable(tx, sch);
                temps.add(currenttemp);
                currentscan = (UpdateScan) currenttemp.open();
            }
        currentscan.close();
        return temps;
    }

    private boolean copy(Scan s, UpdateScan us) {
        us.insert();
        for (String fldname : sch.fields())
            us.setVal(fldname, s.getVal(fldname));
        return s.next();
    }
}
