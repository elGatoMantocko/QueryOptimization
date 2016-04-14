package query;

import global.*;
import heap.HeapFile;
import parser.AST_Select;
import relop.*;
import relop.Iterator;

import java.lang.reflect.Array;
import java.util.*;

/**
 * Execution plan for selecting tuples.
 */
class Select extends TestablePlan {

  private String[] tables;
  private String[] cols;
  private Predicate[][] preds;
  private boolean explain;

  ArrayList<Iterator> mTablesList;

  private Iterator finalIterator;


  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if validation fails
   */
  public Select(AST_Select tree) throws QueryException {

    this.tables = tree.getTables();
    this.preds = tree.getPredicates();
    this.cols = tree.getColumns();
    this.explain = tree.isExplain;
    this.mTablesList = new ArrayList<>();

    validate(); //throws QueryException

    //create a table candidate chooser
    TableManager tm = new TableManager(mTablesList);
    //create a predicate manager
    PredicateManager pm = new PredicateManager(preds);

    if(finalIterator == null) {
      finalIterator = tm.getCandidate();
      pushSelection(pm);

    }
    while(tm.hasNext()) {
      //join on applicable pred.
      joinIfPossible(tm, pm);
      //push selections
      pushSelection(pm);
    }

    applyProjections();

    // explaining for testing purposes
    // finalIterator.explain(0);
    setFinalIterator(finalIterator);
  } // public Select(AST_Select tree) throws QueryException

  private void applyProjections() {
    Schema finalSchema = finalIterator.getSchema();

    Integer[] fieldNums = new Integer[cols.length];
    for (int i = 0; i < cols.length; i++) {
      fieldNums[i] = finalSchema.fieldNumber(cols[i]);
    }

    if (fieldNums.length > 0) {
      finalIterator = new Projection(finalIterator, fieldNums);
    }
  }

  private void joinIfPossible(TableManager tm, PredicateManager pm) {
    Iterator joinCandidate = tm.getCandidate();
    Predicate[] joinPredicate = pm.popApplicableJoinPredicate(finalIterator, joinCandidate);
    if(joinPredicate != null)
      finalIterator = new SimpleJoin(finalIterator, joinCandidate, joinPredicate);
    else
      finalIterator = new SimpleJoin(finalIterator, joinCandidate);
  }

  private void pushSelection(PredicateManager pm) {
    //push selections
    Collection<Predicate[]> selectionPredicates = pm.popApplicablePredicates(finalIterator);
    for(Predicate[] pred : selectionPredicates) {
      finalIterator = new Selection(finalIterator, pred);
    }
  }

  private void validate() throws QueryException {
    HashMap<String, ArrayList<IndexDesc>> indexes = new HashMap<>();
    // check that the predicates are valid
    try {
      // validate the query input
      Schema schemaValidation = new Schema(0);
      for (String table : tables) {
        QueryCheck.tableExists(table);
        Schema tableSchema = Minibase.SystemCatalog.getSchema(table);
        schemaValidation = Schema.join(tableSchema, schemaValidation);

        // this could possibly be bad if there are multiple
        //  indexes with different names that have the same column names
        for (IndexDesc desc : Minibase.SystemCatalog.getIndexes(table)) {
          if (indexes.get(table) == null) {
            indexes.put(table, new ArrayList<IndexDesc>());
            indexes.get(table).add(desc);
          } else {
            indexes.get(table).add(desc);
          }
        }

        // create a new filescan out of the current table
        HeapFile file = new HeapFile(table);
        mTablesList.add(new FileScan(tableSchema, file));
      }

      QueryCheck.predicates(schemaValidation, preds);

      // have to convert the column names to column numbers
      for (int i = 0; i < cols.length; i++) {
        // validate the column
        QueryCheck.columnExists(schemaValidation, cols[i]);
      }
    } catch(QueryException e){
      mTablesList.forEach(Iterator::close);
      throw e;
    }
  }

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    if (explain) {
      finalIterator.explain(0);
      finalIterator.close();
    } else {
      finalIterator.execute();
    }
  } // public void execute()

} // class Select implements Plan

class TableData extends Object implements Comparable{
  private ArrayList<String> tables;
  protected Schema schema;
  protected int cost;

  public TableData(String table) {
    this.tables = new ArrayList<>();
    this.tables.add(table);
    
    this.schema = Minibase.SystemCatalog.getSchema(table);

    this.cost = Minibase.SystemCatalog.getRecCount(table);
  }

  private TableData(TableData copy) {
    this.tables = new ArrayList<>();
    this.tables.addAll(Arrays.asList(copy.getTables()));

    this.schema = Schema.join(new Schema(0), copy.schema);

    this.cost = copy.cost;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    } else if (!(o instanceof TableData)) {
      return false;
    }

    TableData d = (TableData)o;
    return 
      Arrays.deepEquals(this.getTables(), d.getTables()) &&
      this.schema.equals(d.schema) &&
      new Integer(this.cost).equals(new Integer(d.cost));
  }

  @Override
  public int hashCode() {
    int result = 7;
    for (String table : tables) {
      result += table.hashCode();
    }
    result += schema.hashCode();
    result += new Integer(cost).hashCode();

    return 37 * result;
  }
  
  private void addTable(String table) {
    tables.add(table);
    schema = Schema.join(schema, Minibase.SystemCatalog.getSchema(table));
    cost *= Minibase.SystemCatalog.getRecCount(table);
  }

  private String[] getTables() {
    return tables.toArray(new String[tables.size()]);
  }

  public static TableData join(TableData left, TableData right) {
    TableData join = new TableData(left);
    for (String table : right.getTables()) {
      join.addTable(table);
    }
    return join;
  }

  public String toString() {
    return Arrays.deepToString(getTables()) + " " + cost;
  }

  /**
   * Compares this object with the specified object for order.  Returns a
   * negative integer, zero, or a positive integer as this object is less
   * than, equal to, or greater than the specified object.
   * <p>
   * <p>The implementor must ensure <tt>sgn(x.compareTo(y)) ==
   * -sgn(y.compareTo(x))</tt> for all <tt>x</tt> and <tt>y</tt>.  (This
   * implies that <tt>x.compareTo(y)</tt> must throw an exception iff
   * <tt>y.compareTo(x)</tt> throws an exception.)
   * <p>
   * <p>The implementor must also ensure that the relation is transitive:
   * <tt>(x.compareTo(y)&gt;0 &amp;&amp; y.compareTo(z)&gt;0)</tt> implies
   * <tt>x.compareTo(z)&gt;0</tt>.
   * <p>
   * <p>Finally, the implementor must ensure that <tt>x.compareTo(y)==0</tt>
   * implies that <tt>sgn(x.compareTo(z)) == sgn(y.compareTo(z))</tt>, for
   * all <tt>z</tt>.
   * <p>
   * <p>It is strongly recommended, but <i>not</i> strictly required that
   * <tt>(x.compareTo(y)==0) == (x.equals(y))</tt>.  Generally speaking, any
   * class that implements the <tt>Comparable</tt> interface and violates
   * this condition should clearly indicate this fact.  The recommended
   * language is "Note: this class has a natural ordering that is
   * inconsistent with equals."
   * <p>
   * <p>In the foregoing description, the notation
   * <tt>sgn(</tt><i>expression</i><tt>)</tt> designates the mathematical
   * <i>signum</i> function, which is defined to return one of <tt>-1</tt>,
   * <tt>0</tt>, or <tt>1</tt> according to whether the value of
   * <i>expression</i> is negative, zero or positive.
   *
   * @param o the object to be compared.
   * @return a negative integer, zero, or a positive integer as this object
   * is less than, equal to, or greater than the specified object.
   * @throws NullPointerException if the specified object is null
   * @throws ClassCastException   if the specified object's type prevents it
   *                              from being compared to this object.
   */
  @Override
  public int compareTo(Object o) {
    return this.cost - ((TableData)o).cost;
  }
}

