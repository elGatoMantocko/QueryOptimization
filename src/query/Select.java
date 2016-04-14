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

  private HashMap<TableData, Iterator> iteratorMap;
  private ArrayList<Predicate[]> predsList;

  private Iterator finalIterator;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if validation fails
   */
  public Select(AST_Select tree) throws QueryException {

    this.iteratorMap = new HashMap<TableData, Iterator>();
    this.predsList = new ArrayList<Predicate[]>();

    this.tables = tree.getTables();
    this.preds = tree.getPredicates();
    this.cols = tree.getColumns();
    this.explain = tree.isExplain;

    HashMap<String, ArrayList<IndexDesc>> indexes = new HashMap<String, ArrayList<IndexDesc>>();

    for (Predicate[] p : preds) {
      predsList.add(p);
    }

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
        iteratorMap.put(new TableData(table), new FileScan(tableSchema, file));
      }

      QueryCheck.predicates(schemaValidation, preds);

      // have to convert the column names to column numbers
      for (int i = 0; i < cols.length; i++) {
        // validate the column
        QueryCheck.columnExists(schemaValidation, cols[i]);
      }
    } catch(QueryException e){
      for (Iterator i : iteratorMap.values()) {
        i.close();
      }
      throw e;
    }

    while (iteratorMap.size() != 1 || predsList.size() != 0) {
      if (predsList.size() != 0) {
        pushSelectionOperator();
      }

      if (iteratorMap.size() != 1) {
        pushJoinOperator();
      }
    }

    List<Map.Entry<TableData, Iterator>> entries = new ArrayList<>();
    entries.addAll(iteratorMap.entrySet());
    Schema finalSchema = entries.get(0).getKey().schema;

    Integer[] fieldNums = new Integer[cols.length];
    for (int i = 0; i < cols.length; i++) {
      fieldNums[i] = finalSchema.fieldNumber(cols[i]);
    }

    if (fieldNums.length > 0) {
      finalIterator = new Projection(entries.get(0).getValue(), fieldNums);
    } else {
      finalIterator = entries.get(0).getValue();
    }

    // explaining for testing purposes
    // finalIterator.explain(0);
    setFinalIterator(finalIterator);
  } // public Select(AST_Select tree) throws QueryException

  private void pushJoinOperator() {
    TableData[] tables; // = iteratorMap.keySet().toArray(new TableData[iteratorMap.keySet().size()]);

    ArrayList<TableData> sortedSet = new ArrayList<>();
    sortedSet.addAll(iteratorMap.keySet());
    Collections.sort(sortedSet, TableData::compareTo);
    tables = sortedSet.toArray(new TableData[iteratorMap.keySet().size()]);

    Log.trace(Arrays.deepToString(tables));
    Log.trace(Arrays.deepToString(predsList.toArray()));

    int[] tablesToJoin = null;
    int costOfJoin = Integer.MAX_VALUE;

    Predicate[] finalPredToJoinOn = null;

    for (int i = 0; i < tables.length; i++) {
      for (int j = i + 1; j < tables.length; j++) {
        TableData joinedData = TableData.join(tables[i], tables[j]);

        int bestPredScore = Integer.MIN_VALUE;
        Predicate[] predToJoinOn = null;

        if (joinedData.cost < costOfJoin) {
          costOfJoin = joinedData.cost;
          tablesToJoin = new int[] { i, j };
          for (Predicate[] candidate : predsList) {
            boolean validCandidate = true;
            int score = 0;

            for (Predicate p : candidate) {
              validCandidate = validCandidate && p.validate(joinedData.schema);
              if (p.getOper() == AttrOperator.EQ && p.getLtype() == AttrType.COLNAME && p.getRtype() == AttrType.COLNAME) {
                score++;
              }
            }

            if (validCandidate && score > bestPredScore) {
              predToJoinOn = candidate;
              bestPredScore = score;
            }
          }

          finalPredToJoinOn = predToJoinOn;
        }
      }
    }

    if (tablesToJoin == null) {
      throw new RuntimeException("We should have found some tables to join");
    } else {
      int i = tablesToJoin[0];
      int j = tablesToJoin[1];
      SimpleJoin join;
      if (finalPredToJoinOn != null) {
        join = new SimpleJoin(
            iteratorMap.get(tables[i]),
            iteratorMap.get(tables[j]), 
            finalPredToJoinOn
        );
      } else {
        join = new SimpleJoin(
            iteratorMap.get(tables[i]),
            iteratorMap.get(tables[j])
        );
      }

      predsList.remove(finalPredToJoinOn);

      iteratorMap.remove(tables[i]);
      iteratorMap.remove(tables[j]);

      iteratorMap.put(TableData.join(tables[i], tables[j]), join);
    }
  }

  private void pushSelectionOperator() {
    // for each table being joined
    for (TableData key : iteratorMap.keySet()) {
      // save the schema for this table
      Schema tableSchema = key.schema;
      ArrayList<Predicate[]> pListCopy = (ArrayList<Predicate[]>)predsList.clone();

      for (Predicate[] candidate : pListCopy) {
        boolean canPushSelect = true;

        for (Predicate p : candidate) {
          canPushSelect = canPushSelect && p.validate(tableSchema);
        }

        if (canPushSelect) {
          predsList.remove(candidate);
          iteratorMap.put(key, new Selection(iteratorMap.get(key), candidate));
        }
      }
    }
  } // push selections

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

