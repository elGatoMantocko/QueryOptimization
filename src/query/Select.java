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

  ArrayList<TableData> mTablesList;

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

    validate(); //throws QueryException

    //create a table candidate chooser
    TableManager tm = new TableManager(tables);
    //create a predicate manager
    PredicateManager pm = new PredicateManager(preds);

    if(finalIterator == null) {
      finalIterator = tm.getCandidate(pm).getIterator();
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
    Iterator joinCandidate = tm.getCandidate(pm).getIterator();
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
      }

      QueryCheck.predicates(schemaValidation, preds);

      // have to convert the column names to column numbers
      for (int i = 0; i < cols.length; i++) {
        // validate the column
        QueryCheck.columnExists(schemaValidation, cols[i]);
      }
    } catch(QueryException e){
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

