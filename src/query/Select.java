package query;

import global.Minibase;
import heap.HeapFile;
import parser.AST_Select;
import relop.*;

/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {

  Iterator mIterator;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if validation fails
   */
  public Select(AST_Select tree) throws QueryException {
    //simplejoin all tables
    String[] tables = tree.getTables();
    Schema prevschema = Minibase.SystemCatalog.getSchema(tables[0]);
    Iterator prev = new FileScan(prevschema, new HeapFile(tables[0]));
    Schema curschema;
    Iterator cur = prev;

    for (int i = 1; i < tables.length; i++) {
      curschema = Minibase.SystemCatalog.getSchema(tables[i]);
      cur = new FileScan(curschema, new HeapFile(tables[i]));

      prev = new SimpleJoin(prev, cur);
    }


    //apply predicates
    Predicate[][] predicateses = tree.getPredicates();

    Iterator selections = cur;
    for (Predicate[] predicates : predicateses) {
      selections = new Selection(selections, predicates);
    }


    //projection
    String[] columns = tree.getColumns();
    Iterator projections = selections;
    Schema preprojschema = selections.getSchema();
    Integer[] colnums = new Integer[tree.getColumns().length];

    for (int i = 0; i < columns.length; i++) {
      colnums[i] = preprojschema.fieldNumber(columns[i]);
    }
    mIterator = new Projection(selections, colnums);

    mIterator.explain(0);
  } // public Select(AST_Select tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    mIterator.execute();

    // print the output message
    System.out.println("0 rows affected. (Not implemented)");

  } // public void execute()

} // class Select implements Plan
