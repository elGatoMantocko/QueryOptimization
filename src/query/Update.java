package query;

import parser.AST_Update;

import global.Minibase;
import relop.Predicate;
import relop.Schema;

/**
 * Execution plan for updating tuples.
 */
class Update implements Plan {

  private String fileName;
  private Predicate[][] predicates;
  private String[] cols;
  private Object[] values;
  private Schema schema;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if invalid column names, values, or pedicates
   */
  public Update(AST_Update tree) throws QueryException {
    this.fileName = tree.getFileName();
    this.predicates = tree.getPredicates();
    this.cols = tree.getColumns();
    this.values = tree.getValues();
    this.schema = Minibase.SystemCatalog.getSchema(fileName);

    QueryCheck.tableExists(fileName);
    QueryCheck.insertValues(schema, values);
    QueryCheck.predicates(schema, predicates);
    for (String col : cols) {
      QueryCheck.columnExists(schema, col);
    }
  } // public Update(AST_Update tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

    // print the output message
    System.out.println("0 rows affected. (Not implemented)");

  } // public void execute()

} // class Update implements Plan
