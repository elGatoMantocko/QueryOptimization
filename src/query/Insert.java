package query;

import global.Minibase;

import parser.AST_Insert;
import relop.Schema;

/**
 * Execution plan for inserting tuples.
 */
class Insert implements Plan {

  private String fileName;
  private Object[] fields;
  private Schema schema;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exists or values are invalid
   */
  public Insert(AST_Insert tree) throws QueryException {
    this.fileName = tree.getFileName();
    this.fields = tree.getValues();

    QueryCheck.tableExists(fileName);

    this.schema = Minibase.SystemCatalog.getSchema(fileName);

    QueryCheck.insertValues(schema, fields);
  } // public Insert(AST_Insert tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

    // print the output message
    System.out.println("0 rows affected. (Not implemented)");

  } // public void execute()

} // class Insert implements Plan
