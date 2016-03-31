package query;

import parser.AST_Insert;

/**
 * Execution plan for inserting tuples.
 */
class Insert implements Plan {

  private String fileName;
  private Object[] values;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exists or values are invalid
   */
  public Insert(AST_Insert tree) throws QueryException {

    fileName = tree.getFileName();
    values = tree.getValues();

    QueryCheck.fileNotExists(fileName);

  } // public Insert(AST_Insert tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

    // print the output message
    System.out.println("0 rows affected. (Not implemented)");

  } // public void execute()

} // class Insert implements Plan
