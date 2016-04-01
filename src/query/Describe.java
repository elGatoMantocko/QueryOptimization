package query;

import global.Minibase;

import parser.AST_Describe;

/**
 * Execution plan for describing tables.
 */
class Describe implements Plan {

  private String fileName;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exist
   */
  public Describe(AST_Describe tree) throws QueryException {
    fileName = tree.getFileName();
    QueryCheck.tableExists(fileName);
  } // public Describe(AST_Describe tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    Minibase.SystemCatalog.getSchema(fileName).print();
  } // public void execute()

} // class Describe implements Plan
