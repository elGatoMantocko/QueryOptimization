package query;

import global.Minibase;
import global.RID;
import global.SearchKey;
import parser.AST_Insert;
import heap.HeapFile;
import index.HashIndex;
import relop.Tuple;
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
    Tuple tup = new Tuple(schema, fields);
    HeapFile file = new HeapFile(fileName);

    RID rid = file.insertRecord(tup.getData());

    for (IndexDesc desc : Minibase.SystemCatalog.getIndexes(fileName)) {
      HashIndex index = new HashIndex(desc.indexName);
      index.insertEntry(new SearchKey(tup.getField(desc.columnName)), rid);
    }

    System.out.println("1 row inserted.");
  } // public void execute()

} // class Insert implements Plan
