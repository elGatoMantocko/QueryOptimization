package query;

import parser.AST_CreateIndex;
import global.Minibase;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import relop.Tuple;
import relop.Schema;
import relop.FileScan;

/**
 * Execution plan for creating indexes.
 */
class CreateIndex implements Plan {

  private String fileName, tableName, colName;

  private Schema schema;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index already exists or table/column invalid
   */
  public CreateIndex(AST_CreateIndex tree) throws QueryException {

    fileName = tree.getFileName();
    tableName = tree.getIxTable();
    colName = tree.getIxColumn();
    schema = Minibase.SystemCatalog.getSchema(tree.getIxTable());

    // simple parameter check for each of the file names
    QueryCheck.fileNotExists(fileName);
    QueryCheck.tableExists(tableName);
    QueryCheck.columnExists(schema, colName);

    // check that the index doesn't already exist
    IndexDesc[] indecies = Minibase.SystemCatalog.getIndexes(tree.getIxTable());

    for (IndexDesc index : indecies) {
      if (index.columnName.equals(colName)) {
        throw new QueryException("Index already exists.");
      }
    }

  } // public CreateIndex(AST_CreateIndex tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    HashIndex index = new HashIndex(fileName);
    FileScan scan = new FileScan(schema, new HeapFile(tableName));

    int colNum = schema.fieldNumber(colName);
    while (scan.hasNext()) {
      Tuple t = scan.getNext();
      index.insertEntry(new SearchKey(t.getField(colNum)), scan.getLastRID());
    }
    
    scan.close();

    Minibase.SystemCatalog.createIndex(fileName, tableName, colName);

    System.out.println("Index created.");
  } // public void execute()

} // class CreateIndex implements Plan
