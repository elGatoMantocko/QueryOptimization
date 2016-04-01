package tests;

import global.Minibase;
import global.Msql;
import heap.HeapFile;
import parser.TokenMgrError;
import parser.ParseException;
import query.QueryException;
import relop.Schema;
import relop.Tuple;
import relop.FileScan;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class InsertTest extends MinibaseTest {

  @Before
  public void setUp() {
    super.setUp();
    try {
      Msql.execute("CREATE TABLE Students (sid INTEGER, name STRING(50), age FLOAT);\nCREATE INDEX IX_Age ON Students(Age);\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    } catch(QueryException e) {
      e.printStackTrace();
    }
  }
  
  @Test
  public void testInsertGoodRow() {
    boolean passes = false;
    try {
      Msql.execute("INSERT INTO Students VALUES (1, 'Alice', 25.67);\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    } catch(QueryException e) {
      e.printStackTrace();
    }

    Schema schema = Minibase.SystemCatalog.getSchema("Students");

    HeapFile file = new HeapFile("Students");
    FileScan scan = new FileScan(schema, file);

    // have to do a pseudo select
    while (scan.hasNext()) {
      Tuple t = scan.getNext();
      Object[] fields = t.getAllFields();
      
      if ((int)fields[0] == 1 && // sid
          ((String)fields[1]).equals("Alice") && // name
          (new Float((float)fields[2]).equals(new Float(25.67)))) { // age
        passes = true;
      }
    }

    Assert.assertTrue("No row with specified data was found.", passes);
  }

  @Test (expected=QueryException.class)
  public void testInsertBadRow() throws QueryException {
    try {
      Msql.execute("INSERT INTO Students VALUES (1, 'Alice', 25.67, 'test');\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    } 
  }

}
