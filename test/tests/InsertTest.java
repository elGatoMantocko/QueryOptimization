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
  public void setUp() throws Exception {
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
  public void testInsertGoodRow() throws TokenMgrError, ParseException, QueryException {
    boolean passes = false;
    Msql.execute("INSERT INTO Students VALUES (1, 'Alice', 25.67);\nQUIT;");

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

  @Test
  public void testRecordStatsUpdated() throws ParseException, QueryException, TokenMgrError {
    Msql.execute("INSERT INTO Students VALUES (1, 'Alice', 25.67);\nINSERT INTO Students VALUES (2, 'Chris', 12.34);\nQUIT");

    Assert.assertEquals("The record stats weren't updated properly", 2, Minibase.SystemCatalog.getRecCount("Students"));
  }

  @Test (expected=QueryException.class)
  public void testInsertBadRow() throws QueryException, ParseException, TokenMgrError {
    Msql.execute("INSERT INTO Students VALUES (1, 'Alice', 25.67, 'test');\nQUIT;");
  }

}
