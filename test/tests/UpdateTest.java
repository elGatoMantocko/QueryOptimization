package tests;

import global.Minibase;
import global.Msql;
import heap.HeapFile;
import relop.Tuple;
import relop.FileScan;
import parser.ParseException;
import parser.TokenMgrError;
import query.QueryException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class UpdateTest extends MinibaseTest {

  @Before
  public void setUp() throws Exception {
    super.setUp();
    try {
      Msql.execute("CREATE TABLE Students (sid INTEGER, name STRING(50), age FLOAT);\nCREATE INDEX IX_Age ON Students(Age);\nQUIT;");
      Msql.execute("INSERT INTO Students VALUES (1, 'Alice', 25.67);\nINSERT INTO Students VALUES (2, 'Chris', 12.34);\nINSERT INTO Students VALUES (3, 'Bob', 30.0);\nINSERT INTO Students VALUES (4, 'Andy', 50.0);\nINSERT INTO Students VALUES (5, 'Ron', 30.0);\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    } catch(QueryException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testUpdateOneRow() {
    try {
      Msql.execute("UPDATE Students SET sid = 5 WHERE name = 'Chris';\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    } catch(QueryException e) {
      e.printStackTrace();
    }

    HeapFile file = new HeapFile("Students");
    FileScan scan = new FileScan(Minibase.SystemCatalog.getSchema("Students"), file);

    while (scan.hasNext()) {
      Tuple t = scan.getNext();
      if (t.getField("name").equals("Chris")) {
        Assert.assertEquals("The sid for 'Chris' wasn't updated properly", 5, t.getField("sid"));
      }
    }

    scan.close();
  }

  @Test
  public void testUpdateMultipleRows() {
    try {
      Msql.execute("UPDATE Students SET sid = 5 WHERE name = 'Chris' or name = 'Alice';\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    } catch(QueryException e) {
      e.printStackTrace();
    }

    HeapFile file = new HeapFile("Students");
    FileScan scan = new FileScan(Minibase.SystemCatalog.getSchema("Students"), file);

    while (scan.hasNext()) {
      Tuple t = scan.getNext();
      if (t.getField("name").equals("Chris")) {
        Assert.assertEquals("The sid for 'Chris' wasn't updated properly", 5, t.getField("sid"));
      }

      if (t.getField("name").equals("Alice")) {
        Assert.assertEquals("The sid for 'Alice' wasn't updated properly", 5, t.getField("sid"));
      }
    }

    scan.close();
  }

  @Test
  public void testUpdateAllRows() {
    try {
      Msql.execute("UPDATE Students SET name = 'Test';\nQUIT;");
    } catch(ParseException | TokenMgrError | QueryException e){
      e.printStackTrace();
    } 

    HeapFile file = new HeapFile("Students");
    FileScan scan = new FileScan(Minibase.SystemCatalog.getSchema("Students"), file);

    while (scan.hasNext()) {
      Tuple t = scan.getNext();
      Assert.assertEquals("The name should be set to \'Test\'", "Test", t.getField("name"));
    }

    scan.close();
  }

  @Test
  public void testUpdateNoRows() {
    try {
      Msql.execute("UPDATE Students SET sid = 10 WHERE name = 'Elliott';\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    } catch(QueryException e) {
      e.printStackTrace();
    }

    HeapFile file = new HeapFile("Students");
    FileScan scan = new FileScan(Minibase.SystemCatalog.getSchema("Students"), file);

    while (scan.hasNext()) {
      Tuple t = scan.getNext();
      Assert.assertFalse("None of the SIDs should be 10", (int)t.getField("sid") == 10);
    }

    scan.close();
  }

  @Test (expected=QueryException.class)
  public void testUpdateBadTableName() throws QueryException {
    try {
      Msql.execute("UPDATE Bad SET Id = 1;\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    }
  }

  @Test (expected=QueryException.class)
  public void testUpdateBadColumnName() throws QueryException {
    try {
      Msql.execute("UPDATE Courses SET bad = 1;\nQUIT;");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    }
  }

  @Test (expected=QueryException.class)
  public void testUpdateBadColumnValue() throws QueryException {
    try {
      Msql.execute("UPDATE Courses SET cid = 'bad';");
    } catch(ParseException e){
      e.printStackTrace();
    } catch(TokenMgrError e) {
      e.printStackTrace();
    }
  }

}
