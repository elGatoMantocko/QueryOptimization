package tests;

import global.Msql;
import parser.ParseException;
import parser.TokenMgrError;
import query.QueryException;

import org.junit.Before;
import org.junit.Test;

public class UpdateTest extends MinibaseTest {

  @Before
  public void setUp() {
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

    // TODO: Verify update
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

    // TODO: Verify update
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
