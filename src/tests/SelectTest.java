package tests;

import global.Msql;
import parser.TokenMgrError;
import parser.ParseException;
import query.QueryException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import relop.Tuple;

import java.util.List;

public class SelectTest extends MinibaseTest {

  @Before
  public void setUp() {
    super.setUp();
    try {
      //            Msql.execute("DROP TABLE Students;\nQUIT;");
      //            Msql.execute("DROP TABLE Courses;\nQUIT;");
      //            Msql.execute("DROP TABLE Grades;\nQUIT;");
      //            Msql.execute("DROP TABLE Foo;\nQUIT;");

      Msql.execute("CREATE TABLE Students (sid INTEGER, name STRING(50), age FLOAT);\nQUIT;");
      Msql.execute("CREATE TABLE Courses (cid INTEGER, title STRING(50));\nQUIT;");
      Msql.execute("CREATE TABLE Grades (gsid INTEGER, gcid INTEGER, points FLOAT);\nQUIT;");
      Msql.execute("CREATE TABLE Foo (a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER);\nQUIT;");


      Msql.execute("INSERT INTO Students VALUES (1, 'Alice', 25.67);\nQUIT;");
      Msql.execute("INSERT INTO Students VALUES (2, 'Chris', 12.34);\nQUIT;");
      Msql.execute("INSERT INTO Students VALUES (3, 'Bob', 30.0);\nQUIT;");
      Msql.execute("INSERT INTO Students VALUES (4, 'Andy', 50.0);\nQUIT;");
      Msql.execute("INSERT INTO Students VALUES (5, 'Ron', 30.0);\nQUIT;");


      Msql.execute("INSERT INTO Courses VALUES (448, 'DB Fun');\nQUIT;");
      Msql.execute("INSERT INTO Courses VALUES (348, 'Less Cool');\nQUIT;");
      Msql.execute("INSERT INTO Courses VALUES (542, 'More Fun');\nQUIT;");

      Msql.execute("INSERT INTO Grades VALUES (2, 448, 4.0);\nQUIT;");
      Msql.execute("INSERT INTO Grades VALUES (3, 348, 2.5);\nQUIT;");
      Msql.execute("INSERT INTO Grades VALUES (1, 348, 3.1);\nQUIT;");
      Msql.execute("INSERT INTO Grades VALUES (4, 542, 2.8);\nQUIT;");
      Msql.execute("INSERT INTO Grades VALUES (5, 542, 3.0);\nQUIT;");

      Msql.execute("INSERT INTO Foo VALUES (1, 2, 8, 4, 5);\nQUIT;");
      Msql.execute("INSERT INTO Foo VALUES (2, 2, 8, 4, 5);\nQUIT;");
      Msql.execute("INSERT INTO Foo VALUES (1, 5, 3, 4, 5);\nQUIT;");
      Msql.execute("INSERT INTO Foo VALUES (1, 4, 8, 5, 5);\nQUIT;");
      Msql.execute("INSERT INTO Foo VALUES (1, 4, 3, 4, 6);\nQUIT;");
    } catch(ParseException | TokenMgrError | QueryException e){
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testSimpleSelectAll() {
    try {
      List<Tuple> output = Msql.testableexecute("SELECT * FROM Foo;\nQUIT;");
      Assert.assertTrue(0 < output.size());
    } catch(ParseException e){
      e.printStackTrace();
      Assert.fail();
    } catch(TokenMgrError e) {
      e.printStackTrace();
      Assert.fail();
    } catch(QueryException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testSimpleSelectProject() {
    try {
      List<Tuple> output = Msql.testableexecute("SELECT a FROM Foo;\nQUIT;");
      Assert.assertTrue(0 < output.size());
    } catch(ParseException e){
      e.printStackTrace();
      Assert.fail();
    } catch(TokenMgrError e) {
      e.printStackTrace();
      Assert.fail();
    } catch(QueryException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testSimpleSelectSinglePred() {
    try {
      List<Tuple> output = Msql.testableexecute("SELECT * FROM Foo WHERE a = 1;\nQUIT;");
      Assert.assertTrue(0 < output.size());
    } catch(ParseException e){
      e.printStackTrace();
      Assert.fail();
    } catch(TokenMgrError e) {
      e.printStackTrace();
      Assert.fail();
    } catch(QueryException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testSimpleSelectAndPred() {
    try {
      List<Tuple> output = Msql.testableexecute("SELECT * FROM Foo WHERE a = 1 AND b = 2;\nQUIT;");
      Assert.assertTrue(0 < output.size());
    } catch(ParseException | QueryException | TokenMgrError e){
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testSelectCrossOneAndPred() {
    try {
      List<Tuple> output = Msql.testableexecute("SELECT sid, name, points FROM Students, Grades WHERE sid = gsid AND points >= 3.0;\nQUIT;");
      Assert.assertTrue(0 < output.size());
    } catch(ParseException | TokenMgrError | QueryException e){
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testSelectCrossWithOrPred() {
    try {
      List<Tuple> output = Msql.testableexecute("SELECT sid, name, points FROM Students, Grades WHERE sid = gsid AND points >= 3.0 OR sid = gsid AND points <= 2.5;\nQUIT;");
      Assert.assertTrue(0 < output.size());
    } catch(ParseException e){
      e.printStackTrace();
      Assert.fail();
    } catch(TokenMgrError e) {
      e.printStackTrace();
      Assert.fail();
    } catch(QueryException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testSelectFooComplexPred() {
    try {
      List<Tuple> output = Msql.testableexecute("SELECT * FROM Foo WHERE a = 1 and b = 2 or c = 3 and d = 4 and e = 5;\nQUIT;");
      Assert.assertTrue(0 < output.size());
    } catch(ParseException e){
      e.printStackTrace();
      Assert.fail();
    } catch(TokenMgrError e) {
      e.printStackTrace();
      Assert.fail();
    } catch(QueryException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testSelectStudentsGradesAge() {
    try {
      List<Tuple> output = Msql.testableexecute("SELECT * FROM Students, Grades WHERE sid = gsid AND age = 30.0;\nQUIT;");
      Assert.assertTrue(0 < output.size());
    } catch (ParseException e) {
      e.printStackTrace();
      Assert.fail();
    } catch (TokenMgrError e) {
      e.printStackTrace();
      Assert.fail();
    } catch (QueryException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}

