package tests;

import global.Msql;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import relop.Tuple;
import query.QueryException;

import java.util.*;

public class SelectTest extends MinibaseTest {

  @Before
  public void setUp() throws Exception{
    super.setUp();
    //            Msql.execute("DROP TABLE Students;\nQUIT;");
    //            Msql.execute("DROP TABLE Courses;\nQUIT;");
    //            Msql.execute("DROP TABLE Grades;\nQUIT;");
    //            Msql.execute("DROP TABLE Foo;\nQUIT;");

    Msql.execute("CREATE TABLE Students (sid INTEGER, name STRING(50), age FLOAT);\nQUIT;");
    Msql.execute("CREATE TABLE Courses (cid INTEGER, title STRING(50));\nQUIT;");
    Msql.execute("CREATE TABLE Grades (gsid INTEGER, gcid INTEGER, points FLOAT);\nQUIT;");
    Msql.execute("CREATE TABLE Foo (a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER);\nQUIT;");

    Msql.execute("CREATE INDEX IX_Age ON Students(age);\n" +
            "CREATE INDEX IX_Points ON Grades(points);\n" +
            "CREATE INDEX IX_Name ON Students(name);\n" +
            "QUIT;");

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

  }

  @Test
  public void testIndexSelect() throws Exception {
    Msql.execute("explain SELECT * FROM Students WHERE name = 'Alica'; QUIT;");
  }

  @Test
  public void testSelectNoTuples() throws Exception {
    List<Tuple> output = Msql.testableexecute("SELECT * FROM Foo WHERE a = 5;\nQUIT;");
    Assert.assertEquals("Expected different number of tuples", 0, output.size());
  }

  @Test
  public void testSimpleSelectAll() throws Exception {
    List<Tuple> output = Msql.testableexecute("SELECT * FROM Foo;\nQUIT;");
    Assert.assertEquals("Expected different number of tuples", 5, output.size());
  }

  @Test
  public void testSimpleSelectProject() throws Exception {
    List<Tuple> output = Msql.testableexecute("SELECT a FROM Foo;\nQUIT;");
    Assert.assertEquals("Expected different number of tuples", 5, output.size());
    for (Tuple t : output) {
      Assert.assertEquals("Expected different number of tuples", 1, t.getAllFields().length);
    }
  }

  @Test
  public void testSimpleSelectSinglePred() throws Exception {
    List<Tuple> output = Msql.testableexecute("SELECT * FROM Foo WHERE a = 1;\nQUIT;");
    Assert.assertEquals("Expected different number of tuples", 4, output.size());
  }

  @Test
  public void testSimpleSelectAndPred() throws Exception {
    List<Tuple> output = Msql.testableexecute("SELECT * FROM Foo WHERE a = 1 AND b = 2;\nQUIT;");
    Assert.assertEquals("Expected different number of tuples", 1, output.size());
  }

  @Test
  public void testSelectCrossOneAndPred() throws Exception {
    List<Tuple> output = Msql.testableexecute("SELECT sid, name, points FROM Students, Grades WHERE sid = gsid AND points >= 3.0;\nQUIT;");
    // this will throw an exception and fail if the projection is wrong
    for (Tuple t : output) {
      t.getField("sid");
      t.getField("name");
      t.getField("points");
    }
    Assert.assertEquals("Expected different number of tuples", 3, output.size());
  }

  @Test
  public void testSelectCrossWithOrPred() throws Exception {
    List<Tuple> output = Msql.testableexecute("SELECT sid, name, points FROM Students, Grades WHERE sid = gsid AND points >= 3.0 OR sid = gsid AND points <= 2.5;\nQUIT;");
    Assert.assertEquals("Expected different number of tuples", 4, output.size());
  }

  @Test
  public void testSelectFooComplexPred() throws Exception {
    List<Tuple> output = Msql.testableexecute("SELECT * FROM Foo WHERE a = 1 and b = 2 or c = 3 and d = 4 and e = 5;\nQUIT;");
    Assert.assertEquals("Expected different number of tuples", 2, output.size());
  }

  @Test
  public void testSelectStudentsGradesAge() throws Exception {
    List<Tuple> output = Msql.testableexecute("SELECT * FROM Students, Grades WHERE sid = gsid AND age = 30.0;\nQUIT;");
    Assert.assertEquals("Expected different number of tuples", 2, output.size());
  }

  @Test
  public void testSimpleThree() throws Exception {
    List<Tuple> output = Msql.testableexecute("SELECT * FROM Students, Grades, Courses where sid = gsid AND cid = gcid;\nQUIT;");
    Assert.assertEquals("Expected different number of tuples", 5, output.size());
  }

  @Test
  public void testComplexThree() throws Exception {
    List<Tuple> output = Msql.testableexecute("SELECT * FROM Students, Grades, Courses where sid = gsid AND cid = gcid AND points >= 3.0 OR sid = gsid AND cid = gcid AND cid >= 400;\nQUIT;");
    Assert.assertEquals("Expected different number of tuples", 4, output.size());
  }

  @Test
  public void testComplexThreeProj() throws Exception {
    List<Tuple> output = Msql.testableexecute("SELECT name, cid, points FROM Students, Grades, Courses where sid = gsid AND cid = gcid AND points >= 3.0 OR sid = gsid AND cid = gcid AND cid >= 400;\nQUIT;");
    Assert.assertEquals("Expected different number of tuples", 4, output.size());
  }

  @Test
  public void testComplex() throws Exception {
    List<Tuple> output = Msql.testableexecute("SELECT name, cid, points FROM Students, Grades, Courses where sid = gsid AND cid = gcid;\nQUIT;");
    Assert.assertEquals("Expected different number of tuples", 5, output.size());
  }

  @Test
  public void testCrossThreeProj() throws Exception {
    List<Tuple> output = Msql.testableexecute("SELECT name, cid, points FROM Students, Grades, Courses where points >= 3.0 OR cid >= 400;\nQUIT;");
    Assert.assertEquals("Expected different number of tuples", 65, output.size());
  }

  @Test
  public void testSimpleFour() throws Exception {
    List<Tuple> output = Msql.testableexecute("SELECT name, cid, points FROM Students, Grades, Courses, Foo where sid = gsid AND cid = gcid AND points >= 3.0 OR sid = gsid AND cid = gcid AND cid >= 400;\nQUIT;");
    Assert.assertEquals("Expected different number of tuples", 20, output.size());
  }
  
  @Test (expected=QueryException.class)
  public void testSelectInvalidTable() throws Exception {
    Msql.execute("SELECT * FROM Grades, Bad;\nQUIT");
  }
  
  @Test (expected=QueryException.class)
  public void testSelectInvalidColumn() throws Exception {
    Msql.execute("SELECT sid, bad FROM Grades, Students;\nQUIT");
  }
  
  @Test (expected=QueryException.class)
  public void testSelectInvalidPredicates() throws Exception {
    Msql.execute("SELECT * FROM Foo, Grades WHERE a = 1 OR points = 0.0 OR bad = 5;\nQUIT");
  }
}

