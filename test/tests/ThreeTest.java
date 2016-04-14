package tests;

import global.Msql;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by david on 4/11/16.
 */
public class ThreeTest extends MinibaseTest {

    @Before
    public void setUp() throws Exception {
        super.setUp();
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

    }

    @Test
    public void testSimpleThree() throws Exception {
        Msql.execute("SELECT * FROM Students, Grades, Courses where sid = gsid AND cid = gcid;\nQUIT;");
    }

    @Test
    public void testComplexThree() throws Exception {
        Msql.execute("SELECT * FROM Students, Grades, Courses where sid = gsid AND cid = gcid AND points >= 3.0 OR sid = gsid AND cid = gcid AND cid >= 400;\nQUIT;");
    }

    @Test
    public void testComplexThreeProj() throws Exception {
        Msql.execute("SELECT name, cid, points FROM Students, Grades, Courses where sid = gsid AND cid = gcid AND points >= 3.0 OR sid = gsid AND cid = gcid AND cid >= 400;\nQUIT;");
    }

    @Test
    public void testComplex() throws Exception {
        Msql.execute("SELECT name, cid, points FROM Students, Grades, Courses where sid = gsid AND cid = gcid;\nQUIT;");
    }

    @Test
    public void testCrossThreeProj() throws Exception {
        Msql.execute("SELECT name, cid, points FROM Students, Grades, Courses where points >= 3.0 OR cid >= 400;\nQUIT;");
    }

    @Test
    public void testSimpleFour() throws Exception {
        Msql.execute("explain SELECT name, cid, points FROM Students, Grades, Courses, Foo where sid = gsid AND cid = gcid AND points >= 3.0 OR sid = gsid AND cid = gcid AND cid >= 400;\nQUIT;");
        Msql.execute("SELECT name, cid, points FROM Students, Grades, Courses, Foo where sid = gsid AND cid = gcid AND points >= 3.0 OR sid = gsid AND cid = gcid AND cid >= 400;\nQUIT;");
    }
}