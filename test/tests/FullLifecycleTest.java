package tests;

import global.Msql;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by david on 4/4/2016.
 */
public class FullLifecycleTest extends MinibaseTest {

    @Before
    public void setUp() throws Exception{
        super.setUp();

        Msql.execute("CREATE TABLE Courses (cid INTEGER, title STRING(50));\nQUIT;");
        Msql.execute("CREATE TABLE Grades (gsid INTEGER, gcid INTEGER, points FLOAT);\nQUIT;");
        Msql.execute("CREATE TABLE Foo (a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER);\nQUIT;");

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
    public void testAll() throws Exception {
        try {
            Msql.execute(
                    "CREATE TABLE Students (sid INTEGER, name STRING(50), age FLOAT);\n" +
                            "CREATE INDEX IX_Age ON Students(Age);\n" +
                            "INSERT INTO Students VALUES (1, 'Alice', 25.67);\n" +
                            "INSERT INTO Students VALUES (2, 'Chris', 12.34);\n" +
                            "INSERT INTO Students VALUES (3, 'Bob', 30.0);\n" +
                            "INSERT INTO Students VALUES (4, 'Andy', 50.0);\n" +
                            "INSERT INTO Students VALUES (5, 'Ron', 30.0);\n" +
                            "CREATE INDEX IX_Name ON Students(Name);\n" +
                            "explain SELECT * FROM Students;\n" +
                            "explain SELECT * FROM Students WHERE age = 30.0;\n" +
                            "explain SELECT sid, name, points FROM Students, Grades WHERE sid = gsid AND points >= 3.0;\n" +
                            "explain SELECT sid, name, points FROM Students, Grades WHERE sid = gsid AND points >= 3.0 OR sid = gsid AND points <= 2.5;\n" +
                            "explain SELECT * FROM Students, Grades WHERE sid = gsid AND age = 30.0;\n" +
                            "STATS\n" +
//                            "QUIT;");
//            Msql.execute(//bad query
                            "SELECT * FROM Students, Bad;\n" +
                            "QUIT;");
        } catch(Exception e) {
            Msql.execute("STATS\n" +
                    "DESCRIBE Students;\n" +
                    "UPDATE Students SET sid = 5 WHERE name = 'Chris';\n" +
                    "DELETE Students WHERE name = 'Chris';\n" +
                    "DROP INDEX IX_Age;\n" +
                    "DROP INDEX IX_Name;\n" +
                    "DROP TABLE Students;\n" +
                    "QUIT;");
        }
    }
}
