package edu.depauw.csc480.projectv1;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;

import org.apache.derby.jdbc.EmbeddedDriver;

/**
 * This is an example of a menu-driven client for Sciore's student database. It
 * uses straight JDBC to execute SQL queries and commands.
 *
 * @author bhoward
 */
public class Main {
	private static final Scanner in = new Scanner(System.in);
	private static final PrintStream out = System.out;

	public static void main(String[] args) {
		try (Connection conn = getConnection("jdbc:derby:db/studentdb")) {
			displayMenu();
			loop: while (true) {
				switch (requestString("Selection (0 to quit, 9 for menu)? ")) {
				case "0": // Quit
					break loop;

				case "1": // Reset
					resetTables(conn);
					break;

				case "2": // List students
					listBooks(conn);
					//listBooks
					break;

				case "3": // Show transcript
					showBooksRentedByStudent(conn);
					//show books by a student
					break;

				case "4": // Add student
					addStudent(conn);

					break;

				case "5": // Add enrollment
					addBook(conn);
					//add book
					break;

				case "6": // Change grade
					listByDept(conn);
					//list books by a department listByDepartment
					break;

				default:
					displayMenu();
					break;
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		out.println("Done");
	}

	/**
	 * Attempt to open a connection to an embedded Derby database at the given URL.
	 * If the database does not exist, create it with empty tables.
	 *
	 * @param url
	 * @return
	 */
	private static Connection getConnection(String url) {
		Driver driver = new EmbeddedDriver();

		// try to connect to an existing database
		Properties prop = new Properties();
		prop.put("create", "false");
		try {
			Connection conn = driver.connect(url, prop);
			return conn;
		} catch (SQLException e) {
			// database doesn't exist, so try creating it
			try {
				prop.put("create", "true");
				Connection conn = driver.connect(url, prop);
				createTables(conn);
				return conn;
			} catch (SQLException e2) {
				throw new RuntimeException("cannot connect to database", e2);
			}
		}
	}

	private static void displayMenu() {
		out.println("0: Quit");
		out.println("1: Reset tables");
		out.println("2: list of books");
		out.println("3: Show books rented by a student");
		out.println("4: Add student");
		out.println("5: Add book");
		out.println("6: List Books by department");
	}

	private static String requestString(String prompt) {
		out.print(prompt);
		out.flush();
		return in.nextLine();
	}

	private static void createTables(Connection conn) {
		// First clean up from previous runs, if any
		dropTables(conn);

		// Now create the schema
		addTables(conn);
	}

	private static void doUpdate(Connection conn, String statement, String message) {
		try (Statement stmt = conn.createStatement()) {
			stmt.executeUpdate(statement);
			System.out.println(message);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static void doUpdateNoError(Connection conn, String statement, String message) {
		try (Statement stmt = conn.createStatement()) {
			stmt.executeUpdate(statement);
			System.out.println(message);
		} catch (SQLException e) {
			// Ignore error
		}
	}

	/**
	 * Create the tables for the student database from Sciore. Note that the tables
	 * have to be created in a particular order, so that foreign key references
	 * point to already-created tables. This allows the simpler technique of
	 * creating the tables directly with their f.k. constraints, rather than
	 * altering the tables later to add constraints.
	 *
	 * @param conn
	 */
	private static void addTables(Connection conn) {
		StringBuilder sb = new StringBuilder();
		sb.append("create table DEPT(");
      	sb.append("  DId int,");
      	sb.append("  DName varchar(10) not null,");
      	sb.append("  DLocation varchar(10) not null,");
      	sb.append("  primary key (DId)");
      	sb.append(")");
      	doUpdate(conn, sb.toString(), "Table DEPT created.");

		sb = new StringBuilder();
		sb.append("create table STUDENT(");
      	sb.append("  SId int,");
      	sb.append("  SName varchar(100) not null,");
      	sb.append("  SEmail varchar(100) not null,");
      	sb.append("  Phone varchar(12) not null,");
      	sb.append("  primary key (SId)");
      	sb.append(")");
      	doUpdate(conn, sb.toString(), "Table STUDENT created.");


		sb = new StringBuilder();
		sb.append("create table BOOK(");
      	sb.append("  BISBN int,");
      	sb.append("  BTitle varchar(150) not null,");
      	sb.append("  BAuthor varchar(100) not null,");
      	sb.append("  DEPT_Did int,");
      	sb.append("  CHECKOUT_isReturn boolean,");
      	sb.append("  primary key (BISBN),");
      	sb.append("  foreign key (DEPT_Did) references DEPT on delete set null");
      	sb.append(")");
      	doUpdate(conn, sb.toString(), "Table STUDENT created.");


		sb = new StringBuilder();
		sb.append("create table CHECKOUT(");
      	sb.append("  CId int,");
      	sb.append("  STUDENT_SId int,");
      	sb.append("  BOOK_BSIBN int,");
      	sb.append("  isReturn Boolean not null,");
      	sb.append("  primary key (CId),");
      	sb.append("  foreign key (BOOK_BSIBN) references BOOK on delete set null,");
      	sb.append("  foreign key (STUDENT_SId) references STUDENT on delete set null");
      	sb.append(")");
      	doUpdate(conn, sb.toString(), "Table CHECKOUT created.");



	}

	/**
	 * Delete the tables for the student database. Note that the tables are dropped
	 * in the reverse order that they were created, to satisfy referential integrity
	 * (foreign key) constraints.
	 *
	 * @param conn
	 */
	private static void dropTables(Connection conn) {
	 	doUpdateNoError(conn, "drop table CHECKOUT", "Table CHECKOUT dropped.");
      	doUpdateNoError(conn, "drop table BOOK", "Table BOOK dropped.");
      	doUpdateNoError(conn, "drop table STUDENT", "Table STUDENT dropped.");
      	doUpdateNoError(conn, "drop table DEPT", "Table DEPT dropped.");
	}

	/**
	 * Delete the contents of the tables, then reinsert the sample data from Sciore.
	 * Again, note that the order is important, so that foreign key references
	 * already exist before they are used.
	 *
	 * @param conn
	 */
	private static void resetTables(Connection conn) {
		try (Statement stmt = conn.createStatement()) {
			int count = 0;
			count += stmt.executeUpdate("delete from BOOK");
			count += stmt.executeUpdate("delete from DEPT");
			count += stmt.executeUpdate("delete from CHECKOUT");
			count += stmt.executeUpdate("delete from STUDENT");
			System.out.println(count + " records deleted");

			String[] deptvals = {
					"(10, 'compsci', 'JULIAN'')", "(20, 'math', 'julian')", "(30, 'drama', 'Asbury')"

			};
			count = 0;
			for (String val : deptvals) {
				count += stmt.executeUpdate("insert into DEPT(DId, DName, DLocation) values " + val);
			}
			System.out.println(count + " DEPT records inserted.");

			String[] studvals = {
					"(1, 'joe', 'joe_2024@depauw.edu', '7657122287')",
					"(2, 'amy', 'amy_2024@depauw.edu', '7657122288')",
					"(3, 'max', 'max_2024@depauw.edu', '7657122289')"

			};
			count = 0;
			for (String val : studvals) {
				count += stmt.executeUpdate("insert into STUDENT(SId, SName, SEmail, Phone) values " + val);
			}
			System.out.println(count + " STUDENT records inserted.");

			String[] coursevals = {
					"(5564, 'Harry potter', 'jk rowling', 30, true)",
				   	"(5563, '100 python days', 'unknown', 10, true)"

			};
			count = 0;
			for (String val : coursevals) {
				count += stmt.executeUpdate("insert into BOOK(BISBN, BTitle, BAuthor, DEPT_Did, CHECKOUT_isReturn ) values " + val);
			}
			System.out.println(count + " BOOK records inserted.");

			String[] sectvals = {
					"(1, 1, 5564, true)",
					"(2, 3, 5563, false)"

			};
			count = 0;
			for (String val : sectvals) {
				count += stmt.executeUpdate("insert into CHECKOUT(CId, STUDENT_SId, BOOK_BSIBN, isReturn) values " + val);
			}
			System.out.println(count + " CHECKOUT records inserted.");

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Print a table of all students with their id number, name, graduation year,
	 * and major.
	 *
	 * @param conn
	 */
	private static void listBooks(Connection conn) {
		StringBuilder query = new StringBuilder();
		query.append("select *");
		query.append("  from BOOK b");


		try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query.toString())) {
			out.printf("%-3s %-8s %-20s %-4s %-8s\n", "ISBN", "Title", "Author", "Dept ID", "isReturned");
			out.println("----------------------------");
			while (rs.next()) {
				int bISBN = rs.getInt("BISBN");
				String BTitle = rs.getString("BTitle");
				int BAuthor = rs.getInt("BAuthor");
				String dID = rs.getString("DEPT_Did");
				boolean isRet = rs.getBoolean("CHECKOUT_isReturn");
//CHANGE BOOLEAN IF IT DOESNT WORK
				out.printf("%-3s %-8s %-20s %-4s %-8s\n", bISBN, BTitle, BAuthor , dID, isRet);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Request a student name and print a table of their course enrollments.
	 *
	 * @param conn
	 */
	private static void showBooksRentedByStudent(Connection conn) {
		String sname = requestString("Student name? ");

		StringBuilder query = new StringBuilder();
		query.append("select s.SName, s.SId, s.SEmail, b.BISBN, b.BTitle, b.BAuthor");
		query.append("  from STUDENT s, BOOK b, CHECKOUT c");
		query.append("  where c.BOOK_BISBN = b.BISBN");
		query.append("    and c.STUDENT_SId = s.SId");
		query.append("    and c.isReturn = true");


		try (PreparedStatement pstmt = conn.prepareStatement(query.toString())) {
			pstmt.setString(1, sname);
			ResultSet rs = pstmt.executeQuery();

			out.printf("%-3s %-8s %-20s %-4s %-8s %-5s\n", "SName", "Student ID", "Email", "ISBN", "Title", "Author");
			out.println("-----------------------------------------------------");
			while (rs.next()) {
				int sName = rs.getInt("SName");
				String sID = rs.getString("SId");
				String sEmail = rs.getString("SEmail");
				int bISBN = rs.getInt("BISBN");
				String bTitle = rs.getString("BTitle");
				String bAuthor = rs.getString("BAuthor");

				out.printf("%3d %-8s %-20s %-4d %-8s %-5s\n", sName, sID, sEmail, bISBN, bTitle, bAuthor);
			}

			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Request information to add a new student to the database. The id number must
	 * be unique, and the major must be an existing department name.
	 *
	 * @param conn
	 */
	private static void addStudent(Connection conn) {
		String sid = requestString("Id number? ");
		String sname = requestString("Student name? ");
		String email = requestString(" Student email? ");
		String phone = requestString(" Phone number? ");

		StringBuilder command = new StringBuilder();
		command.append("insert into STUDENT(SId, SName, SEmail, Phone)");
		command.append("  select ?, ?, ?, ?");
		command.append("  from STUDENT");
		command.append("  where d.DName = ?");

		try (PreparedStatement pstmt = conn.prepareStatement(command.toString())) {
			pstmt.setString(1, sid);
			pstmt.setString(2, sname);
			pstmt.setString(3, email);
			pstmt.setString(4, phone);
			int count = pstmt.executeUpdate();

			out.println(count + " student(s) inserted");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Request information to add an enrollment record for a student. The id number
	 * must be unique, the student name and course title must exist, and the course
	 * must have been offered (exactly once) in the given year. The grade is set to
	 * NULL.
	 *
	 * @param conn
	 */
	private static void addBook(Connection conn) {
		String isbn = requestString(" ISBN ?  ");
		String title = requestString("Title? ");
		String author = requestString("Author ? ");
		String dID = requestString("Department ID? ");

		StringBuilder command = new StringBuilder();
		command.append("insert into BOOK(BISBN, BTitle, BAuthor, DEPT_Did)");
		command.append("  select ?, ?, ?, d.DEPT_Did");
		command.append("  from Book b, DEPT d");
		command.append("  where b.BISBN = ?");
		command.append("    and b.BTitle = ?");
		command.append("    and b.BAuthor = ?");
		command.append("    and b.DEPT_Did = d.DId");

		try (PreparedStatement pstmt = conn.prepareStatement(command.toString())) {
			pstmt.setString(1, isbn);
			pstmt.setString(2, title);
			pstmt.setString(3, author);
			pstmt.setString(4, dID);
			int count = pstmt.executeUpdate();

			out.println(count + " record(s) inserted");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Request an enrollment id and a new grade to be entered, then update the
	 * enrollment table accordingly.
	 *
	 * @param conn
	 */
	private static void listByDept(Connection conn) {
		String dID = requestString(" Department #? ");


		StringBuilder command = new StringBuilder();
		command.append("SELECT b.*");
		command.append("  FROM BOOK b, DEPT d");
		command.append("  where b.DEPT_Did = D.DId");

		try (PreparedStatement pstmt = conn.prepareStatement(command.toString())) {
			pstmt.setString(1, dID);

			int count = pstmt.executeUpdate();

			out.println(count + " record(s) updated");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
