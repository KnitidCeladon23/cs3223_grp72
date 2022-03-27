package simpledb.test;

import simpledb.plan.Planner;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class CreateStudentDB {
	public static void main(String[] args) {
		// analogous to the driver
		SimpleDB db = new SimpleDB("studentdb");

		// analogous to the connection
		Transaction tx  = db.newTx();
		Planner planner1 = db.planner();


		String s1 = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
		planner1.executeUpdate(s1, tx);
		System.out.println("Table STUDENT created.");
		
		// s1 = "create index studentid on STUDENT(sid) using btree";
		// planner1.executeUpdate(s1, tx);

		s1 = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
		String[] studvals1 = {"(1, 'joe', 10, 2021)",
				"(2, 'amy', 20, 2020)",
				"(3, 'max', 10, 2022)",
				"(4, 'sue', 20, 2022)",
				"(5, 'bob', 30, 2020)",
				"(6, 'kim', 20, 2020)",
				"(7, 'art', 30, 2021)",
				"(8, 'pat', 20, 2019)",
		"(9, 'lee', 10, 2021)"};
		for (int i=0; i<studvals1.length; i++)
			planner1.executeUpdate(s1 + studvals1[i], tx);
		System.out.println("STUDENT records inserted.");

		s1 = "create table DEPT(DId int, DName varchar(8))";
		planner1.executeUpdate(s1, tx);
		System.out.println("Table DEPT created.");

		s1 = "insert into DEPT(DId, DName) values ";
		String[] deptvals1 = {"(10, 'compsci')",
				"(20, 'math')",
		"(30, 'drama')"};
		for (int i=0; i<deptvals1.length; i++)
			planner1.executeUpdate(s1 + deptvals1[i], tx);
		System.out.println("DEPT records inserted.");

		s1 = "create table COURSE(CId int, Title varchar(20), DeptId int)";
		planner1.executeUpdate(s1, tx);
		System.out.println("Table COURSE created.");

		s1 = "insert into COURSE(CId, Title, DeptId) values ";
		String[] coursevals1 = {"(12, 'db systems', 10)",
				"(22, 'compilers', 10)",
				"(32, 'calculus', 20)",
				"(42, 'algebra', 20)",
				"(52, 'acting', 30)",
		"(62, 'elocution', 30)"};
		for (int i=0; i<coursevals1.length; i++)
			planner1.executeUpdate(s1 + coursevals1[i], tx);
		System.out.println("COURSE records inserted.");

		s1 = "create table SECTION(SectId int, CourseId int, Prof varchar(8), YearOffered int)";
		planner1.executeUpdate(s1, tx);
		System.out.println("Table SECTION created.");

		s1 = "insert into SECTION(SectId, CourseId, Prof, YearOffered) values ";
		String[] sectvals1 = {"(13, 12, 'turing', 2018)",
				"(23, 12, 'turing', 2019)",
				"(33, 32, 'newton', 2019)",
				"(43, 32, 'einstein', 2017)",
		"(53, 62, 'brando', 2018)"};
		for (int i=0; i<sectvals1.length; i++)
			planner1.executeUpdate(s1 + sectvals1[i], tx);
		System.out.println("SECTION records inserted.");

		s1 = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))";
		planner1.executeUpdate(s1, tx);
		System.out.println("Table ENROLL created.");
		
		s1 = "create index testindex on ENROLL(studentID) using hash";
		planner1.executeUpdate(s1, tx);

		s1 = "insert into ENROLL(EId, StudentId, SectionId, Grade) values ";
		String[] enrollvals1 = {"(14, 1, 13, 'A')",
				"(24, 1, 43, 'C' )",
				"(34, 2, 43, 'B+')",
				"(44, 4, 33, 'B' )",
				"(54, 4, 53, 'A' )",
		"(64, 6, 53, 'A' )"};
		for (int i=0; i<enrollvals1.length; i++)
			planner1.executeUpdate(s1 + enrollvals1[i], tx);
		System.out.println("ENROLL records inserted.");
		
		tx.commit();

		// Transaction with new tables begins here

		Transaction tx2  = db.newTx();
		Planner planner2 = db.planner();


		String s2 = "create table STUDENT2(SId int, SName varchar(10), MajorId int, GradYear int)";
		planner2.executeUpdate(s2, tx2);
		System.out.println("Table STUDENT2 created.");

		s2 = "create index testindex on STUDENT2 (MajorId) using btree";
		planner2.executeUpdate(s2, tx2);

		s2 = "insert into STUDENT2(SId, SName, MajorId, GradYear) values ";
		String[] studvals = {"(1, 'liam', 20, 2022)",
				"(2, 'olivia', 30, 2023)",
				"(3, 'noah', 10, 2012)",
				"(4, 'emma', 10, 2020)",
				"(5, 'oliver', 30, 2022)",
				"(6, 'ava', 10, 2020)",
				"(7, 'elijah', 20, 2018)",
				"(8, 'charlotte', 20, 2019)",
				"(9, 'william', 30, 2012)",
				"(10, 'sophia', 30, 2020)",
				"(11, 'james', 30, 2018)",
				"(12, 'amelia', 30, 2019)",
				"(13, 'benjamin', 30, 2017)",
				"(14, 'isabella', 30, 2018)",
				"(15, 'lucas', 30, 2016)",
				"(16, 'mia', 30, 2020)",
				"(17, 'henry', 30, 2021)",
				"(18, 'evelyn', 30, 2020)",
				"(19, 'alexander', 30, 2017)",
				"(20, 'harper', 30, 2012)",
				"(21, 'mason', 30, 2022)",
				"(22, 'camila', 30, 2017)",
				"(23, 'michael', 30, 2018)",
				"(24, 'gianna', 30, 2018)",
				"(25, 'ethan', 30, 2022)",
				"(26, 'abigail', 30, 2018)",
				"(27, 'daniel', 30, 2016)",
				"(28, 'luna', 30, 2017)",
				"(29, 'jacob', 30, 2021)",
				"(30, 'ella', 30, 2020)",
				"(31, 'logan', 30, 2016)",
				"(32, 'elizabeth', 30, 2019)",
				"(33, 'jackson', 30, 2019)",
				"(34, 'sofia', 30, 2016)",
				"(35, 'levi', 30, 2022)",
				"(36, 'emily', 30, 2023)",
				"(37, 'sebastian', 30, 2020)",
				"(38, 'avery', 30, 2015)",
				"(39, 'louis', 30, 2015)",
				"(40, 'stanley', 30, 2022)",
				"(41, 'hannah', 30, 2020)",
				"(42, 'nathan', 30, 2018)",
				"(43, 'rose', 30, 2017)",
				"(44, 'boris', 30, 2019)"};
		for (int i=0; i<studvals.length; i++)
			planner2.executeUpdate(s2 + studvals[i], tx2);
		System.out.println("STUDENT2 records inserted.");

		s2 = "create table DEPT2(DId int, DName varchar(8))";
		planner2.executeUpdate(s2, tx2);
		System.out.println("Table DEPT2 created.");

		s2 = "insert into DEPT2(DId, DName) values ";
		String[] deptvals2 = {"(10, 'FASS')",
				"(20, 'FOS')",
				"(30, 'COM')"};
		for (int i=0; i<deptvals2.length; i++)
			planner2.executeUpdate(s2 + deptvals2[i], tx2);
		System.out.println("DEPT2 records inserted.");

		s2 = "create table COURSE2(CId int, Title varchar(20), DeptId int)";
		planner2.executeUpdate(s2, tx2);
		System.out.println("Table COURSE2 created.");

		s2 = "insert into COURSE2(CId, Title, DeptId) values ";
		String[] coursevals2 = {"(12, 'db systems', 30)",
				"(22, 'compilers', 30)",
				"(32, 'calculus', 20)",
				"(42, 'algebra', 20)",
				"(52, 'acting', 10)",
				"(62, 'elocution', 10)"};
		for (int i=0; i<coursevals2.length; i++)
			planner2.executeUpdate(s2 + coursevals2[i], tx2);
		System.out.println("COURSE2 records inserted.");

		s2 = "create table SECTION2(SectId int, CourseId int, Prof varchar(8), YearOffered int)";
		planner2.executeUpdate(s2, tx2);
		System.out.println("Table SECTION2 created.");

		s2 = "insert into SECTION2(SectId, CourseId, Prof, YearOffered) values ";
		String[] sectvals = {"(13, 12, 'knuth', 2022)",
				"(23, 12, 'knuth', 2013)",
				"(33, 32, 'leibniz', 2018)",
				"(43, 32, 'newton', 2016)",
				"(53, 62, 'shelley', 2022)"};
		for (int i=0; i<sectvals.length; i++)
			planner2.executeUpdate(s2 + sectvals[i], tx2);
		System.out.println("SECTION2 records inserted.");

		s2 = "create table ENROLL2(EId int, StudentId int, SectionId int, Grade varchar(2))";
		planner2.executeUpdate(s2, tx2);
		System.out.println("Table ENROLL2 created.");

		s2 = "create index testindex on ENROLL2(studentID) using hash";
		planner2.executeUpdate(s2, tx2);

		s2 = "insert into ENROLL2(EId, StudentId, SectionId, Grade) values ";
		String[] enrollvals = {"(14, 1, 13, 'A+')",
				"(24, 1, 43, 'D' )",
				"(34, 2, 43, 'D+')",
				"(44, 4, 33, 'C' )",
				"(54, 4, 53, 'B' )",
				"(64, 6, 53, 'A+' )",
				"(74, 8, 13, 'B-' )",
				"(84, 11, 23, 'C-' )",
				"(94, 4, 23, 'B' )",
				"(104, 14, 13, 'A-' )",
				"(114, 22, 43, 'D' )",
				"(124, 43, 23, 'A' )",
				"(134, 16, 13, 'C' )",
				"(144, 26, 43, 'A-' )",
				"(154, 32, 53, 'B+' )",
				"(164, 35, 53, 'C-' )",
				"(174, 38, 53, 'A+' )",
				"(184, 19, 53, 'F' )",
				"(194, 18, 13, 'A' )",
				"(204, 17, 33, 'B' )",
				"(214, 43, 43, 'C-' )",
				"(224, 31, 23, 'C+' )",
				"(234, 26, 53, 'C-' )",
				"(244, 23, 13, 'A-' )",
				"(254, 28, 33, 'B-' )",
				"(264, 29, 23, 'A' )",
				"(274, 33, 13, 'B' )",
				"(284, 11, 43, 'C+' )",
				"(294, 17, 23, 'B-' )",
				"(304, 41, 13, 'A' )",
				"(314, 14, 23, 'A+' )",
				"(324, 19, 23, 'C' )",
				"(334, 12, 53, 'D+' )",
				"(344, 26, 43, 'C' )",
				"(354, 36, 13, 'B+' )",
				"(364, 6, 23, 'C-' )",
				"(374, 13, 33, 'A' )",
				"(384, 22, 13, 'B' )",
				"(394, 40, 53, 'B' )",
				"(404, 10, 43, 'C+' )",
				"(414, 29, 43, 'C-' )",
				"(424, 33, 23, 'A-' )",
				"(434, 6, 33, 'A' )",
				"(444, 14, 33, 'B' )"};
		for (int i=0; i<enrollvals.length; i++)
			planner2.executeUpdate(s2 + enrollvals[i], tx2);
		System.out.println("ENROLL2 records inserted.");

		tx2.commit();

	}
}
