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
		Planner planner = db.planner();


		String s = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
		planner.executeUpdate(s, tx);
		System.out.println("Table STUDENT created.");

		s = "create index testindex on STUDENT (MajorId) using btree";
		planner.executeUpdate(s, tx);
		System.out.println("Index (btree) testindex on STUDENT(MajorId) created.");

		s = "create index idx_gradyear on STUDENT (gradyear) using hash";
		planner.executeUpdate(s, tx);
		System.out.println("Index (hash) idx_gradyear on STUDENT(gradyear) created.");

		s = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
		String[] studvals = {"(1, 'liam', 20, 2022)",
				"(2, 'olivia', 30, 2014)",
				"(3, 'noah', 10, 2012)",
				"(4, 'emma', 10, 2020)",
				"(5, 'oliver', 30, 2022)",
				"(6, 'ava', 10, 2020)",
				"(7, 'harper', 20, 2018)",
				"(8, 'charlotte', 20, 2019)",
				"(9, 'william', 30, 2012)",
				"(10, 'sophia', 10, 2020)",
				"(11, 'james', 30, 2018)",
				"(12, 'amelia', 10, 2019)",
				"(13, 'benjamin', 10, 2017)",
				"(14, 'isabella', 30, 2018)",
				"(15, 'lucas', 20, 2016)",
				"(16, 'mia', 30, 2020)",
				"(17, 'henry', 10, 2021)",
				"(18, 'evelyn', 30, 2020)",
				"(19, 'riley', 30, 2017)",
				"(20, 'harper', 30, 2012)",
				"(21, 'mason', 10, 2022)",
				"(22, 'camila', 30, 2017)",
				"(23, 'michael', 20, 2018)",
				"(24, 'gianna', 10, 2018)",
				"(25, 'ethan', 30, 2022)",
				"(26, 'abigail', 10, 2018)",
				"(27, 'daniel', 20, 2016)",
				"(28, 'luna', 20, 2017)",
				"(29, 'jacob', 30, 2021)",
				"(30, 'ella', 20, 2020)",
				"(31, 'penelope', 30, 2016)",
				"(32, 'elizabeth', 20, 2019)",
				"(33, 'jackson', 10, 2019)",
				"(34, 'sofia', 30, 2016)",
				"(35, 'levi', 30, 2022)",
				"(36, 'emily', 30, 2012)",
				"(37, 'sebastian', 10, 2020)",
				"(38, 'avery', 20, 2015)",
				"(39, 'samuel', 30, 2015)",
				"(40, 'stanley', 10, 2022)",
				"(41, 'hannah', 10, 2020)",
				"(42, 'nathan', 30, 2018)",
				"(43, 'rose', 20, 2017)",
				"(44, 'boris', 30, 2019)",					
				"(45, 'mateo', 20, 2022)",
				"(46, 'mila', 30, 2023)",
				"(47, 'jack', 10, 2012)",
				"(48, 'scarlett', 10, 2020)",
				"(49, 'owen', 30, 2022)",
				"(50, 'eleanor', 10, 2020)",
				"(51, 'theodore', 20, 2018)",
				"(52, 'madison', 20, 2019)",
				"(53, 'aiden', 30, 2012)",
				"(54, 'layla', 30, 2020)",
				"(55, 'samuel', 10, 2018)",
				"(56, 'penelope', 30, 2019)",
				"(57, 'joseph', 20, 2017)",
				"(58, 'aria', 20, 2018)",
				"(59, 'john', 30, 2016)",
				"(60, 'chloe', 30, 2020)",
				"(61, 'david', 10, 2021)",
				"(62, 'grace', 30, 2020)",
				"(63, 'elizabeth', 20, 2017)",
				"(64, 'ellie', 30, 2012)",
				"(65, 'matthew', 10, 2022)",				
				"(66, 'nora', 30, 2017)",
				"(67, 'luke', 30, 2018)",
				"(68, 'hazel', 20, 2018)",
				"(69, 'asher', 30, 2022)",
				"(70, 'zoey', 10, 2018)",
				"(71, 'carter', 20, 2016)",
				"(72, 'riley', 10, 2017)",
				"(73, 'julian', 30, 2021)",
				"(74, 'victoria', 20, 2020)",
				"(75, 'samuel', 30, 2016)",
				"(76, 'lily', 30, 2019)",				
				"(77, 'leo', 20, 2019)",
				"(78, 'aurora', 20, 2016)",
				"(79, 'jayden', 30, 2022)",
				"(80, 'violet', 30, 2013)",		
				"(81, 'gabriel', 10, 2020)"};				
		for (int i=0; i<studvals.length; i++)
			planner.executeUpdate(s + studvals[i], tx);
		System.out.println("STUDENT records inserted.");

		s = "create table DEPT(DId int, DName varchar(8))";
		planner.executeUpdate(s, tx);
		System.out.println("Table DEPT created.");

		s = "insert into DEPT(DId, DName) values ";
		String[] deptvals = {"(10, 'FASS')",
				"(20, 'FOS')",
				"(30, 'COM')"};
		for (int i=0; i<deptvals.length; i++)
			planner.executeUpdate(s + deptvals[i], tx);
		System.out.println("DEPT records inserted.");

		s = "create table COURSE(CId int, Title varchar(20), DeptId int)";
		planner.executeUpdate(s, tx);
		System.out.println("Table COURSE created.");

		s = "insert into COURSE(CId, Title, DeptId) values ";
		String[] coursevals2 = {"(12, 'intro to algorithms', 30)",
				"(22, 'compiler design', 30)",
				"(32, 'calculus for engineering', 20)",
				"(42, 'galois theory', 20)",
				"(52, 'method acting', 10)",
				"(62, 'elocution', 10)"};
		for (int i=0; i<coursevals2.length; i++)
			planner.executeUpdate(s + coursevals2[i], tx);
		System.out.println("COURSE records inserted.");

		s = "create table SECTION(SectId int, CourseId int, Prof varchar(8), YearOffered int)";
		planner.executeUpdate(s, tx);
		System.out.println("Table SECTION created.");

		s = "insert into SECTION(SectId, CourseId, Prof, YearOffered) values ";
		String[] sectvals = {"(13, 12, 'knuth', 2021)",
				"(23, 22, 'knuth', 2013)",
				"(33, 32, 'leibniz', 2018)",
				"(43, 32, 'newton', 2017)",				
				"(53, 12, 'knuth', 2012)",
				"(63, 42, 'galois', 2019)",
				"(73, 52, 'clooney', 2022)",
				"(83, 52, 'clooney', 2013)",
				"(93, 32, 'leibniz', 2021)",
				"(103, 62, 'churchill', 2017)",
				"(113, 32, 'leibniz', 2014)",
				"(123, 42, 'galois', 2016)",
				"(133, 32, 'gauss', 2016)",
				"(143, 32, 'newton', 2017)",
				"(153, 32, 'leibniz', 2015)",
				"(163, 32, 'newton', 2020)",
				"(173, 42, 'galois', 2018)",
				"(183, 32, 'newton', 2013)",
				"(193, 52, 'clooney', 2020)",
				"(203, 32, 'gauss', 2014)",
				"(213, 32, 'neumann', 2018)",
				"(223, 62, 'churchill', 2016)",
				"(233, 32, 'leibniz', 2020)",
				"(243, 32, 'newton', 2017)",
				"(253, 52, 'clooney', 2022)",
				"(263, 12, 'knuth', 2019)",
				"(273, 62, 'churchill', 2014)",
				"(283, 32, 'gauss', 2019)",
				"(293, 32, 'neumann', 2016)",
				"(303, 42, 'galois', 2015)",
				"(313, 32, 'gauss', 2018)",
				"(323, 32, 'newton', 2020)",
				"(333, 52, 'clooney', 2022)",
				"(343, 32, 'newton', 2021)",
				"(353, 22, 'knuth', 2012)",
				"(363, 32, 'leibniz', 2019)",
				"(373, 32, 'gauss', 2013)",
				"(383, 62, 'churchill', 2017)",
				"(393, 32, 'neumann', 2016)",
				"(403, 12, 'knuth', 2013)",
				"(413, 32, 'leibniz', 2018)",
				"(423, 32, 'gauss', 2020)",
				"(433, 42, 'galois', 2019)",
				"(443, 62, 'churchill', 2016)",
				"(453, 52, 'clooney', 2022)",
				"(463, 32, 'leibniz', 2020)",
				"(473, 32, 'gauss', 2015)",
				"(483, 32, 'neumann', 2019)",
				"(493, 12, 'knuth', 2012)",
				"(503, 52, 'clooney', 2017)"};
		for (int i=0; i<sectvals.length; i++)
			planner.executeUpdate(s + sectvals[i], tx);
		System.out.println("SECTION records inserted.");

		s = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))";
		planner.executeUpdate(s, tx);
		System.out.println("Table ENROLL created.");

		// s = "create index testindex on ENROLL(studentID) using hash";
		// planner.executeUpdate(s, tx);

		s = "insert into ENROLL(EId, StudentId, SectionId, Grade) values ";
		String[] enrollvals = {"(14, 1, 453, 'A+')",
				"(24, 81, 193, 'D' )",
				"(34, 2, 203, 'D+')",
				"(44, 42, 413, 'C' )",
				"(54, 45, 253, 'B' )",
				"(64, 68, 173, 'A+' )",
				"(74, 8, 283, 'B-' )",
				"(84, 11, 33, 'C-' )",
				"(94, 33, 483, 'B' )",				
				"(104, 57, 103, 'A-' )",
				"(114, 38, 303, 'D' )",				
				"(124, 43, 243, 'A' )",			
				"(134, 19, 503, 'C' )",			
				"(144, 26, 413, 'A-' )",
				"(154, 42, 313, 'B+' )",
				"(164, 35, 73, 'C-' )",
				"(174, 49, 333, 'A+' )",
				"(184, 19, 383, 'F' )",	
				"(194, 18, 163, 'A' )",
				"(204, 47, 53, 'B' )",
				"(214, 43, 103, 'C-' )",
				"(224, 41, 423, 'C+' )",
				"(234, 26, 173, 'C-' )",
				"(244, 23, 173, 'A-' )",
				"(254, 28, 383, 'B-' )",
				"(264, 59, 223, 'A' )",
				"(274, 33, 263, 'B' )",
				"(284, 67, 413, 'C+' )",
				"(294, 17, 93, 'B-' )",
				"(304, 61, 343, 'A' )",
				"(314, 14, 33, 'A+' )",
				"(324, 19, 503, 'C' )",
				"(334, 12, 363, 'D+' )",
				"(344, 76, 433, 'C' )",
				"(354, 36, 493, 'B+' )",				
				"(364, 67, 313, 'C-' )",
				"(374, 13, 243, 'A' )",
				"(384, 2, 273, 'B' )",
				"(394, 40, 253, 'B' )",
				"(404, 1, 333, 'C+' )",
				"(414, 29, 93, 'C-' )",
				"(424, 33, 63, 'A-' )",
				"(434, 51, 173, 'A' )",
				"(444, 4, 233, 'C' )",
				"(454, 32, 263, 'B' )",
				"(464, 9, 353, 'A+' )",
				"(474, 82, 473, 'B-' )",
				"(484, 71, 293, 'C-' )",
				"(494, 34, 223, 'B' )",
				"(504, 24, 313, 'A-' )",
				"(514, 22, 103, 'D' )",		
				"(524, 33, 483, 'A+' )",
				"(534, 66, 143, 'C' )",
				"(544, 26, 173, 'A-' )",
				"(554, 62, 193, 'B+' )",
				"(564, 35, 253, 'C-' )",
				"(574, 38, 303, 'A+' )",
				"(584, 59, 443, 'F' )",
				"(594, 18, 463, 'A+' )",				
				"(604, 57, 243, 'B' )",
				"(614, 43, 143, 'C-' )",
				"(624, 31, 293, 'C+' )",
				"(634, 68, 33, 'C' )",
				"(644, 23, 313, 'A-' )",
				"(654, 68, 213, 'B-' )",
				"(664, 29, 343, 'A' )",
				"(674, 33, 263, 'B' )",
				"(684, 61, 343, 'B+' )",
				"(694, 17, 13, 'B-' )",
				"(704, 41, 323, 'A' )",				
				"(714, 74, 463, 'D+' )",
				"(724, 19, 103, 'C' )",
				"(734, 72, 383, 'D+' )",
				"(744, 26, 213, 'C' )",
				"(754, 36, 353, 'B+' )",
				"(764, 6, 463, 'C-' )",
				"(774, 53, 353, 'A' )",
				"(784, 22, 143, 'C' )",
				"(794, 80, 83, 'F' )",
				"(804, 10, 163, 'C+' )",
				"(814, 39, 153, 'C-' )",
				"(824, 3, 353, 'A-' )",
				"(834, 70, 313, 'A' )",
				"(844, 53, 493, 'B' )",
				"(854, 54, 323, 'B' )"};
		for (int i=0; i<enrollvals.length; i++)
			planner.executeUpdate(s + enrollvals[i], tx);
		System.out.println("ENROLL records inserted.");

		tx.commit();

	}
}