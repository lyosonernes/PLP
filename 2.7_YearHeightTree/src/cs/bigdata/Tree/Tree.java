package cs.bigdata.Tree;

public abstract class Tree {
	protected static String height;
	protected static String year;

	public static String getHeight() {
		return Tree.height;
	}
	public static void setHeight(String hght) {
		if (hght==null || hght.isEmpty()) {
			Tree.height = "No height given";
		}
		else {
			Tree.height = hght;
		}
	}
	public static String getYear() {
		return Tree.year;
	}
	public static void setYear(String yr) {
		if (yr ==null || yr.isEmpty()) {
			Tree.year = "No year given";
		}
		else {
			Tree.year = yr;			
		}
	}

	public static void readLine(String line, String splitter) {
		String [] tree = line.split(splitter);
		Tree.setYear(tree[5]);
		Tree.setHeight(tree[6]);
		System.out.println("Year: " + Tree.year + ", Height: " + Tree.height);

	}

}
