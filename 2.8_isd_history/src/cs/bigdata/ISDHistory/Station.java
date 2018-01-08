package cs.bigdata.ISDHistory;

public abstract class Station {
	protected static String usaf;
	protected static String name;
	protected static String country;
	protected static String elevation;

	
	public static String getUsaf() {
		return usaf;
	}
	
	public static void setUsaf(String usaf) {
		if (usaf==null || usaf.isEmpty()) {
			Station.usaf = "No USAF given";
		}
		else {
			Station.usaf = usaf;
		}
	}
	
	public static String getName() {
		return name;
	}
	
	public static void setName(String name) {
		
		if (name==null || name.isEmpty()) {
			Station.name = "No name station given";
		}
		else {
			Station.name = name;
		}
	}
	
	public static String getCountry() {
		return country;
	}
	
	public static void setCountry(String country) {
		if (country==null || country.isEmpty()) {
			Station.country = "No country given";
		}
		else {
			Station.country = country;
		}
	}
	
	public static String getElevation() {
		return elevation;
	}
	
	public static void setElevation(String elevation) {
		if (elevation==null || elevation.isEmpty()) {
			Station.elevation = "No elevation given";
		}
		else {
			Station.elevation = elevation;
		}
	}

	public static void readLine(String line) {
		Station.setUsaf(line.substring(0, 6));
		Station.setName(line.substring(13, 42));
		Station.setCountry(line.substring(43, 45));
		Station.setElevation(line.substring(74, 81));
		
		System.out.println("USAF Code: "+ Station.usaf + "; Name of station: " + Station.name + "; The FIPS: " + Station.country + "; The elevation: " + Station.elevation);

	}
}
