package hiHive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class myFirstHive {
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	public static void main(String[] args) throws Exception{
		Class.forName(driverName);
		Connection con = DriverManager.getConnection("jdbc:hive://10.184.134.86:10000/default", "", "");
	    Statement stmt = con.createStatement();
	    String sql = "SELECT TAB.DTYPE, COUNT(TAB.DTYPE) FROM("+
	    		"SELECT DEPT_2.DEPTYPE DTYPE FROM USER_2 "+
	    		" JOIN DEPT_2 ON (USER_2.dept=DEPT_2.depnum) )TAB "+
	    		" GROUP BY TAB.DTYPE";
	    System.out.println("Running: " + sql);
	    ResultSet res = stmt.executeQuery(sql);
	    while(res.next())
	    	System.out.println(">>>"+res.getString(1) + ":" +res.getInt(2));
	    res.close();
	    con.close();
	}

}
