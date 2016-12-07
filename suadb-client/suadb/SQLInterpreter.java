import java.sql.*;
import suadb.remote.SimpleDriver;
import suadb.remote.SimpleMetaData;
import suadb.remote.SimpleResultSet;

import java.io.*;
import java.util.List;

public class SQLInterpreter {
    private static Connection conn = null;

    public static void main(String[] args) {
	   try {
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:suadb://localhost", null);

			Reader rdr = new InputStreamReader(System.in);
			BufferedReader br = new BufferedReader(rdr);

			while (true) {
				// process one line of input
				System.out.print("\nAFL% ");
				String cmd = br.readLine().trim();
				if (cmd.startsWith("exit"))
					break;
				else if (cmd.startsWith("scan")
						|| cmd.startsWith("project")
						|| cmd.startsWith("filter")
						|| cmd.startsWith("list"))
					doQuery(cmd);
				else if (cmd.startsWith("create")
						|| cmd.startsWith("input")
						|| cmd.startsWith("remove"))
					doUpdate(cmd);
				else{
					System.out.println("Invalid AFL");
				}
		    }
	    }
	    catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (conn != null)
					conn.close();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static void doQuery(String cmd) {
		try {
		    Statement stmt = conn.createStatement();
		    SimpleResultSet rs = (SimpleResultSet)stmt.executeQuery(cmd);
			SimpleMetaData md = (SimpleMetaData)rs.getMetaData();
			int numOfDimensions = md.getDimensionCount();
			int numOfAttributes = md.getAttributeCount();


		    int totalwidth = 0;

		    // print header
			System.out.print("{");
			for(int i=1; i<=numOfDimensions; i++){
				System.out.print(md.getDimensionName(i));
				if(i != numOfDimensions)
					System.out.print(",");
				else
					System.out.print("} ");
			}
			for(int i=1; i<= numOfAttributes; i++){
				System.out.print(md.getAttributeName(i));
				if(i != numOfAttributes)
					System.out.print(",");
			}

			System.out.println();

		    // print cells
		    while(rs.next()) {
			    List<Integer> currentDimension = rs.getCurrentDimension();
			    System.out.print("{");
			    for(int i=0;i<numOfDimensions;i++){
				    System.out.print(currentDimension.get(i));
				    if(i != numOfDimensions-1)
					    System.out.print(",");
				    else
					    System.out.print("} ");
			    }
			    for(int i=1;i<=numOfAttributes;i++){
				    int type = md.getAttributeType(i);
				    if(type == Types.INTEGER)
					    System.out.print(rs.getInt(md.getAttributeName(i)));
				    else if(type == Types.VARCHAR)
					    System.out.print(rs.getString(md.getAttributeName(i)));

				    if(i!= numOfAttributes)
					    System.out.print(",");
			    }
//
//				for (int i=1; i<=numOfAttributes; i++) {
//					String fldname = md.getColumnName(i);
//					int fldtype = md.getColumnType(i);
//					String fmt = "%" + md.getColumnDisplaySize(i);
//					if (fldtype == Types.INTEGER)
//						System.out.format(fmt + "d", rs.getInt(fldname));
//					else
//						System.out.format(fmt + "s", rs.getString(fldname));
//				}
				System.out.println();
			}
			rs.close();
		}
		catch (SQLException e) {
			System.out.println("SQL Exception: " + e.getMessage());
			e.printStackTrace();
		}
	}

	private static void doUpdate(String cmd) {
		try {
		    Statement stmt = conn.createStatement();
		    int howmany = stmt.executeUpdate(cmd);
//		    System.out.println(howmany + " records processed");
			System.out.println("Query was executed successfully");
		}
		catch (SQLException e) {
			System.out.println("SQL Exception: " + e.getMessage());
			e.printStackTrace();
		}
	}
}