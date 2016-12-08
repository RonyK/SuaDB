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
				System.out.print("AFL% ");
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
				else {
					System.out.println("Invalid AFL");
				}
			}
		} catch (Exception e) {
//		    System.out.println("Invalid AFL");
			e.printStackTrace();
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static void doQuery(String cmd) {
		try {
			Statement stmt = conn.createStatement();
			SimpleResultSet rs = (SimpleResultSet) stmt.executeQuery(cmd);
			SimpleMetaData md = (SimpleMetaData) rs.getMetaData();
			int numOfDimensions = md.getDimensionCount();
			int numOfAttributes = md.getAttributeCount();

			//scan,project,filter
			if (!cmd.startsWith("list")){
				String result = "";
				// print header
				if (numOfDimensions > 0) {
					result += "{";
					for (int i = 1; i <= numOfDimensions; i++) {
						result += md.getDimensionName(i);
						if (i != numOfDimensions)
							result += ",";
						else
							result += "} ";
					}
				}
				for (int i = 1; i <= numOfAttributes; i++) {
					result += md.getAttributeName(i);
					if (i != numOfAttributes)
						result += ",";
				}

				result += "\n";

				// print cells
				while (rs.next()) {
					List<Integer> currentDimension = rs.getCurrentDimension();
					if (currentDimension.size() > 0) {
						result += "{";
						for (int i = 0; i < numOfDimensions; i++) {
							result += currentDimension.get(i);
							if (i != numOfDimensions - 1)
								result += ",";
							else
								result += "} ";
						}
					}
					for (int i = 1; i <= numOfAttributes; i++) {
						int type = md.getAttributeType(i);
						if (type == Types.INTEGER)
							result += rs.getInt(md.getAttributeName(i));
						else if (type == Types.VARCHAR)
							result += rs.getString(md.getAttributeName(i));

						if (i != numOfAttributes)
							result += ",";
					}

					result += "\n";
				}
				System.out.print(result);
			}
			else{//list
				int index = 0;
				String result = "";
				if(cmd.contains("fields")) {//targetName.equals("fields")
					result = "{No} array, attribute, type\n";
					String array = md.getAttributeName(1);
					String attribute = md.getAttributeName(2);
					String type = md.getAttributeName(3);
					String length = md.getAttributeName(4);
					while(rs.next()){
						result += "{" + index + "} ";
						index++;
						result += "'" + rs.getString(array) + "',"
								+ "'" + rs.getString(attribute) + "',";
						if(rs.getInt(type) == Types.INTEGER)
							result += "int";
						else if(rs.getInt(type) == Types.VARCHAR) {
							result += "string("+rs.getInt(length)+")";
						}
						result += "\n";
					}
				}
				else{//targetName == null || targetName.length() == 0 || targetName.equals("arrays")
					result = "{No} name,schema\n";
					String name = md.getAttributeName(1);
					String definition = md.getAttributeName(2);
					while (rs.next()) {
						result += "{" + index + "} ";
						index++;
						//2 attributes : arrayName,definition
						result += "'" + rs.getString(name) + "',"
								+ "'" + rs.getString(definition) + "'\n";
					}
				}
				System.out.print(result);
			}
			rs.close();

		} catch (SQLException e) {
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
		} catch (SQLException e) {
			System.out.println("SQL Exception: " + e.getMessage());
			e.printStackTrace();
		}
	}
}