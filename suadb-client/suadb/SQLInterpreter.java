import java.sql.*;

import suadb.remote.NullInfo;
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
		    System.out.println("Invalid AFL");
//			e.printStackTrace();
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (Exception e) {
//				e.printStackTrace();
				System.out.println("Exception occurred");
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
				// print header
				if (numOfDimensions > 0) {
					System.out.print("{");
					for (int i = 1; i <= numOfDimensions; i++) {
						System.out.print(md.getDimensionName(i));
						if (i != numOfDimensions)
							System.out.print(",");
						else
							System.out.print("} ");
					}
				}
				for (int i = 1; i <= numOfAttributes; i++) {
					System.out.print(md.getAttributeName(i));
					if (i != numOfAttributes)
						System.out.print(",");
				}

				System.out.print("\n");

				// print cells
				while (rs.next()) {
					NullInfo nullInfo = rs.whichIsNull();
					if(nullInfo.getNullValues() == numOfAttributes)//If all attributes are null
						continue;

					List<Integer> currentDimension = rs.getCurrentDimension();
					if (currentDimension.size() > 0 ) {
						System.out.print("{");
						for (int i = 0; i < numOfDimensions; i++) {
							System.out.print(currentDimension.get(i));
							if (i != numOfDimensions - 1)
								System.out.print(",");
							else
								System.out.print("} ");
						}
					}
					for (int i = 1; i <= numOfAttributes; i++) {
						if(!nullInfo.isNull(i-1)) {//Not null
							int type = md.getAttributeType(i);
							if (type == Types.INTEGER)
								System.out.print(rs.getInt(md.getAttributeName(i)));
							else if (type == Types.VARCHAR)
								System.out.print(rs.getString(md.getAttributeName(i)));
						}
						else // null
							System.out.print("null");

						if (i != numOfAttributes)
							System.out.print(",");

					}

					System.out.print("\n");
				}
			}
			else{//list
				int index = 0;
				if(cmd.contains("fields")) {//targetName.equals("fields")
					System.out.println("{No} array, attribute, type");
					String array = md.getAttributeName(1);
					String attribute = md.getAttributeName(2);
					String type = md.getAttributeName(3);
					String length = md.getAttributeName(4);
					while(rs.next()){
						System.out.print("{" + index + "} ");
						index++;
						System.out.print("'" + rs.getString(array) + "',"
								+ "'" + rs.getString(attribute) + "',");
						if(rs.getInt(type) == Types.INTEGER)
							System.out.print("int");
						else if(rs.getInt(type) == Types.VARCHAR) {
							System.out.print("string("+rs.getInt(length)+")");
						}
						System.out.println();
					}
				}
				else{//targetName == null || targetName.length() == 0 || targetName.equals("arrays")
					System.out.println("{No} name,schema");
					String name = md.getAttributeName(1);
					String definition = md.getAttributeName(2);
					while (rs.next()) {
						System.out.print("{" + index + "} ");
						index++;
						//2 attributes : arrayName,definition
						System.out.println("'" + rs.getString(name) + "',"
								+ "'" + rs.getString(definition) + "'");
					}
				}
			}
			rs.close();

		} catch (SQLException e) {
			System.out.println("SQL Exception: " + e.getMessage());
//			e.printStackTrace();
		}
	}

	private static void doUpdate(String cmd) {
		try {
			Statement stmt = conn.createStatement();
			stmt.executeUpdate(cmd);
			System.out.println("Query was executed successfully");
		} catch (SQLException e) {
			System.out.println("SQL Exception: " + e.getMessage());
//			e.printStackTrace();
		}
	}
}