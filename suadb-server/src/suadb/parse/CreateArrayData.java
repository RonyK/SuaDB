package suadb.parse;

import suadb.record.Schema;

public class CreateArrayData
{
	private String arrayName;
	private Schema sch;

	/**
	 * Saves the table name and schema.
	 */
	public CreateArrayData(String arrayName, Schema sch) {
		this.arrayName = arrayName;
		this.sch = sch;
	}

	/**
	 * Returns the name of the new array.
	 * @return the name of the new array
	 */
	public String arrayName() {
		return arrayName;
	}

	/**
	 * Returns the schema of the new array.
	 * @return the schema of the new array
	 */
	public Schema newSchema() {
		return sch;
	}
}

