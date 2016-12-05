package suadb.record;

import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

/**
 * The record schema of a table.
 * A schema contains the name and type of
 * each field of the table, as well as the length
 * of each varchar field.
 * @author Edward Sciore
 *
 */
public class Schema {
	//key : attribute name, value : attribute type & length(if String type) - CDS
	private Map<String, attributeInfo> attributeInfo = new HashMap<String, attributeInfo>();
	//key : dimensionName, value : DimensionInfo(start,end,chunkSize) - CDS
	private Map<String, DimensionInfo> dimensionInfo = new LinkedHashMap<String, DimensionInfo>();

	/**
	 * Creates an empty schema.
	 * Field information can be added to a schema
	 * via the five addXXX methods. 
	 */
	public Schema() {}

	/**
	 * Adds a field to the schema having a specified
	 * name, type, and length.
	 * If the field type is "integer", then the length
	 * value is irrelevant.
	 * @param attributeName the name of the field
	 * @param type the type of the field, according to the constants in simpledb.sql.types
	 * @param length the conceptual length of a string attribute.
	 */
	public void addField(String attributeName, int type, int length) {
		attributeInfo.put(attributeName, new attributeInfo(type, length));
	}
	
	/**
	 * Adds a attribute to the schema having a specified
	 * name, type, and length.
	 * If the attribute type is "integer", then the length
	 * value is irrelevant.
	 * @param attributeName the name of the attribute
	 * @param type the type of the attribute, according to the constants in simpledb.sql.types
	 * @param length the conceptual length of a string attribute.
	 */
	public void addAttribute(String attributeName, int type, int length) {
		attributeInfo.put(attributeName, new attributeInfo(type, length));
	}
	
	/**
	 * Adds a dimension in the schema.
	 *
	 * @param dimensionName   the name of dimension
	 * @param start     the number of dimension start
	 * @param end       the number of dimension end
	 * @param chunkSize the size of chunk
	 */
	public void addDimension(String dimensionName, int start, int end, int chunkSize) {
		dimensionInfo.put(dimensionName, new DimensionInfo(start,end,chunkSize));
	}

	/**
	 * Adds an integer field to the schema.
	 * @param fldname the name of the field
	 */
	public void addIntField(String fldname) {
		addAttribute(fldname, INTEGER, 0);
	}

	/**
	 * Adds a string field to the schema.
	 * The length is the conceptual length of the field.
	 * For example, if the field is defined as varchar(8),
	 * then its length is 8.
	 * @param fldname the name of the field
	 * @param length the number of chars in the varchar definition
	 */
	public void addStringField(String fldname, int length) {
		addAttribute(fldname, VARCHAR, length);
	}
	
	/**
	 * Adds a field to the schema having the same
	 * type and length as the corresponding field
	 * in another schema.
	 * @param fldname the name of the field
	 * @param sch the other schema
	 */
	public void add(String fldname, Schema sch) {
		int type	= sch.type(fldname);
		int length = sch.length(fldname);
		addAttribute(fldname, type, length);
	}
	
	/**
	 * Adds all of the fields in the specified schema
	 * to the current schema.
	 * @param sch the other schema
	 */
	public void addAll(Schema sch) {
		attributeInfo.putAll(sch.attributeInfo);
		dimensionInfo.putAll(sch.dimensionInfo);
	}

	/**
	 * Returns a collection containing the name of
	 * each field in the schema.
	 * @return the collection of the schema's attribute names
	 */
	
	public Collection<String> fields() {
		return attributeInfo.keySet();
	}
	/**
	 * Returns a collection containing the name of
	 * each attribute in the schema.
	 * same as fields(), just for convenience - CDS
	 * @return the collection of the schema's attribute names
	 */
	
	public Collection<String> attributes() {
		return attributeInfo.keySet();
	}
	/**
	 * Returns a collection containing the name of
	 * each dimension in the schema. - CDS
	 * @return the collection of the schema's dimension names
	 */
	
	public Collection<String> dimensions() {
		return dimensionInfo.keySet();
	}
	
	/**
	 * Returns true if the specified field
	 * is in the schema
	 * @param fldname the name of the field
	 * @return true if the field is in the schema
	 */
	public boolean hasField(String fldname) {
		return fields().contains(fldname);
	}

	/**
	 * Return true if the specified dimension
	 * is in the schema
	 *
	 * @param dimName	the name of the dimension
	 * @return			 true if the dimension in the schema
	 */
	 public boolean hasDimension(String dimName)
	 {
	 return dimensions().contains(dimName);
	 }

	 /**
	 * Returns the type of the specified field, using the
	 * constants in {@link java.sql.Types}.
	 * @param fldname the name of the field
	 * @return the integer type of the field
	 */
	public int type(String fldname) {
		return attributeInfo.get(fldname).type;
	}
	
	/**
	 * Returns the conceptual length of the specified field.
	 * If the field is not a string field, then
	 * the return value is undefined.
	 * @param fldname the name of the field
	 * @return the conceptual length of the field
	 */
	public int length(String fldname) {
		return attributeInfo.get(fldname).length;
	}

	/**
	 * Returns the start point of the specified dimension. - CDS
	 * @param dimensionName the name of the field
	 * @return the start point
	 */
	public int start(String dimensionName) {
		return dimensionInfo.get(dimensionName).start;
	}

	/**
	 * Returns the end point of the specified dimension. - CDS
	 * @param dimensionName the name of the field
	 * @return the end point
	 */
	public int end(String dimensionName) {
		return dimensionInfo.get(dimensionName).end;
	}

	/**
	 * Returns the chunkSize of the specified dimension. - CDS
	 * @param dimensionName the name of the field
	 * @return the chunkSize
	 */
	public int chunkSize(String dimensionName) {
		return dimensionInfo.get(dimensionName).chunkSize;
	}

	/**
	 * The number of chunks == (int) Math.ceil((double)(end-start+1)/chunkSize);
	 * @param dimensionName
	 * @return
	 */
	public int getNumOfChunk(String dimensionName){
		return dimensionInfo.get(dimensionName).numOfChunk;
	}

	class attributeInfo {
		int type, length;
		public attributeInfo(int type, int length) {
			this.type = type;
			this.length = length;
		}
		
		@Override
		public String toString()
		{
			String str;
			
			switch (this.type)
			{
				case Types.INTEGER:
				{
					str = "INTEGER";
					break;
				}
				case Types.DOUBLE:
				{
					str =  "DOUBLE";
					break;
				}
				case Types.VARCHAR:
				{
					str = "VARCHAR";
					break;
				}
				default:
				{
					str = "UNKNOWN";
				}
			}
			
			return str + "(" + Integer.toString(this.length) + ")";
		}
	}

	class DimensionInfo
	{
		int start, end, chunkSize;
		//chunkSize : The number of cells in one chunk along one dimension - CDS
//		int overlap;
		int numOfChunk; // The number of chunks in the dimension

		public DimensionInfo(int start, int end, int chunkSize)
		{
			this.start = start;
			this.end = end;
			this.chunkSize = chunkSize;
			this.numOfChunk = (int) Math.ceil((double)(end-start+1)/chunkSize);
		}
		
		@Override
		public String toString()
		{
			return Integer.toString(start) + ":" +
					Integer.toString(end) + "," +
					Integer.toString(chunkSize);
		}
	}
}
