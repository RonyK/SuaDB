package suadb.record;

import java.util.HashMap;
import java.util.Map;

import static java.sql.Types.INTEGER;
import static suadb.file.Page.*;

/**
 * Created by Rony on 2016-11-09.
 */
public class ArrayInfo
{
	private Schema schema;
	private Map<String, Integer> offsets;
	// TODO :: What record length means? - RonyK
	private int recordLen;
	private String arrayName;

	public ArrayInfo(String arrayName, Schema schema)
	{
		this.schema = schema;
		this.arrayName = arrayName;
		offsets = new HashMap<String, Integer>();
		int pos = 0;
		for (String fldName : schema.fields())
		{
			offsets.put(fldName, pos);
			pos += lengthInBytes(fldName);
		}
	}

	public ArrayInfo(String arrayName, Schema schema, Map<String, Integer> offsets, int recordLen)
	{
		this.arrayName  = arrayName;
		this.schema     = schema;
		this.offsets    = offsets;
		this.recordLen  = recordLen;
	}

	public String fileName()
	{
		return arrayName + ".arr";
	}

	public Schema schema()
	{
		return schema;
	}

	public int recordLen()
	{
		return recordLen;
	}

	private int lengthInBytes(String fldname) {
		int fldtype = schema.type(fldname);
		if (fldtype == INTEGER)
			return INT_SIZE;
		else
			return STR_SIZE(schema.length(fldname));
	}
}
