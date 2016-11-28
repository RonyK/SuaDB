package suadb.record;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.sql.Types.INTEGER;
import static suadb.file.Page.*;

/**
 * Created by Rony on 2016-11-09.
 */
public class ArrayInfo {
    private String arrayName;
	private Schema schema;


	public ArrayInfo(String arrayName, Schema schema) {
		this.schema = schema;
		this.arrayName = arrayName;
        schema.attributes();
	}

	public String arrayName() {
		return arrayName;
	}

	public Schema schema() {
		return schema;
	}

	// ILHYUN : this method is needed to retrieve the length of record for an attribute
	public int recordLength(String fldname){
		return lengthInBytes(fldname);
	}

	private int lengthInBytes(String fldname) {
		int fldtype = schema.type(fldname);
		if (fldtype == INTEGER)
			return INT_SIZE;
		else
			return STR_SIZE(schema.length(fldname));
	}
}
