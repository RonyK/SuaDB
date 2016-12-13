package suadb.remote;

import java.io.Serializable;

/**
 * Created by CDS on 2016-12-09.
 */
public class NullInfo implements Serializable{
	protected boolean[] whichIsNull;
	protected int nullValues;//The number of null values

	NullInfo(int numberOfAttributes){
		this.whichIsNull = new boolean[numberOfAttributes];
		this.nullValues = 0;
	}

	public int getNullValues() {
		return nullValues;
	}

	public void setNullValues(int nullValues) {
		this.nullValues = nullValues;
	}

	public boolean isNull(int index){
		return whichIsNull[index];
	}
}