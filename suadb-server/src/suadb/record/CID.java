package suadb.record;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import suadb.exceptions.CannotComparableException;

/**
 * Created by ILHYUN on 2016-11-20.
 */
public class CID implements Serializable{
    private List<Integer> dimensionValues;

    /**
     * Creates a CID for the suadb.record having the
     * specified dimension values in the array
     * @param dimensionValues the dimension values
     */
    public CID(List<Integer> dimensionValues)
    {
        this.dimensionValues = dimensionValues;
    }
    
    public List<Integer> toList() {
        return dimensionValues;
    }

    public boolean equals(Object obj) {
        return dimensionValues.equals(obj);
    }
	
	//SciDB style
    public String toString() {
	    String retString = "{";
	    retString += String.join(",", dimensionValues.stream().map(dim -> Integer.toString(dim)).collect(Collectors.toList()));
	    retString += "}";
	
	    return retString;
    }
    
    public int compareTo(CID cid) throws CannotComparableException
    {
	    int size = dimensionValues.size();
	    List<Integer> target = cid.toList();
	    
	    if(size != target.size())
	    	throw new CannotComparableException();
	    
	    for(int i = 0; i < size; i++)
	    {
			int value = dimensionValues.get(i);
		    int tValue = target.get(i);
		    
		    if(value < tValue)
		    {
			    return -1;
		    }else if(value > tValue)
		    {
			    return 1;
		    }
	    }
	    
	    return 0;
    }
}

