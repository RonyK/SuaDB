package suadb.record;

import java.util.List;
import java.util.Map;

/**
 * Created by ILHYUN on 2016-11-20.
 */
public class CID {
    private List<Integer> dimensionvalues;
    private ArrayInfo arrayinfo;

    /**
     * Creates a CID for the suadb.record having the
     * specified dimension values in the array
     * @param dimensionvalues the dimension values
     */
    public CID(List<Integer> dimensionvalues, ArrayInfo arrayinfo) {
        this.dimensionvalues = dimensionvalues;
        this.arrayinfo = arrayinfo;
    }


    public List<Integer> dimensionValues() {
        return dimensionvalues;
    }

    public ArrayInfo arrayInfo() {
        return arrayinfo;
    }

    public boolean equals(Object obj) {
        return dimensionvalues.equals(obj);
    }

    public String toString() {
        String retString = "[";
        for(int i = 0 ; i < dimensionvalues.size() ; i++){
            retString += " " + i + " : ";
            retString += dimensionvalues.get(i) + " ";
        }
        retString += "]";
        return retString;
    }
}

