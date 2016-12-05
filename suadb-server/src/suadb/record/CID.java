package suadb.record;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ILHYUN on 2016-11-20.
 */
public class CID {
    private List<Integer> dimensionvalues;
    private ArrayInfo arrayInfo;

    /**
     * Creates a CID for the suadb.record having the
     * specified dimension values in the array
     * @param dimensionvalues the dimension values
     */
    public CID(List<Integer> dimensionvalues,ArrayInfo arrayInfo)
    {
        this.dimensionvalues = dimensionvalues;
        this.arrayInfo = arrayInfo;
    }
    
    public List<Integer> dimensionValues() {
        return dimensionvalues;
    }

    public boolean equals(Object obj) {
        return dimensionvalues.equals(obj);
    }

    public String toString() {
        ArrayList<String> dimensions = new ArrayList<String>(arrayInfo.schema().dimensions());

//        String retString = "[";
//        for(int i = 0 ; i < dimensionvalues.size() ; i++){
//            retString += " " + dimensions.get(i) + " : ";
//            retString += dimensionvalues.get(i) + "   ";
//        }
//        retString += "]";

        String retString = "{";//SciDB style
        int dimensionValuesSize = dimensionvalues.size();
        for(int i=0;i<dimensionValuesSize;i++){
            retString += dimensionvalues.get(i);
            if(i != dimensionValuesSize-1)
                retString +=",";
        }
        retString += "}";
        return retString;
    }
}

