package suadb.parse;

/**
 * Created by ILHYUN on 2016-12-06.
 */
public class RemoveArrayData implements QueryData{
    private String arrayName;
    public RemoveArrayData(String arrayName) {
        this.arrayName = arrayName;
    }

    /**
     * Returns the name of the array to be removed.
     * @return the name of the array to be removed
     */
    public String arrayName() {
        return arrayName;
    }

}
