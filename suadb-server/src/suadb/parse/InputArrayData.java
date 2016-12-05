package suadb.parse;

/**
 * Created by Aram on 2016-12-05.
 */
public class InputArrayData
{
	private String arrayName;
	private String inputFile;

	public InputArrayData(String arrayName,String inputFile) {
		this.arrayName = arrayName;
		this.inputFile = inputFile;
	}

	/**
	 * Returns the name of the source array.
	 * @return the name of the source array
	 */
	public String arrayName() {
		return arrayName;
	}

	public String fileName() { return inputFile; }
}
