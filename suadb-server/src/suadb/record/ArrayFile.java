package suadb.record;

import exception.ArrayInputException;
import suadb.parse.Constant;
import suadb.parse.IntConstant;
import suadb.query.Region;
import suadb.tx.Transaction;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static suadb.file.Page.BLOCK_SIZE;
import static suadb.file.Page.INT_SIZE;
import static java.sql.Types.*;

/**
 * Created by ILHYUN on 2016-11-17.
 */
public class ArrayFile {
	private ArrayInfo ai;
	private CellFile currentCFiles[];

	private int numberOfChunks = 1;             // total number of chunks of the array - IHSUH
	private int currentChunkNum = -1;           //Just for the next() function.

	private List<String> dimensions;
	private int numberOfDimensions;             // total number of dimensions of the array -IHSUH
	private int numberOfChunksPerDimension[];   // number of chunks per dimension - IHSUh
	private int numberOfCellsInChunk;
	private int numChunksFollowingDim[];
	private int numCellsFollowingDim[];
	private int chunkSizes[];

	private List<String> attributes;
	private int numberOfAttributes;

	private int currentDimensionValues[];       //Keep current coordinates of the array.
	
	private List<Schema.DimensionInfo> dInfos;
	
	private static Region currentChunkRegion;
	
	/**
	 * sdf
	 * Constructs an object to manage a suadb.file of records.
	 * If the suadb.file does not exist, it is created.
	 *
	 * @param ai the table suadb.metadata
	 * @param tx the transaction
	 */
	public ArrayFile(ArrayInfo ai, Transaction tx) {
		this.ai = ai;

		// set up array information from schema
		dimensions = new ArrayList<>(ai.schema().dimensions());
		attributes = new ArrayList<>(ai.schema().attributes());
		
		this.numberOfDimensions = dimensions.size();
		this.numberOfAttributes = attributes.size();
		
		currentDimensionValues = ai.schema().dimensionInfo().values().stream().mapToInt(d -> d.start()).toArray();
		chunkSizes = ai.schema().dimensionInfo().values().stream().mapToInt(d -> d.chunkSize()).toArray();
		numberOfChunksPerDimension = ai.schema().dimensionInfo().values().stream().mapToInt(d -> d.numOfChunk()).toArray();
		
		numChunksFollowingDim = new int[numberOfDimensions + 1];
		Arrays.fill(numChunksFollowingDim, 1);
		numCellsFollowingDim = new int[numberOfDimensions + 1];
		Arrays.fill(numCellsFollowingDim, 1);
		
		int factor = 1;
		int cellFactor = 1;
		for (int i = numberOfDimensions - 1; i >= 0; i--)
		{
			numberOfChunks *= numberOfChunksPerDimension[i];
			factor *= numberOfChunksPerDimension[i];
			numChunksFollowingDim[i] = factor;

			cellFactor *= chunkSizes[i];
			numCellsFollowingDim[i] = cellFactor;
		}
		
		numberOfCellsInChunk = numCellsFollowingDim[0];
		
		this.dInfos = new ArrayList<>();
		for (Schema.DimensionInfo dInfo : ai.schema().dimensionInfo().values())
		{
			dInfos.add(dInfo);
		}

		// set up CellFiles that corresponds to the array ( for each attribute )
		currentCFiles = new CellFile[numberOfAttributes];
		int j = 0;
		for (String attributename : attributes) {
			currentCFiles[j] = new CellFile(ai, tx, 0, attributename, getNumberOfBlocksPerChunk(attributename), numberOfChunks);
			j++;
		}
	}

	/*
	calculates the number of blocks in a chunk for a given attributename
	 */
	private int getNumberOfBlocksPerChunk(String attributename) {
		int numberofcellsinablock = 1;
		int numberofcellsinachunk = 1;

		int recsize = ai.recordLength(attributename) + INT_SIZE;

		numberofcellsinablock = (int) Math.floor((double) BLOCK_SIZE / recsize);

		for (String dimensionname : dimensions) {
			numberofcellsinachunk *= ai.schema().chunkSize(dimensionname);
		}
		return (int) Math.ceil((double) numberofcellsinachunk / numberofcellsinablock);
	}

	// calculate the offset of cell in C style order
	// for example, the offset of (2,3) in [0:3,0:9] array is ((9 + 1)  * (2) ) + 3  = 23
	// watch out for incrementing by 1 of the given index;
	// TODO ::needs test!! - IHSUh
	private int calcCellOffsetInChunk(CID cid) {
		int dimensionvalueinchunk[] = new int[numberOfDimensions];
		int offset = 0;
		for (int i = 0; i < numberOfDimensions; i++) {
			dimensionvalueinchunk[i] = cid.toList().get(i) - ai.schema().start(dimensions.get(i));
			dimensionvalueinchunk[i] %= ai.schema().chunkSize(dimensions.get(i));
		}

		for (int i = 0; i < numberOfDimensions; i++) {
			int numbercellsinfollowingdimension = 1;
			for (int j = i + 1; j < numberOfDimensions; j++) {
				numbercellsinfollowingdimension *= ai.schema().chunkSize(dimensions.get(j));
			}
			offset += numbercellsinfollowingdimension * (dimensionvalueinchunk[i]);
		}
		return offset;
	}

	/**
	 * Closes the suadb.record suadb.file.
	 */
	public void close() {
		for (int j = 0; j < numberOfAttributes; j++) {
			try {
				currentCFiles[j].close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Positions the current suadb.record so that a call to method next
	 * will wind up at the first suadb.record.
	 */
	public void beforeFirst() {
		for (int j = 0; j < numberOfAttributes; j++) {
			currentCFiles[j].beforeFirst();
		}
	}

	public void beforeFirst(String attributename) {
		int index = attributes.indexOf(attributename);
		if (index < 0)
			return;
		currentCFiles[index].beforeFirst();
	}

	/**
	 * Is this attribute 'Null' in the current dimension value?
	 *
	 * @param attIndex
	 * @return
	 */
	private boolean isNull(int attIndex) {
		return currentCFiles[attIndex].isNull();
	}

	public boolean isNull(String attrName) {
		return isNull(attributes.indexOf(attrName));
	}

	/**
	 * Moves to the next suadb.cell.
	 *
	 * @return false if there is no next suadb.cell.
	 */
	private boolean next(String attributeName)
	{
		int index = attributes.indexOf(attributeName);
		if (index < 0)
		{
			return false;       //Invalid attribute name
		}
			
		return currentCFiles[index].next();
	}
	
	public boolean next()
	{
		return next(attributes);
	}

	public boolean next(List<String> attributesName) {
		boolean result;
		boolean isNull = true;
		
		// TODO ::Reset All attribute at same current position
//		for(int i = 0; i < attributesName.size(); i++)
//		{
//
//		}
//
		do
		{
			result = false;
			
			for(int i = 0; i < attributesName.size(); i++)
			{
				result |= next(attributes.get(i));
				isNull &= currentCFiles[i].isNull();
			}
		} while (isNull && result);

		int index = attributes.indexOf(attributesName.get(0));
		currentDimensionValues = getCIDFrom(currentCFiles[index]).toArray();
		currentChunkNum = currentCFiles[index].currentChunkNum();
		
		return result;
	}

	/**
	 * Current dimension of this array for ARAM.
	 *
	 * @return CID
	 */
	public CID getCID() {
		return new CID(currentDimensionValues);
	}

	/**
	 * Returns the value of the specified field
	 * in the current suadb.record.
	 *
	 * @return the integer value at that field
	 */
	public int getInt(String attributename) {
		int index = attributes.indexOf(attributename);
		if (index < 0)
			return -1;
		return currentCFiles[index].getInt();
	}

	public int getInt(int attributeIndex) {
		if (attributeIndex < 0)
			return -1;
		return currentCFiles[attributeIndex].getInt();
	}

	/**
	 * Returns the value of the specified field
	 * in the current suadb.record.
	 *
	 * @return the string value at that field
	 */
	public String getString(String attributename) {
		int index = attributes.indexOf(attributename);
		if (index < 0)
			return null;
		return currentCFiles[index].getString();
	}

	public String getString(int attributeIndex) {
		if (attributeIndex < 0)
			return null;
		return currentCFiles[attributeIndex].getString();
	}

	/**
	 * Sets the value of the specified field
	 * in the current suadb.record.
	 *
	 * @param val the new value for the field
	 */
	public void setInt(String attributename, int val) {
		int index = attributes.indexOf(attributename);
		if (index < 0)
			return;
		currentCFiles[index].setInt(val);
	}

	public void setInt(int attributeIndex, int val) {
		if (attributeIndex < 0)
			return;
		currentCFiles[attributeIndex].setInt(val);
	}

	/**
	 * Sets the value of the specified field
	 * in the current suadb.record.
	 *
	 * @param val the new value for the field
	 */
	public void setString(String attributename, String val) {
		int index = attributes.indexOf(attributename);
		if (index < 0)
			return;
		currentCFiles[index].setString(val);
	}

	public void setString(int attributeIndex, String val) {
		currentCFiles[attributeIndex].setString(val);
	}

	/**
	 * Deletes the current suadb.record.
	 * The client must call next() to move to
	 * the next suadb.record.
	 * Calls to methods on a deleted suadb.record
	 * have unspecified behavior.
	 */
	public void delete() {
		for (int j = 0; j < numberOfAttributes; j++) {
			currentCFiles[j].delete();
		}
	}
	
	public void moveToCid(CID cid)
	{
		moveToCid(cid, 'r');
	}
	
	public void moveToCidWriteMode(CID cid)
	{
		moveToCid(cid, 'w');
	}

	/**
	 * Positions the current suadb.record as indicated by the
	 * specified CID.
	 * @param cid
	 * @param mode "r" or "w"
	 */
	private void moveToCid(CID cid, char mode){
		for (int j = 0; j < numberOfAttributes; j++) {
			currentCFiles[j].moveTo(getChunknumber(cid));
			currentCFiles[j].moveToId(calcCellOffsetInChunk(cid));
		}

		currentDimensionValues = cid.toArray();
	}
	
	private void moveToCid(String attributename, CID cid, char mode) {
		int index = attributes.indexOf(attributename);
		if (index < 0)
			return;
		currentCFiles[index].moveTo(getChunknumber(cid));
		currentCFiles[index].moveToId(calcCellOffsetInChunk(cid));
		
		currentDimensionValues = cid.toArray();
	}

	public int getDimension(String dimName) {
		CID cid = getCID();
		int dIndex = dimensions.indexOf(dimName);

		return cid.toList().get(dIndex);
	}

	public Constant getDimensionVal(String dimName) {
		CID cid = getCID();
		int dIndex = dimensions.indexOf(dimName);

		return new IntConstant(cid.toList().get(dIndex));
	}

	/**
	 * Load data from the file.
	 *
	 * @param fileName
	 * @return
	 */
	public boolean input(String fileName) throws ArrayInputException {
		String s;
		Schema schema = ai.schema();
		int numberOfOpens = 0;//The number of square brackets.
		boolean ing = false;

		int[] dimensionStart = new int[numberOfDimensions];
		List<Integer> dimensionValue = new ArrayList<>();
		int[] dimensionEnd = new int[numberOfDimensions];
		for (int i = 0; i < numberOfDimensions; i++) {
			dimensionStart[i] = schema.start(dimensions.get(i));
			dimensionValue.add(dimensionStart[i]);
			dimensionEnd[i] = schema.end(dimensions.get(i));
		}
		CID cid = new CID(dimensionValue);

		//Identify the types of attributes.
		int[] attributeTypes = new int[numberOfAttributes];
		for (int i = 0; i < numberOfAttributes; i++)
			attributeTypes[i] = schema.type(attributes.get(i));

		//Open the file.
		try {
			BufferedReader in = new BufferedReader(new FileReader(fileName));
			while ((s = in.readLine()) != null) {
				s = s.replaceAll(" ", ""); // remove all space
				String[] line = s.split("\\)");

				//One cell
				for (int i = 0; i < line.length; i++) {
					String token = line[i];

					//When encountering "[" or "]"
					if (token.contains("[") || token.contains("]")) {
						for (int b = 0; b < token.length(); b++) {
							if (token.charAt(b) == '[') {
								numberOfOpens++;
								ing = false;
							} else if (token.charAt(b) == ']') {
								numberOfOpens--;
								raising(dimensionValue, numberOfOpens, dimensionStart[numberOfOpens]);
								ing = true;
							}
						}
					}

					//Is the dimension Valid? (It filters odd inputs)
					if (!ing) {
						boolean overflowFlag = false;
						for (int d = numberOfDimensions - 1; d >= 0; d--)
							if (dimensionValue.get(d) == dimensionEnd[d] + 1)
								overflowFlag = true;
						if (overflowFlag) {
							in.close();
							throw new ArrayInputException("The input file doesn't match with the array definition.");
						}
					}

					if (!token.contains("("))
						continue;

					moveToCidWriteMode(cid);
					String[] realToken = token.split("\\(");
					if (realToken.length > 1) {
						String[] value = realToken[1].split(",");
						for (int j = 0; j < value.length; j++) {
							if (value[j].equals("")) {//empty attribute

							} else if (attributeTypes[j] == INTEGER)
								currentCFiles[j].setInt(Integer.parseInt(value[j]));
							else if (attributeTypes[j] == VARCHAR)
								currentCFiles[j].setString(value[j]);
						}
					}

					dimensionValue.set(numberOfDimensions - 1, dimensionValue.get(numberOfDimensions - 1) + 1);
				}
			}
			in.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return true;
	}

	private void raising(List<Integer> dimensionValue, int d, int initial) {
		dimensionValue.set(d, initial);
		if (d > 0)
			dimensionValue.set(d - 1, dimensionValue.get(d - 1) + 1);
	}

	/**
	 * Print a cell with next().
	 *
	 * @return
	 */
	private String printCell() {
		String result = getCID() + " ";
		Schema schema = ai.schema();
		for (int i = 0; i < numberOfAttributes; i++) {
			if (!isNull(i)) {
				if (schema.type(attributes.get(i)) == INTEGER)
					result += getInt(attributes.get(i));
				else if (schema.type(attributes.get(i)) == VARCHAR)
					result += getString(attributes.get(i));
				else if (schema.type(attributes.get(i)) == DOUBLE) {

				}
			} else
				result += "null";

			if (i != numberOfAttributes - 1)
				result += ",";
		}

		return result;
	}

	/**
	 * Print array to the console.
	 */
	public int printArray() {
		int count = 0;
		beforeFirst();

		//Print dimension and attribute information at first line.
		String firstLine = "{";
		for (int i = 0; i < numberOfDimensions; i++) {
			firstLine += dimensions.get(i);
			if (i != numberOfDimensions - 1)
				firstLine += ",";
			else
				firstLine += "} ";
		}

		for (int i = 0; i < numberOfAttributes; i++) {
			firstLine += attributes.get(i);
			if (i != numberOfAttributes - 1)
				firstLine += ",";
		}
		System.out.println(firstLine);

		while (next()) {
			System.out.println(printCell());
			count++;
		}

		return count;
	}

	private int getChunknumber(CID cid) {
		int chunkindex[] = new int[numberOfDimensions];
		int chunkOffset = 0;
		for (int i = 0; i < numberOfDimensions; i++) {
			chunkindex[i] = ((cid.toList().get(i) - ai.schema().start(dimensions.get(i))) / ai.schema().chunkSize(dimensions.get(i)));
		}
		for (int i = 0; i < numberOfDimensions; i++) {
			int numberchunksinfollowingdimension = 1;
			for (int j = i + 1; j < numberOfDimensions; j++) {
				numberchunksinfollowingdimension *= numberOfChunksPerDimension[j];
			}
			chunkOffset += numberchunksinfollowingdimension * (chunkindex[i]);
		}
		return chunkOffset;
	}
	
	private Region calcTargetChunk(CID cid)
	{
		List<Integer> coor = cid.toList();
		List<Integer> low = new ArrayList<>();
		List<Integer> high = new ArrayList<>();
		
		int chunkIndex[] = new int[numberOfDimensions];
		
		for(int i = 0; i < numberOfDimensions; i++)
		{
			Schema.DimensionInfo dInfo = dInfos.get(i);
			chunkIndex[i] = (coor.get(i) - dInfo.start()) / dInfo.chunkSize();
			
			int start = chunkIndex[i] * dInfo.chunkSize() + dInfo.start();
			low.add(start);
			high.add(start + dInfo.chunkSize() - 1);
		}
		
		return new Region(low, high);
	}

	public Region calcTargetChunk(int chunkNum)
	{
		List<Integer> low = new ArrayList<>(Arrays.asList(new Integer[numberOfDimensions]));
		List<Integer> high = new ArrayList<>(Arrays.asList(new Integer[numberOfDimensions]));
		
		for(int i = 0; i < numberOfDimensions; i++)
		{
			int coor = (chunkNum) / numChunksFollowingDim[i + 1];
			low.set(i, dInfos.get(i).start() + coor * dInfos.get(i).chunkSize());
			high.set(i, low.get(i) + dInfos.get(i).chunkSize() - 1);

			chunkNum %= numChunksFollowingDim[i + 1];
		}

		return new Region(low, high);
	}
	
	private CID getCIDFrom(CellFile cf)
	{
		List<Integer> coor = new ArrayList<>(Arrays.asList(new Integer[numberOfDimensions]));
		if(currentChunkRegion == null || currentChunkNum != cf.currentChunkNum())
		{
			currentChunkRegion = calcTargetChunk(cf.currentChunkNum());
		}
		
		int offset = cf.currentId();
		for(int i = 0; i < numberOfDimensions; i++)
		{
			int c = offset / numCellsFollowingDim[i + 1];
			coor.set(i, currentChunkRegion.low().get(i) + c);
			offset %= numCellsFollowingDim[i + 1];
		}
//		System.out.println(String.format("Chunk : %d, ID : %d, Coor : %s", cf.currentChunkNum(), cf.currentId(), coor.toString()));
		return new CID(coor);
	}
}
