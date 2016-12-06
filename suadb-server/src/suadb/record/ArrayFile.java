package suadb.record;

import suadb.parse.Constant;
import suadb.parse.IntConstant;
import suadb.tx.Transaction;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static suadb.file.Page.BLOCK_SIZE;
import static suadb.file.Page.INT_SIZE;
import static java.sql.Types.*;

/**
 * Created by ILHYUN on 2016-11-17.
 */
public class ArrayFile
{
    private ArrayInfo ai;
    private Transaction tx;
    private String filename;
    private CellFile currentCFiles[];
	
    // total number of chunks of the array - IHSUH
    private int numberofchunks = 1;

    private List<String> dimensions;
    // total number of dimensions of the array -IHSUH
    private int numberofdimensions;
    // number of chunks per dimension - IHSUh
    private int numberofchunksperdimension[];

    private List<String> attributes;
    private int numberofattributes;

    private List<Integer> currentDimensionValues;//Keep current coordinates of the array.
    private boolean[] rearestAttribute;//Manage rearest attributes. true == rearest

    /**sdf
     * Constructs an object to manage a suadb.file of records.
     * If the suadb.file does not exist, it is created.
     * @param ai the table suadb.metadata
     * @param tx the transaction
     */
    public ArrayFile(ArrayInfo ai, Transaction tx) {
        this.ai = ai;
        this.tx = tx;

        // set up array information from schema
        dimensions = new ArrayList<String>(ai.schema().dimensions());
        attributes = new ArrayList<String>(ai.schema().attributes());
        this.numberofdimensions = dimensions.size();
        this.numberofattributes = attributes.size();
        currentDimensionValues = new ArrayList<Integer>(this.numberofdimensions);//Keep current coordinates of array.
        rearestAttribute = new boolean[this.numberofattributes];//Manage rearest attributes.


        numberofchunksperdimension = new int[numberofdimensions];
        int index = 0;
        for(String dimname: dimensions){
            // total number of chunks is multiplication of chunks of each dimension
            numberofchunksperdimension[index] = (int)Math.ceil(((float)(ai.schema().end(dimname) - ai.schema().start(dimname)+1))/ai.schema().chunkSize(dimname));
            index++;
        }
        for(int i = 0 ; i < numberofdimensions ; i++){
            numberofchunks *= numberofchunksperdimension[i];
            currentDimensionValues.add(0);//Initialize the current coordinate of an array.(0,0,0,...)
        }

        // set up CellFiles that corresponds to the array ( for each attribute )
        currentCFiles = new CellFile[numberofattributes];
        int j = 0;
        for(String attributename : attributes){
            currentCFiles[j] = new CellFile(ai,tx,0,attributename,getNumberOfBlocksPerChunk(attributename),numberofchunks);
            rearestAttribute[j] = true;//Initialize rearestAttribute.
            j++;
        }


    }

    /*
    calculates the number of blocks in a chunk for a given attributename
     */
    private int getNumberOfBlocksPerChunk(String attributename){
        int numberofcellsinablock = 1;
        int numberofcellsinachunk = 1;

        int recsize = ai.recordLength(attributename) + INT_SIZE;

        numberofcellsinablock = (int)Math.floor((double)BLOCK_SIZE / recsize);

        for(String dimensionname : dimensions){
            numberofcellsinachunk*= ai.schema().chunkSize(dimensionname);
        }
        return (int) Math.ceil( (double)numberofcellsinachunk / numberofcellsinablock);
    }
    
    private int getChunknumber(CID cid){
        int chunkindex[] = new int [numberofdimensions];
        int chunkOffset = 0;
        for(int i = 0 ; i <numberofdimensions ; i++){
            chunkindex[i] = ( (cid.dimensionValues().get(i) - ai.schema().start(dimensions.get(i)) ) / ai.schema().chunkSize(dimensions.get(i)));
        }
        for(int i = 0  ; i < numberofdimensions ; i++){
            int numberchunksinfollowingdimension = 1;
            for (int j = i + 1; j < numberofdimensions ; j++){
                numberchunksinfollowingdimension *= numberofchunksperdimension[j];
            }
            chunkOffset += numberchunksinfollowingdimension * (chunkindex[i]);
        }
        return chunkOffset;
    }

    // calculate the offset of cell in C style order
    // for example, the offset of (2,3) in [0:3,0:9] array is ((9 + 1)  * (2) ) + 3  = 23
    // watch out for incrementing by 1 of the given index;
    // TODO ::needs test!! - IHSUh
    private int calculateCellOffsetInChunk(CID cid){

        int dimensionvalueinchunk[] = new int [numberofdimensions];
        int offset = 0;
        for(int i = 0 ; i < numberofdimensions ; i++){
            dimensionvalueinchunk[i] = cid.dimensionValues().get(i) - ai.schema().start(dimensions.get(i));
            dimensionvalueinchunk[i] %= ai.schema().chunkSize(dimensions.get(i));
         }

        for(int i = 0  ; i < numberofdimensions ; i++){
            int numbercellsinfollowingdimension = 1;
            for (int j = i+1; j < numberofdimensions ; j++){
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
        for( int j = 0 ; j < numberofattributes ; j++){
            currentCFiles[j].close();
        }
    }

    /**
     * Positions the current suadb.record so that a call to method next
     * will wind up at the first suadb.record.
     */
    public void beforeFirst() {
        for( int j = 0 ; j < numberofattributes ; j++){
            currentCFiles[j].beforeFirst();
   //         currentCFiles[j].moveTo(0);
    //        currentCFiles[j].moveToId(0);
            rearestAttribute[j] = true;//Initialize rearestAttributes to True.

        }
    }

    public void beforeFirst(String attributename) {
        int index = attributes.indexOf(attributename);
        if( index < 0)
            return;
        currentCFiles[index].beforeFirst();
        rearestAttribute[index] = true;//Initialize rearestAttributes to True.
   //     currentCFiles[index].moveTo(0);
   //     currentCFiles[index].moveToId(0);
    }

	/**
     * Is this attribute 'Null' in the current dimension value?
     * @param whichAttribute
     * @return
     */
    boolean isNullAttribute(int whichAttribute){
        return !rearestAttribute[whichAttribute];
    }
    /**
     * Moves to the next suadb.cell.
     * @return false if there is no next suadb.cell.
     */

    public boolean next(){
        return next(attributes);
    }

    int currentChunkNum = -1; //Just for the next() function.
    public boolean next(List<String> attributesName){
        boolean result = false;
        boolean initial = true;
        List<Integer> rearCandidate = new ArrayList<Integer>(numberofattributes);



        for(String attribute : attributesName){
            int index = attributes.indexOf(attribute);
            if(index < 0)//Invalid attribute name
                return false;
            if(rearestAttribute[index]){
                result |= currentCFiles[index].next();//Call next() if the attribute is rearest.
            }

            List<Integer> dimension = currentCFiles[index].getCurrentCID().dimensionValues();
            if(initial)
                currentChunkNum = currentCFiles[index].getCurrentchunknum();

            boolean[] rearestTestResult = rearestTest(dimension,currentCFiles[index].getCurrentchunknum());
            if(initial || rearestTestResult[0]) {//Update the current dimension value.
                for(int i=0;i<numberofdimensions;i++)
                    currentDimensionValues.set(i,dimension.get(i));
                if(rearestTestResult[1])//If all equal to current dimension value
                    rearCandidate.add(index);
                else {//New rearer attribute is detected.
                    rearCandidate.clear();
                    rearCandidate.add(index);
                }
                initial = false;
            }
        }

        //Update rearestAttribute
        for(int i=0;i<numberofattributes;i++)
            rearestAttribute[i] = false;
        for(int i=0;i<rearCandidate.size();i++)
            rearestAttribute[rearCandidate.get(i)] = true;

        return result;
    }

	/**
     * Compare between currentDimensionValues and an dimension value.
     * @param dimensionValue
     * @param chunkNum
     * @return rearestTest[0] == true : The attribute is rearest
     *         rearestTest[1] == true : The attribute is equal to previous attributes.
     */
    public boolean[] rearestTest(List<Integer> dimensionValue,int chunkNum){

        //If each attribute has a different chunk number,
        if(chunkNum < currentChunkNum) {
            currentChunkNum = chunkNum;
            return new boolean[]{true, false};
        }
        else if(chunkNum > currentChunkNum){
            return new boolean[]{false, false};
        }



        for(int i=0;i<numberofdimensions;i++)
            if(dimensionValue.get(i) > currentDimensionValues.get(i))
                return new boolean[]{false,false};
            else if(dimensionValue.get(i) < currentDimensionValues.get(i))
                return new boolean[]{true,false};//The dimension is the rearest.

        return new boolean[]{true,true};//The dimension is the rearest.
    }

	/**
     * Current dimension of this array for ARAM.
     * @return CID
     */
    public CID getCurrentDimensionValues(){
        return new CID(currentDimensionValues,ai);
    }

    public boolean next(String attributename) {
        int index = attributes.indexOf(attributename);
        if( index < 0)
            return false;
        return currentCFiles[index].next();
    }

    /**
     * Returns the value of the specified field
     * in the current suadb.record.
     * @return the integer value at that field
     */
    public int getInt(String attributename) {
        int index = attributes.indexOf(attributename);
        if( index < 0)
            return -1;
        return currentCFiles[index].getInt();
    }
    public int getInt(int attributeIndex) {
        if( attributeIndex < 0)
            return -1;
        return currentCFiles[attributeIndex].getInt();
    }

    /**
     * Returns the value of the specified field
     * in the current suadb.record.
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
     * @param val the new value for the field
     */
    public void setInt(String attributename,int val) {
        int index = attributes.indexOf(attributename);
        if (index < 0)
            return ;
        currentCFiles[index].setInt(val);
    }
    public void setInt(int attributeIndex,int val) {
        if (attributeIndex < 0)
            return ;
        currentCFiles[attributeIndex].setInt(val);
    }

    /**
     * Sets the value of the specified field
     * in the current suadb.record.
     * @param val the new value for the field
     */
    public void setString(String attributename,String val) {
        int index = attributes.indexOf(attributename);
        if (index < 0)
            return ;
        currentCFiles[index].setString(val);
    }
    public void setString(int attributeIndex,String val) {
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
        for( int j = 0 ; j < numberofattributes ; j++){
            currentCFiles[j].delete();
        }
    }

    /**
     * Positions the current suadb.record as indicated by the
     * specified CID.
     * @param cid a suadb.record identifier
     */
    public void moveToCid(CID cid) {

        for( int j = 0 ; j < numberofattributes ; j++) {
            currentCFiles[j].moveTo(getChunknumber(cid));
            currentCFiles[j].moveToId(calculateCellOffsetInChunk(cid));

            rearestAttribute[j] = true;//Initialize rearestAttributes to True.
        }

        //Update current dimension value.
        for(int i=0;i<numberofdimensions;i++)
            currentDimensionValues.set(i,cid.dimensionValues().get(i));
    }

    public void moveToCid(String attributename, CID cid) {
        int index = attributes.indexOf(attributename);
        if (index < 0)
            return ;
        currentCFiles[index].moveTo(getChunknumber(cid));
        currentCFiles[index].moveToId(calculateCellOffsetInChunk(cid));

        //Update current dimension value.
        for(int i=0;i<numberofdimensions;i++)
            currentDimensionValues.set(i,cid.dimensionValues().get(i));

        //Update rearestAttribute
        for(int j=0;j<numberofattributes;j++)//If moveToCid(String,CID) is called,
            rearestAttribute[j] = true;//Assume all attributes are the rearest.
        //So, if next() is called followed by this method, all attributes can call next();
    }

    /**
     * Returns the RID of the current suadb.record.
     * @return a suadb.record identifier
     */

    // TODO :: this method should be implemented - IHSUh
    /*
    public CID currentCid() {
        int id = cp.currentId();
        return new CID(currentchunknum, id);
    }
    */
    
    public int getDimension(String dimName)
    {
	    CID cid = getCurrentDimensionValues();
	    int dIndex = dimensions.indexOf(dimName);
	    
	    return cid.dimensionValues().get(dIndex);
    }
    
    public Constant getDimensionVal(String dimName)
    {
	    CID cid = getCurrentDimensionValues();
	    int dIndex = dimensions.indexOf(dimName);
	    
	    return new IntConstant(cid.dimensionValues().get(dIndex));
    }

	// TODO :: Insert()                     - RonyK
	// insert data sequentially.
	// TODO :: Insert(Dimension[] dim)      - RonyK


    /**
     * Load data from the file.
     * @param fileName
     * @return
     */
    public boolean input(String fileName){
        String s;
        Schema schema = ai.schema();

        List<Integer> dimensionValue = new ArrayList<Integer>();
        int[] dimensionLengths = new int[numberofdimensions];
        for(int i=0;i<numberofdimensions;i++) {
            dimensionValue.add(0);
            dimensionLengths[i] = schema.dimensionLength(dimensions.get(i));
        }
        CID cid = new CID(dimensionValue , ai);

        //Identify the types of attributes.
        int[] attributeTypes = new int[numberofattributes];
        for(int i=0;i<numberofattributes;i++)
            attributeTypes[i] = schema.type(attributes.get(i));




        //Open the file.
        try {
            BufferedReader in = new BufferedReader(new FileReader(fileName));
            while ((s = in.readLine()) != null) {
                String[] line = s.split("\\)");

                //One cell
                for(int i=0;i<line.length;i++){
                    String token = line[i];
                    if(!token.contains("("))
                        continue;

                    moveToCid(cid);
                    String[] realToken = token.split("\\(");
                    if(realToken.length > 1) {
                        String[] value = realToken[1].split(",");
                        for(int j=0;j<value.length;j++){
                            if(value[j].equals("")) {//empty attribute

                            }
                            else if(attributeTypes[j] == INTEGER)
                                currentCFiles[j].setInt(Integer.parseInt(value[j]));
                            else if(attributeTypes[j] == VARCHAR)
                                currentCFiles[j].setString(value[j]);
                        }
                    }

                    dimensionValue.set(numberofdimensions-1,dimensionValue.get(numberofdimensions-1)+1);
                    //Update the coordinate.
                    for(int d=numberofdimensions-1;d>0;d--) {
                        if(dimensionValue.get(d) == dimensionLengths[d]){
                            dimensionValue.set(d,0);
                            dimensionValue.set(d-1,dimensionValue.get(d-1)+1);
                        }
                    }
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

	/**
     * Print a cell with next().
     * @return
     */
    private String printCell(){
        String result = getCurrentDimensionValues()+" ";
        Schema schema = ai.schema();
        for(int i=0;i<numberofattributes;i++){
            if(!isNullAttribute(i)) {
                if(schema.type(attributes.get(i)) == INTEGER)
                    result += getInt(attributes.get(i));
                else if(schema.type(attributes.get(i)) == VARCHAR)
                    result += getString(attributes.get(i));
                else if(schema.type(attributes.get(i)) == DOUBLE){

                }
            }
            else
                result += "null";

            if(i != numberofattributes-1)
                result += ",";
        }

        return result;
    }

	/**
	 * Print array to the console.
     */
    public int printArray(){
        int count=0;
        beforeFirst();


        //Print dimension and attribute information at first line.
        String firstLine="{";
        for(int i=0;i<numberofdimensions;i++){
            firstLine += dimensions.get(i);
            if(i != numberofdimensions-1)
                firstLine += ",";
            else
                firstLine += "} ";
        }

        for(int i=0;i<numberofattributes;i++){
            firstLine += attributes.get(i);
            if(i != numberofattributes-1)
                firstLine += ",";
        }
        System.out.println(firstLine);

        while(next()){
            System.out.println(printCell());
            count++;
        }
        return count;
    }



}
