package suadb.record;

import static java.sql.Types.*;
import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import suadb.file.Chunk;
import suadb.server.SuaDB;
import suadb.test.SuaDBExeTestBase;
import suadb.test.SuaDBTestBase;
import suadb.tx.Transaction;
import suadb.tx.TransactionTest;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ILHYUN on 2016-11-23.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ArrayFileTest  extends SuaDBExeTestBase
{
    private Schema schema;
    private ArrayInfo arrayinfo;
    private ArrayFile arrayfile;

    //IHSUh
    @Test
    public void test_00_array_creation_write_read(){
        // this is an example of creating an 3-dimensional array with to attributes
        int start[] = {0,0,0};
        int end[] = {9,9,9};
        int chunksize[] = {5,5,5};
	    
        schema  = new Schema();

        for(int i = 0 ; i < 3 ; i++){
            schema .addDimension("dim"+i,start[i],end[i],chunksize[i]);
        }

        schema.addAttribute("attA",INTEGER,999999);   // length doesn't matter for INTEGER type
        schema.addAttribute("attB", VARCHAR, 7);

        arrayinfo = new ArrayInfo("testArray", schema);

        // the below code shows how to write values to array cells

        Transaction tx = new Transaction();
        arrayfile = new ArrayFile(arrayinfo,tx);

        // to specify a cell, you should make a list of dimension values,
        // the order of dimension you put in the list is critical. you should follow the order of array definition
        List<Integer> dimensionvalue = new ArrayList<Integer>();
        dimensionvalue.add(0);
        dimensionvalue.add(0);
        dimensionvalue.add(0);
        CID cid = new CID(dimensionvalue);

        arrayfile.beforeFirst();
        int index = 0;
        for(int i = start[0] ; i <= end[0] ; i++ ) {
            for (int j = start[1]; j <= end[1]; j++) {
                for (int k = start[2]; k <= end[2]; k++) {
                    dimensionvalue.set(0, i);
                    dimensionvalue.set(1, j);
                    dimensionvalue.set(2, k);
                    arrayfile.moveToCid(cid);
                    arrayfile.setInt("attA", index);
                    arrayfile.setString("attB", Integer.toString(index));
                    index++;
                }
            }
        }

        arrayfile.close();
        tx.commit();
	    
        // the below code shows how to read values from array cells
        tx = new Transaction();
        arrayfile = new ArrayFile(arrayinfo,tx);

        arrayfile.beforeFirst();
        index = 0;
        for(int i = start[0] ; i <= end[0] ; i++ ){
            for(int j = start[1] ; j <= end[1] ; j++ )
            {
                for (int k = start[2]; k <= end[2]; k++) {
                    dimensionvalue.set(0, i);
                    dimensionvalue.set(1, j);
                    dimensionvalue.set(2, k);
                    arrayfile.moveToCid(cid);
                    assertTrue( (arrayfile.getInt("attA"))== index) ;
                    assertTrue( (arrayfile.getString("attB")).equals(Integer.toString(index))) ;

                    CID dimensionTest = arrayfile.getCurrentDimension();
                    assertTrue(dimensionTest.dimensionValues().get(0) == i);
                    assertTrue(dimensionTest.dimensionValues().get(1) == j);
                    assertTrue(dimensionTest.dimensionValues().get(2) == k);

                    index++;
                }
            }
        }
        
        arrayfile.close();
        tx.commit();

        // the below code shows how to read values from array cells using next method
        // in this case , only one attribute , namely attA is read from the array
        // note the order of cells read by using next method
        // 1) row major order in a single chunk
        // 2) if has no next cell in  the chunk, then move on to the next chunk

        tx = new Transaction();
        arrayfile = new ArrayFile(arrayinfo,tx);

        arrayfile.beforeFirst();
        index = 0;
        boolean nextFlag;
        for(int i = start[0] ; i <= end[0] ; i++ ){
            for(int j = start[1] ; j <= end[1] ; j++ )
            {
                for (int k = start[2]; k <= end[2]; k++) {

                    nextFlag = arrayfile.next("attA");
                    assertEquals(nextFlag, true);
                    System.out.println("linear offset of the cell :" + arrayfile.getInt("attA"));
                    index++;
                }
            }
        }
        
        nextFlag= arrayfile.next("attA");
        assertEquals(nextFlag, false);
        arrayfile.close();
        tx.commit();
    }

    @AfterClass
    public static void tearDown(){
        try{
            SuaDB.fileMgr().flushAllFiles();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
