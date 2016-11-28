package suadb.tx;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import suadb.file.Chunk;
import suadb.server.SuaDB;
import suadb.test.SuaDBTestBase;
/**
 * Created by CDS on 2016-11-22.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TransactionTest extends SuaDBTestBase{
	@BeforeClass
	public static void beforeClass(){
		SuaDB.init(dbName);
	}

	@Test
	public void test0_basic_pin_setInt_getInt(){
		Transaction tx = new Transaction();
		Chunk chunk1 = new Chunk("chunk1",1,3);//A chunk that has 3 blocks.
		Chunk chunk2 = new Chunk("chunk2",2,5);//A chunk that has 5 blocks.
		tx.pin(chunk1);
		tx.pin(chunk2);

		tx.setInt(chunk1,0,4);
		assertTrue( tx.getInt(chunk1,0) == 4 );
		assertTrue( tx.getInt(chunk2,0) == 0 );

		tx.setInt(chunk1,500,4);//Assign an INT value above one block size.
		assertTrue( tx.getInt(chunk1,500) == 4 );

		tx.commit();
	}

	@Test
	public void test1_basic_pin_setString_getString(){
		Transaction tx = new Transaction();
		Chunk chunk1 = new Chunk("chunk1",1,3);//A chunk that has 3 blocks.
		Chunk chunk2 = new Chunk("chunk2",2,5);//A chunk that has 5 blocks.
		tx.pin(chunk1);
		tx.pin(chunk2);

		tx.setString(chunk1,0,"SuaDB Test");
		assertTrue( (tx.getString(chunk1,0)).equals("SuaDB Test") );
		assertTrue( (tx.getString(chunk2,0)).equals("") );

		tx.setString(chunk1,500,"SuaDB Test2");//Assign a STRING value above one block size.
		assertTrue( (tx.getString(chunk1,500)).equals("SuaDB Test2") );

		tx.commit();
	}

	@Test
	public void test2_basic_pin_setDouble_getDouble(){
		Transaction tx = new Transaction();
		Chunk chunk1 = new Chunk("chunk1",1,3);//A chunk that has 3 blocks.
		tx.pin(chunk1);

		tx.setDouble(chunk1,0,3.14);
		assertTrue( tx.getDouble(chunk1,0) == 3.14 );

		tx.setDouble(chunk1,500,4.3245);//Assign an DOUBLE value above one block size.
		assertTrue( tx.getDouble(chunk1,500) == 4.3245 );

		tx.commit();
	}

	@Test
	public void test3_basic_pin_setInt_getInt_setString_getString(){
		Transaction tx = new Transaction();
		Chunk chunk1 = new Chunk("chunk1",1,3);//A chunk that has 3 blocks.
		tx.pin(chunk1);

		tx.setInt(chunk1,0,4);
		tx.setString(chunk1,14 ,"SuaDB Test");

		assertTrue( (tx.getInt(chunk1,0))==4 );
		assertTrue( (tx.getString(chunk1,14)).equals("SuaDB Test") );

		tx.commit();
	}


	@Test
	public void test4_concurrent_three_transactions(){
		TestA t1 = new TestA();
		new Thread(t1).start();
		TestB t2 = new TestB();
		new Thread(t2).start();
		TestC t3 = new TestC();
		new Thread(t3).start();

		try {//wait until threads finish. (Don't start tearDown() immediately)
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@AfterClass
	public static void tearDown(){
		try{
			SuaDB.fileMgr().flushAllFiles();
		}catch (Exception e){
			e.printStackTrace();
		}
	}


	class TestA implements Runnable{
		public void run(){
			try{
				Transaction tx = new Transaction();
				Chunk chunk1 = new Chunk("junk1",1,3);//A chunk that has 3 blocks.
				Chunk chunk2 = new Chunk("junk2",2,5);//A chunk that has 5 blocks.
				tx.pin(chunk1);
				tx.pin(chunk2);
				System.out.println("Tx A: read 1 start");
				tx.getInt(chunk1,0);
				System.out.println("Tx A: read 1 end");
				Thread.sleep(1000);
				System.out.println("Tx A: read 2 start");
				tx.getInt(chunk2,0);
				System.out.println("Tx A: read 2 end");
				tx.commit();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	class TestB implements Runnable{
		public void run(){
			try{
				Transaction tx = new Transaction();
				Chunk chunk1 = new Chunk("junk1",1,3);//A chunk that has 3 blocks.
				Chunk chunk2 = new Chunk("junk2",2,5);//A chunk that has 5 blocks.
				tx.pin(chunk1);
				tx.pin(chunk2);
				System.out.println("Tx B: write 2 start");
				tx.setInt(chunk2,0,0);
				System.out.println("Tx B: write 2 end");
				Thread.sleep(1000);
				System.out.println("Tx B: read 1 start");
				tx.getInt(chunk1,0);
				System.out.println("Tx B: read 1 end");
				tx.commit();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	class TestC implements Runnable{
		public void run(){
			try{
				Transaction tx = new Transaction();
				Chunk chunk1 = new Chunk("junk1",1,3);//A chunk that has 3 blocks.
				Chunk chunk2 = new Chunk("junk2",2,5);//A chunk that has 5 blocks.
				tx.pin(chunk1);
				tx.pin(chunk2);
				System.out.println("Tx C: write 1 start");
				tx.setInt(chunk1,0,0);
				System.out.println("Tx C: write 1 end");
				Thread.sleep(1000);
				System.out.println("Tx C: read 2 start");
				tx.getInt(chunk2,0);
				System.out.println("Tx C: read 2 end");
				tx.commit();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}