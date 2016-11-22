package suadb.buffer;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import suadb.file.Block;
import suadb.file.Chunk;
import suadb.file.FileMgr;
import suadb.server.SuaDB;
import suadb.server.SuaDBTestBase;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
/**
 * Created by Rony on 2016-11-11.
 */
public class dalsuTest extends SuaDBTestBase
{
	@Test(expected = IllegalArgumentException.class)
	public void example(){
		Person p = mock(Person.class);
		assertTrue( p != null );
		when(p.getName()).thenReturn("JDM");

		when(p.getAge()).thenReturn(20);
		assertTrue("JDM".equals(p.getName()));
		assertTrue(20 == p.getAge());


//		doThrow(new IllegalArgumentException()).when(p).setName(eq("JDM"));
//		String name = "JDM";
//		p.setName(name);

		doNothing().when(p).setAge(anyInt());
		p.setAge(20);
		verify(p).setAge(anyInt());
	}

	public class Person {
		private String name;
		private int age;

		public String getName() { return name; }
		public void setName(String name) { this.name = name; }
		public int getAge() { return age; }
		public void setAge(int age) { this.age = age; }
	}
}