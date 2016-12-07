package suadb.test;

/**
 * Created by Rony on 2016-12-06.
 */
public class T3A<A, B, C>
{
	public final A a;
	public final B b;
	public final C c;
	
	public T3A(A a, B b, C c)
	{
		this.a = a;
		this.b = b;
		this.c = c;
	}
	
	@Override
	public String toString()
	{
		String result = "(";
		if(this.a != null)
		{
			result += a.toString() + ",";
		}else
		{
			result += ",";
		}
		
		if(this.b != null)
		{
			result += b.toString() + ",";
		}else
		{
			result += ",";
		}
		
		if(this.c != null)
		{
			result += c.toString();
		}else
		{
			result += "";
		}
		
		result += ")";
		
		return result;
	}
	
	@Override
	public boolean equals(Object obj)
	{
		if(obj instanceof T3A)
		{
			T3A<A, B, C> t = (T3A<A, B, C>)obj;
			return a.equals(t.a) && b.equals(t.b) && c.equals(t.c);
		}
		
		return false;
		
	}
}