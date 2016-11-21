package suadb.parse;

import java.io.Reader;
import java.io.StreamTokenizer;

/**
 * Created by rony on 16. 11. 18.
 */
public class SuaDBStreamTokenizer extends StreamTokenizer
{
	boolean _lowerCaseMode = false;
	
	public SuaDBStreamTokenizer(Reader r)
	{
		super(r);
	}
	
	@Override
	public void lowerCaseMode(boolean fl)
	{
		_lowerCaseMode = fl;
	}
	
	public String sval()
	{
		if(_lowerCaseMode)
			return sval.toLowerCase();
		else
			return sval;
	}
	
	public String svalOriginal()
	{
		return sval;
	}
}
