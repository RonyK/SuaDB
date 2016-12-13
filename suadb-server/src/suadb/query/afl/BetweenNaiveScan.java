package suadb.query.afl;

import java.util.ArrayList;
import java.util.List;

import suadb.parse.Constant;
import suadb.query.Region;
import suadb.query.Scan;
import suadb.record.CID;
import suadb.record.Schema;

/**
 * Created by rony on 2016-12-12.
 */
public class BetweenNaiveScan implements Scan
{
	private Scan s;
	private Region region;
	private boolean moveToCid = false;
	private static int count = 0;

	public BetweenNaiveScan(Scan s, Region region)
	{
		this.s = s;
		this.region = region;

		CID startCID = new CID(region.low());
		moveToCid(startCID);
	}

	@Override
	public void beforeFirst()
	{
		s.beforeFirst();
	}

	@Override
	public boolean next()
	{
		if(moveToCid)
		{
			count++;
			moveToCid = false;
			int compare = region.compareTo(getCurrentDimension());
			if(compare == 0)
			{
				return true;
			}
		}

		while (s.next())
		{
			count++;
			CID currentCID = getCurrentDimension();
			int compare = region.compareTo(currentCID);
			if(compare == 0)
			{
				return true;
			}
		}

		System.out.println(String.format("BETWEEN NAIVE SEARCH COUNT : %d", count));

		return false;
	}

	@Override
	public void close()
	{
		s.close();
	}

	@Override
	public Constant getVal(String fldname)
	{
		return s.getVal(fldname);
	}

	@Override
	public int getInt(String fldname)
	{
		return s.getInt(fldname);
	}

	@Override
	public String getString(String fldname)
	{
		return s.getString(fldname);
	}

	@Override
	public boolean isNull(String attrName)
	{
		return s.isNull(attrName);
	}

	@Override
	public boolean hasField(String fldname)
	{
		return s.hasField(fldname);
	}

	@Override
	public boolean hasDimension(String dimName)
	{
		return s.hasDimension(dimName);
	}

	@Override
	public Constant getDimensionVal(String dimName)
	{
		return s.getDimensionVal(dimName);
	}

	@Override
	public int getDimension(String dimName)
	{
		return s.getDimension(dimName);
	}

	@Override
	public CID getCurrentDimension()
	{
		return s.getCurrentDimension();
	}

	@Override
	public void moveToCid(CID cid)
	{
		s.moveToCid(cid);
		moveToCid = true;
	}
}
