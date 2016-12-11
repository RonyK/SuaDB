package suadb.query.afl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import suadb.parse.Constant;
import suadb.query.Region;
import suadb.query.Scan;
import suadb.record.CID;
import suadb.record.Schema;
import suadb.record.Schema.*;

/**
 * Created by Rony on 2016-12-09.
 */
public class BetweenScan implements Scan
{
	private Scan s;
	private Region region;
	private boolean moveToCid = false;

	public BetweenScan(Scan s, Region region, Schema schema)
	{
		this.s = s;
		this.region = region;

		moveToCid(new CID(region.low()));
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
			moveToCid = false;
			int compare = region.compareTo(getCurrentDimension());
			if(compare == 0)
			{
				return true;
			}
		}

		while (s.next())
		{
			int compare = region.compareTo(getCurrentDimension());
			if(compare == 0)
			{
				return true;
			}

			
		}
		
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
	
//	private Region calcTargetChunk(CID cid)
//	{
//		List<Integer> coor = cid.dimensionValues();
//		List<Integer> low = new ArrayList<>();
//		List<Integer> high = new ArrayList<>();
//		int size = coor.size();
//
//		for(int i = 0 ; i < size; i++)
//		{
//			DimensionInfo dInfo = dInfos.get(i);
//			int start = dInfo.start() + ((low.get(i) - dInfo.start()) / dInfo.chunkSize()) * dInfo.chunkSize() + dInfo.chunkSize();
//			low.add(start);
//			high.add(start + dInfo.chunkSize() - 1);
//		}
//
//		return new Region(low, high);
//	}
}
