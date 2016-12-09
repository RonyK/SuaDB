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
	private Region curChunk;
	private List<Integer> chunkSize = new ArrayList<>();
	private List<Region> targetChunkRegions = new ArrayList<>();
	
	private List<Integer> endChunkIndex = new ArrayList<>();
	private List<Integer> curChunkIndex = new ArrayList<>();
	
	private List<DimensionInfo> dInfos = new ArrayList<>();
	
	public BetweenScan(Scan s, Region region, Schema schema)
	{
		this.s = s;
		this.region = region;
		
		Map<String, DimensionInfo> dimensionInfo = schema.dimensionInfo();
		int size = dimensionInfo.size();
		int i = 0;
		
		List<Integer> low = region.low();
		List<Integer> high = region.high();
		
		List<List<Integer>> chunkCoors = new ArrayList<>();
		for(DimensionInfo dInfo : dimensionInfo.values())
		{
			dInfos.add(dInfo);
			chunkSize.add(dInfo.chunkSize());
			
			if(dInfo.end() < high.get(i))
			{
				throw new IndexOutOfBoundsException();
			}
			
			chunkCoors.add(new ArrayList<>());
			List<Integer> curChunkCoors = chunkCoors.get(i);
			int start = dInfo.start() + ((low.get(i) - dInfo.start()) / dInfo.chunkSize()) * dInfo.chunkSize() + dInfo.chunkSize();
			curChunkCoors.add(low.get(i));
			
			do
			{
				curChunkCoors.add(start);
				start += dInfo.chunkSize();
			}while (high.get(i) < start);
			
			endChunkIndex.add(curChunkCoors.size());
			curChunkIndex.add(0);
			i++;
		}
		
		while(curChunkIndex.equals(endChunkIndex))
		{
//			targetChunkRegions
		}
		
		moveToCid(new CID(low));
	}
	
	@Override
	public void beforeFirst()
	{
		s.beforeFirst();
	}
	
	@Override
	public boolean next()
	{
		while (s.next())
		{
			int compare = region.compareTo(getCurrentDimension());
			if(compare == 0)
			{
				return true;
			}else
			{
				
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
		curChunk = calcTargetChunk(cid);
		s.moveToCid(cid);
	}
	
	private Region calcTargetChunk(CID cid)
	{
		List<Integer> coor = cid.dimensionValues();
		List<Integer> low = new ArrayList<>();
		List<Integer> high = new ArrayList<>();
		int size = coor.size();
		
		for(int i = 0 ; i < size; i++)
		{
			DimensionInfo dInfo = dInfos.get(i);
			int start = dInfo.start() + ((low.get(i) - dInfo.start()) / dInfo.chunkSize()) * dInfo.chunkSize() + dInfo.chunkSize();
			low.add(start);
			high.add(start + dInfo.chunkSize() - 1);
		}
		
		return new Region(low, high);
	}
}
