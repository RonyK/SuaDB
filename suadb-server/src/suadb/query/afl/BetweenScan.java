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
	private Schema schema;
	private List<DimensionInfo> dInfos;

	private Region currentChunkRegion;
	private int dimSize;

	private static int count = 0;

	public BetweenScan(Scan s, Region region, Schema schema)
	{
		this.s = s;
		this.region = region;
		this.schema = schema;
		this.dimSize = schema.dimensions().size();
		this.dInfos = new ArrayList<>();

		for (DimensionInfo dInfo : schema.dimensionInfo().values())
		{
			dInfos.add(dInfo);
		}

		CID startCID = new CID(region.low());
		moveToCid(startCID);
		currentChunkRegion = calcTargetChunk(startCID);
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

			if(currentChunkRegion.compareTo(currentCID) != 0)
			{
				currentChunkRegion = calcTargetChunk(currentCID);
//				System.out.println(
//						String.format(
//								"Next Chunk (%d %d %d) (%d %d %d)",
//								currentChunkRegion.low().get(0), currentChunkRegion.low().get(1), currentChunkRegion.low().get(2),
//								currentChunkRegion.high().get(0), currentChunkRegion.high().get(1), currentChunkRegion.high().get(2)));
			}

			List<Integer> newCID = new ArrayList<>();
			List<Integer> curCID = currentCID.dimensionValues();

			for(int i = 0; i < dimSize; i++)
			{
				if(curCID.get(i) > region.high().get(i))
				{
					newCID.add(currentChunkRegion.high().get(i));
				}else if(curCID.get(i) <= region.low().get(i))
				{
					newCID.add(region.low().get(i));
				}else
				{
					newCID.add(curCID.get(i));
				}
			}

//			System.out.print(String.format("CUR CID : %d, %d %d", curCID.get(0), curCID.get(1), curCID.get(2)));
//			System.out.println(String.format("\t-> : %d, %d %d", newCID.get(0), newCID.get(1), newCID.get(2)));

			moveToCid(new CID(newCID));
		}

		System.out.println(String.format("BETWEEN SEARCH COUNT : %d", count));

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
	
	private Region calcTargetChunk(CID cid)
	{
		List<Integer> coor = cid.dimensionValues();
		List<Integer> low = new ArrayList<>();
		List<Integer> high = new ArrayList<>();

		int chunkIndex[] = new int[dimSize];

		for(int i = 0; i < dimSize; i++)
		{
			DimensionInfo dInfo = dInfos.get(i);
			chunkIndex[i] = (coor.get(i) - dInfo.start()) / dInfo.chunkSize();

			int start = chunkIndex[i] * dInfo.chunkSize() + dInfo.start();
			low.add(start);
			high.add(start + dInfo.chunkSize() - 1);
		}

		return new Region(low, high);
	}
}
