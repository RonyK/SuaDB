package suadb.query.afl;

import java.util.ArrayList;
import java.util.List;

import suadb.exceptions.CannotComparableException;
import suadb.parse.Constant;
import suadb.query.Region;
import suadb.query.Scan;
import suadb.record.CID;
import suadb.record.Schema;

/**
 * Created by Rony on 2016-12-12.
 */
public class JoinScan implements Scan
{
	private Scan left, right;
	private Scan rightBetweenScan;
	private Schema schema;
	private Region leftChunkRegion;
	
	private List<Schema.DimensionInfo> dInfos;
	private int dimSize;
	private CID leftCID;
	private CID rightCID;
	
	private static boolean stopRight = false;
	
	public JoinScan(Scan left, Scan right, Schema schema)
	{
		this.left = left;
		this.right = right;
		this.schema = schema;
		this.dInfos = new ArrayList<>();
		dInfos.addAll(schema.dimensionInfo().values());
		this.dimSize = dInfos.size();
		
		List<Integer> low = new ArrayList<>();
		for(int i = 0; i < dInfos.size(); i++)
		{
			low.add(dInfos.get(i).start());
		}
		
		leftChunkRegion = calcTargetChunk(new CID(low));
		rightBetweenScan =  new BetweenScan(right, leftChunkRegion, schema);
	}
	
	@Override
	public void beforeFirst()
	{
		left.beforeFirst();
		rightBetweenScan.beforeFirst();
	}
	
	@Override
	public boolean next()
	{
		try
		{
			while(left.next())
			{
				leftCID = left.getCurrentDimension();
				if(leftChunkRegion.compareTo(leftCID) != 0)
				{
					leftChunkRegion = calcTargetChunk(leftCID);
					rightBetweenScan = new BetweenScan(this.right, leftChunkRegion, schema);
//			        System.out.println(
//				        String.format(
//							"Next Chunk (%d %d %d) (%d %d %d)",
//							    leftChunkRegion.low().get(0), leftChunkRegion.low().get(1), leftChunkRegion.low().get(2),
//							    leftChunkRegion.high().get(0), leftChunkRegion.high().get(1), leftChunkRegion.high().get(2)));
				}
				
				while(stopRight || (rightBetweenScan.next() && leftCID.compareTo(rightBetweenScan.getCurrentDimension()) >= 0))
				{
					this.rightCID = rightBetweenScan.getCurrentDimension();
					if(leftCID.compareTo(this.rightCID) == 0)
					{
						return true;
					}
					stopRight = false;
				}
			}
		} catch (CannotComparableException e)
		{
			e.printStackTrace();
		}
		
		return false;
	}
	
	@Override
	public void close()
	{
		left.close();
		right.close();
		rightBetweenScan.close();
	}
	
	@Override
	public Constant getVal(String fldname)
	{
		if(left.hasField(fldname))
		{
			return left.getVal(fldname);
		}else if(rightBetweenScan.hasField(fldname))
		{
			return rightBetweenScan.getVal(fldname);
		}
		
		throw new RuntimeException("field " + fldname + " not found.");
	}
	
	@Override
	public int getInt(String fldname)
	{
		if(left.hasField(fldname))
		{
			return left.getInt(fldname);
		}else if(rightBetweenScan.hasField(fldname))
		{
			return rightBetweenScan.getInt(fldname);
		}
		
		throw new RuntimeException("field " + fldname + " not found.");
	}
	
	@Override
	public String getString(String fldname)
	{
		if(left.hasField(fldname))
		{
			return left.getString(fldname);
		}else if(rightBetweenScan.hasField(fldname))
		{
			return rightBetweenScan.getString(fldname);
		}
		
		throw new RuntimeException("field " + fldname + " not found.");
	}
	
	@Override
	public boolean isNull(String attrName)
	{
		if(left.hasField(attrName))
		{
			return left.isNull(attrName);
		}else if(rightBetweenScan.hasField(attrName))
		{
			CID cur = rightBetweenScan.getCurrentDimension();
			System.out.println("RIGHT(" + attrName + ") isNull ? : " + cur.toString());
			return rightBetweenScan.isNull(attrName);
		}
		
		throw new RuntimeException("field " + attrName + " not found.");
	}
	
	@Override
	public boolean hasField(String fldname)
	{
		if(left.hasField(fldname) || rightBetweenScan.hasField(fldname))
		{
			return true;
		}
		
		return false;
	}
	
	@Override
	public boolean hasDimension(String dimName)
	{
		if(left.hasDimension(dimName))
		{
			return true;
		}
		
		return false;
	}
	
	@Override
	public Constant getDimensionVal(String dimName)
	{
		return left.getDimensionVal(dimName);
	}
	
	@Override
	public int getDimension(String dimName)
	{
		return left.getDimension(dimName);
	}
	
	@Override
	public CID getCurrentDimension()
	{
		return left.getCurrentDimension();
	}
	
	@Override
	public void moveToCid(CID cid)
	{
		left.moveToCid(cid);
		rightBetweenScan.moveToCid(cid);
		
		stopRight = true;
	}
	
	private Region calcTargetChunk(CID cid)
	{
		List<Integer> coor = cid.toList();
		List<Integer> low = new ArrayList<>();
		List<Integer> high = new ArrayList<>();
		
		int chunkIndex[] = new int[dimSize];
		
		for(int i = 0; i < dimSize; i++)
		{
			Schema.DimensionInfo dInfo = dInfos.get(i);
			chunkIndex[i] = (coor.get(i) - dInfo.start()) / dInfo.chunkSize();
			
			int start = chunkIndex[i] * dInfo.chunkSize() + dInfo.start();
			low.add(start);
			high.add(start + dInfo.chunkSize() - 1);
		}
		
		return new Region(low, high);
	}
}

