package suadb.test;

import java.util.List;
import java.util.Random;

import suadb.record.Schema;

/**
 * Created by Rony on 2016-12-06.
 */
public class DummyData
{
	public static T3A<Integer, Integer, Integer>[][][] getArrayDummy_3A_3D()
	{
		return new T3A[][][]
				{
						{
								{new T3A<>(0, 100, 1), new T3A(1, 99, null), new T3A(2, null, 3), new T3A(null, 97, 4)},
								{new T3A<>(4, null, null), new T3A(null, 95, null), new T3A(9, null, null), new T3A(null, null, null)},
								{new T3A<>(9, 92, null), new T3A(9, 91, null), new T3A(null, null, 11), new T3A(11, 89, null)},
								{new T3A<>(12, 88, 13), new T3A(null, 13, 14), new T3A(14, 86, 15), new T3A(15, 85, 16)}
						}
				};
	}
	
	public static T3A<Integer, Integer, Integer>[][][] getArrayDummy_3A_3D_ver2()
	{
		return new T3A[][][]
				{
						{
								{new T3A<>(9, 92, 5), new T3A(9, 91, null), new T3A(null, null, 11), new T3A(11, 89, null)},
								{new T3A<>(0, 100, 1), new T3A(1, 99, null), new T3A(2, null, 3), new T3A(null, 97, 4)},
								{new T3A<>(4, null, null), new T3A(null, 95, null), new T3A(9, null, null), new T3A(null, null, null)},
								{new T3A<>(12, 88, 13), new T3A(null, 13, 14), new T3A(14, 86, 15), new T3A(15, 85, 16)}
							
						}
				};
	}
	
	public static String getInputDummy_3A_3D(T3A<Integer, Integer, Integer>[][][] dummy)
	{
		String result = "[";
		int dim_01 = 1;
		int dim_02 = 4;
		int dim_03 = 4;
		
		for(int x = 0; x < dim_01; x++)
		{
			result += "[";
			for(int y = 0; y < dim_02; y++)
			{
				result += "[";
				for(int z = 0; z < dim_03; z++)
				{
					result += dummy[x][y][z].toString();
					
					if(z < dim_03 - 1)
					{
						result += ",";
					}
				}
				result += "]";
				
				if(y < dim_02 - 1)
				{
					result += ",";
				}
			}
			result += "]";
			
			if(x < dim_01 - 1)
			{
				result += ",";
			}
		}
		result += "]";
		
		return result;
	}
	
	public static final String InputDummy_3A_1D =
			"[" +
				"(0,100,1),(1,99),(2,,3),(,97,4),(4,,),(,95,),(6),(),(8,92),(9,91),(,,11),(11,89),(12,88,13),(,13,14),(14,86,15),(15,85,16)" +
			"]";

	public DummyData()
	{
	}

	public int[][][] generate3DDummy(List<Schema.DimensionInfo> dInfos)
	{
		Random random = new Random();

		int dummy[][][] = new int[dInfos.get(0).end() + 1][dInfos.get(1).end() + 1][dInfos.get(2).end() + 1];

		for(int x = dInfos.get(0).start(); x <= dInfos.get(0).end(); x++)
		{
			for(int y = dInfos.get(1).start(); y <= dInfos.get(1).end(); y++)
			{
				for(int z = dInfos.get(2).start(); z <= dInfos.get(2).end(); z++)
				{
					dummy[x][y][z] = random.nextInt();
				}
			}
		}

		return dummy;
	}
}
