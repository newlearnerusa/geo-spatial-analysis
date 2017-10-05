package dds.phase3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/*
Authors
1. Anshuman Bora (abora3@asu.edu)
2. Ankit Nadig (anadig@asu.edu)
3. Saumya Parikh (ssparik1@asu.edu)
4. Vraj Delhivala (vdelhiva@asu.edu)
*/

class TupleComp implements Comparator<String>, Serializable {

	
	public int compare(String tuple1, String tuple2) {
		Double g1 = Double.valueOf(tuple1.split(",")[0]);
        Double g2 = Double.valueOf(tuple2.split(",")[0]);
        return -1*g1.compareTo(g2);
    }
}

public class App 
{
	public static JavaSparkContext sc;
	public static String inputPath="";
	public static String outputFilePath="";
	public static double latStart=40.5;
	public static double latEnd=40.9;
	public static double longEnd=-73.7;
	public static double longStart=-74.25;
	public static double dayStart=1;
	public static double dayEnd=31;
	public static int timeStep=1;
	public static double cellSize=0.01;
	//cell dimensions= 0.01*0.01*1;

	//each cell has a unique identifier, for eg- latitude between 40.500 and 40.501, need to placed in the same cell

	//we can use the floor function to ensure this
	
	public static Double calculateGetis(String s,double mean, double sd,int totalCells)
	{
		String[] values=s.split(",");
		
		double neighborAttributeSum=Double.parseDouble(values[0]);
		double neighborCount=Double.parseDouble(values[1]);
		
		double upperEquation=neighborAttributeSum-mean*neighborCount;
		double underRoot= (totalCells*neighborCount-Math.pow(neighborCount, 2))/(totalCells-1);
		double lowerEquation=sd*(Math.sqrt(underRoot));
	
		double getis_score=upperEquation/lowerEquation;
		return getis_score;
	}
	//this function determines if a give cell is valid (within our query envelope)
	public static boolean checkifCellisvalid(double x, double y, double z)
	{
		if(y<-7425 || y>-7370)
			return false;
		else if(x<4050 || x> 4090)
			return false;
		else if(z<1 || z>31)
			return false;
		else
			return true;
	}
	// this function returns a list of valid neighbors for a given cell
	public static List<String> getNeighbors(String x,String y, String z)
	{
		List<String> returnList=new LinkedList<String>();
		double _x=Double.parseDouble(x);  //longitude
		double _y=Double.parseDouble(y); //latitude
		double _z=Double.parseDouble(z);  //day
		
		for (double i=-1;i<2;i++)
		{
			for (double j=-1;j<2;j++)
			{
				for(double k=-1;k<2;k++)
				{
					double x1=_x+i;
					double y1=_y+j;
					double z1=_z+k;
					if(checkifCellisvalid(x1,y1,z1))
					{
							String isCellinDataset="N";
							if(i==0 && j==0 && k==0)
								isCellinDataset="Y";
							
							String str=x1+","+y1+","+z1+","+isCellinDataset;
							returnList.add(str);
					}
					// the Y/N encoding helps us to determine if a given lat,long,day is present in the Datasets.
					// this way we can filter out the entries that don't exist in our dataset.
						
				}
			}
		}
		
		return returnList;
	}
	
	//getting cell identifier according to output coordinate system
	public static String getCellIdentifier(String latitude,String longitude)
	{
		double lat=Double.parseDouble(latitude);
		double lon=Double.parseDouble(longitude);
		String id_x="";
		String id_y="";
	
		if(lat<latStart || lat>latEnd)
			return "";
		else if(lon <longStart || lon > longEnd)
			return "";
		else
		{
			int x= (int)Math.floor((lat-0)/cellSize);
			int y= (int)Math.floor((lon-0)/cellSize);
			return Integer.toString(x)+","+Integer.toString(y);
		}
	}
	//start the hot spot analysis.
	public static void startAnalysis()
	{
		final String header=sc.textFile(inputPath).first();
		//removing the column header from the inputs
		JavaRDD<String> allLines=sc.textFile(inputPath).filter(new Function<String, Boolean>() {
			
			public Boolean call(String arg0) throws Exception {
				if(arg0.equals(header))
					return false;
				else
					return true;
			}
		});
		//flatMapToPair function helps assign count ,1 for each cell
		JavaPairRDD<String,Integer> lines=allLines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {

			public Iterator<Tuple2<String, Integer>> call(String str) throws Exception {
				// TODO Auto-generated method stub
				List<Tuple2<String, Integer>> returnList=new ArrayList<Tuple2<String,Integer>>();
				String[] values=str.split(",");
				String pickUpDateTime=values[1];
				String cellIndentifier= getCellIdentifier(values[6], values[5]);
				//cellIndentifier(String latitude,String longitude)
				
				if(cellIndentifier.equals(""))
				{
					//do not add it
				}
				else
				{
					String[] date= pickUpDateTime.split(" ");
					String[] day=date[0].split("-");
					int day2=Integer.parseInt(day[2]);
					String newRDDLine=cellIndentifier+","+Integer.toString(day2-1);
					Tuple2<String, Integer> tuple= new Tuple2<String, Integer>(newRDDLine,1);
					returnList.add(tuple);
				}
				return returnList.iterator();
			}
		});
		// we can add up all tuples with the same KEY (date,cell_x,cell_y), this will give us the Attribute value of a particular cell
		JavaPairRDD<String,Integer>	attributeRDD=lines.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			public Integer call(Integer c1, Integer c2) throws Exception {
				return c1+c2;
			}
		});
	    JavaPairRDD<String,Double> squaredRDD=attributeRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Integer>,String,Double>() {

			public Iterator<Tuple2<String,Double>> call(Tuple2<String, Integer> tuple) throws Exception {
				// TODO Auto-generated method stub
				List<Tuple2<String,Double>> returnList=new ArrayList<Tuple2<String,Double>>();
				Tuple2<String, Double> t=new Tuple2<String, Double>("SquaredSum", (double) (tuple._2*tuple._2));
				returnList.add(t);
				return returnList.iterator();
			}
		}).reduceByKey(new Function2<Double, Double, Double>() {
			
			public Double call(Double arg0, Double arg1) throws Exception {
				return arg0+arg1;
			}
		});
				
		final int totalCells=(int)Math.round(31*((latEnd-latStart)*(longEnd-longStart)/(cellSize*cellSize)));
		final double squaredAttributeSum=squaredRDD.values().first();
		final double AttributeSum=lines.count();
		final double mean=AttributeSum/totalCells;
		final double standardDev= Math.sqrt((squaredAttributeSum)/totalCells - Math.pow(mean, 2));  
		
		JavaPairRDD<String,String> neighborPairRDD=attributeRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Integer>, String, String>() {

			public Iterator<Tuple2<String, String>> call(Tuple2<String, Integer> tuple) throws Exception {
				// TODO Auto-generated method stub
				List<Tuple2<String, String>> returnList= new ArrayList<Tuple2<String,String>>();
				String[] cellValues= tuple._1.split(",");
				List<String> neighborsList= getNeighbors(cellValues[0],cellValues[1],cellValues[2]);
				int numNeighbors=neighborsList.size();
				int attributeValue=tuple._2;
				for(int i=0;i<numNeighbors;i++)
				{
					String values[]=neighborsList.get(i).split(",");
					String cellinDataset=values[3];
					String x_y_z=values[0]+","+values[1]+","+values[2];
					returnList.add(new Tuple2<String, String>(x_y_z,attributeValue+","+"1"+","+cellinDataset ));
				}
				return returnList.iterator();
			}
		});
		JavaPairRDD<String,String> neighborReducedRDD=neighborPairRDD.reduceByKey(new Function2<String, String, String>() {

			public String call(String s1, String s2) throws Exception {
				// TODO Auto-generated method stub
				String[] values1=s1.split(",");
				String[] values2=s2.split(",");
				String cellinDataset="N";
				if(values1[2].equals("Y") || values2[2].equals("Y"))
					cellinDataset="Y";
				
				int neighborCount=Integer.parseInt(values1[1])+Integer.parseInt(values2[1]);
				int neighborAttributeSum=Integer.parseInt(values1[0])+Integer.parseInt(values2[0]);
				return neighborAttributeSum+","+neighborCount+","+cellinDataset;
			}
		});
		JavaPairRDD<String,String> neighborReducedFilteredRDD=neighborReducedRDD.filter(new Function<Tuple2<String,String>, Boolean>() {
			
			public Boolean call(Tuple2<String, String> tuple) throws Exception {
				String str=tuple._2.split(",")[2];
				return str.contains("Y");
			}
		});
		JavaPairRDD<String,Double> gscoresRDD=neighborReducedFilteredRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,String>, String, Double>() {

			public Iterator<Tuple2<String, Double>> call(Tuple2<String, String> tuple) throws Exception {
				// TODO Auto-generated method stub
				List<Tuple2<String, Double>> returnList=new ArrayList<Tuple2<String,Double>>();
				String values[]=tuple._2.split(",");
				String val=values[0]+","+values[1];
				double getisScore=calculateGetis(val, mean, standardDev, totalCells);
				returnList.add(new Tuple2<String, Double>(getisScore+","+tuple._1,getisScore));
				return  returnList.iterator();
			}
		});
		 JavaPairRDD<String, Double> gscoresSorted =  gscoresRDD.sortByKey(new TupleComp());
	 
		 List<Tuple2<String,Double>> lister=gscoresSorted.take(50);
		 List<String> resultList=new LinkedList<String>();
		 
		 int listSize=lister.size();
		 
		 for(int i=0;i<50;i++)
		 {
			 Tuple2<String,Double> tuple=lister.get(i);
			 String[] values=tuple._1.split(",");
			 String result=Double.parseDouble(values[1])/100+","+Double.parseDouble(values[2])/100+","+(int)Double.parseDouble(values[3])+","+tuple._2;
			 resultList.add(result);
		 }
		 
		 JavaRDD fileRDD=sc.parallelize(resultList);
		 fileRDD=fileRDD.coalesce(1);
		 fileRDD.saveAsTextFile(outputFilePath);
	}
    public static void main( String[] args )
    {
    	SparkConf conf= new SparkConf().setAppName("DDSPhase3").setMaster("local");
    	sc=new JavaSparkContext(conf);
    	sc.setLogLevel("ERROR");
    	inputPath="/home/vraj/Downloads/yellow_tripdata_2015-01.csv";
    	inputPath=args[0];
//    	inputPath="/home/vraj/Desktop/test.csv";
    	outputFilePath="/home/vraj/Desktop/output_test5.csv";
    	outputFilePath=args[1];
		startAnalysis();
    }
}	
