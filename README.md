#mysql_to_hive

sqoop import -m 1 --connect 'jdbc:mysql://localhost:3306/word' --username root -password acadgild --table word --hive-import --create-hive-table --hive-table word.word

hdfs_to_mysql

sqoop export -m 1 --connect 'jdbc:mysql://localhost:3306/word' --username root -P --table word --export-dir /word.txt

mysql_to_hdfs

sqoop import -m 1 --connect 'jdbc:mysql://localhost:3306/word' --username root -P --table word --target-dir /import


sqoop import -m 1 --connect 'jdbc:mysql://localhost:3306/word' --username root -password acadgild --table word --target-dir /import --incremental append --check-column sal --last-value 250


lead lag sal

from(select name,sal, lag(sal) over (partition by (dep) order by sal desc) as new from word)s select s.name,s.new where (s.new-s.sal) > 100;


import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;



public class new_udaf extends UDAF
{

	public class inner_udf implements UDAFEvaluator
	{
		public class column
		{
			int a =0;
			double b =0;
		}
		private column col =null;
		public void init()
		{
			col = new column();
		}
		public boolean iterate(double value) throws HiveException 
		{
			if(col==null)
				throw new HiveException();
		
				col.b = col.b+value;
				col.a=col.a+1;
				return true;
			
			
			
		}
		public column terminatePartial()
		{
			return col;
		}
		public boolean merge(column other)
		{  
			if(other==null)
				return true;
			
				col.b+=other.b;
				col.a+=other.a;
				return true;
			
		}
		 public double terminate()
		{
			return col.b/col.a;
		}
	}
}
