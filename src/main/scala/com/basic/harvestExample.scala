package com.basic

/**
  * Created by Ravee on 12/19/2016.
  */
object harvestExample {

  //df1 = sqlContext.read.option("mergeSchema", "true").parquet("s3://jdisg.sb.analytics/user/mv66708/07182015-1000_repart_5/")
  //df1.registerTempTable("df1")
  //result_df1 = sqlContext.sql("Select org_id, count(distinct(concat(projection,grid_id))) from df1 WHERE comb_operation='Harvest' Group by org_id")
}
