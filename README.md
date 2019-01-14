# spark2_review

### This collects various Spark projects collected through the years from my work and personal projects, including


   1. DanubeStatesAnalysis, ResolverDetailAnalysis
      A task for Rovi to ensure the java-transform system is compatible with Pri-java-transform system, utilizing
      parsing, filtering & grouping
        
       a) DanubeStatesAnalysis parses transformer logs from non java-transform and java-transform environment into
          Spark Dataset.  In order to compare them side by side easily, I parse them into into a common obeject:
          DanubeStates which also include additional numeric fields jtNo and jtYes of value 0 or 1. Non java-transform
          loge entries will have jtNo of 1 and jtYes of 0 and java-transform log entries will have ice versa. I
          generate grouping report of sum(jtNo) and sum(jtYes) side by side grouped by publish state,
          
       b) ResolverDetailAnalysis joins Resolver log entries from non java-transform and java-transform environment
          It uses filter and join and format extensively. 
          
       c) Notice that flatMap will stripe Option wraper but map won't    
         
   2. OverlayProcessing 
      It covers both Content and Station Overlay processing of Tivo
       
       a) To categorize into resource_types: movie, epsiode, series and other based upon SHOWTYPE etc. Use when
          and other extensively/
          
       b) Use UDF (User defined function) to generate overlay trio numeric value for content/ collection/ station.
          The function behind UDF call exitsing Java class (IdGen) to accomplish this. I have to get sbt compile
          order working to support it.
             
       c) Save key values to Mongodb OverlaysLookup collection of unified database so that I can use it
          for IdAuthResponse overlay simulator.   
       
   3. MovieRatingRealCase - Data Coding Challenge
      I get it from a coding interview. Design and create a Apache Spark job will process a provided daily file 
      and build a dataset calculating average movie ratings as of the latest file received.  Requirement include:
      
      - Processing
        - Only the latest movie rating by a user should be used/counted.
        - Assume that daily files are ~2TB and lookup data is about 1MB.
        - Resultant dataset to be read performant/optimized for querying.
      - Metadata join
        - Metadata can be updated at any time and the resultant dataset needs to only show the latest runtime and title.
        - Metadata may not always have the respective movie listed.
      - Reprocessing
        - Reprocessing of any specific days, i.e. We should be able to reprocess any one day's file with mini
        
      - Features include
        - Take multiple files under recurvive folder into consideration and allow expandable folder structure
        - Have to de-duplicate rating (the same userId, movieId, rating and ts) first. Window functions won't help
          de-duplicate.  It will select both if duplicate records are first in the Window.
        - Use WindowSpec and Window functions to only select the latest rating for the same userId and movieId
        - groupBy agg and left_join
        
          
   4. HousingSpark
      ml-spark-sklearn-tensor https://github.com/threecuptea/ml-spark-sklearn-tensor migrates a Scikit-Learn 
      (plus Panda, matplotlib and Numpy) project based upon California Housing data to Spark written in Jupyter 
      Notebook.   This further refactors, streamline and modularize it into a 
      standalone Spark application.
      
   5. MovieLenALSMongo use Spark-ML ALS use MongoDB collections as both source and sink
   
      I removed MovieLensALS and MovieLensALSCv to spark2_emr 
      
   6. Add WindowFunctions 
      Find two excellent articles introducing SQL Window function
      https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html   
      https://alvinhenrick.com/2017/05/16/apache-spark-analytical-window-functions/
      practice
      
      - How to use window functions: rank, dense_rank, rwo_number, lead, lag, first, last
      - How to list first n ranks in a window frame
      - How to combine window function and row value, 
        ex (the difference between highest revenue in window and revenue)
      - One suprise, sum (running total) will includes multiple rows with the same value.  The running total of the 
        second to the last has include 1250 in the the last row.
      
            |7499 |ALLEN |SALESMAN |7698|20-Feb-81|1600|300 |30    |4450          |
            |7844 |TURNER|SALESMAN |7698|8-Sep-81 |1500|0   |30    |5950          |
            |7521 |WARD  |SALESMAN |7698|22-Feb-81|1250|500 |30    |8450          |
            |7654 |MARTIN|SALESMAN |7698|28-Sep-81|1250|1400|30    |8450          |
            +-----+------+---------+----+---------+----+----+------+--------------+
  
      - How to change frame (row frame and range frame) of WindowSpec to correct window function 'last'.  The default
        window frame is range between unbounded preceding and current row.  It won't go beyond the current row. 
        Therefore, the last alway select iteslf if we keep the default window frame.  Changing the Window frame as the
        followings fix the issue.
        
            wsByDeptOrderBySal.rowsBetween(Window.currentRow, Window.unboundedFollowing) 
          
      - Try to implement id (row  number) with 3 options: 
        a) using sql function monotonically_increasing_id()
        b) using zipWithUniqueId of RDD
        c) using zipWithIndex of RDD
        
        sql function monotonically_increasing_id() will generate incremental ids within a partition. However, ids won't 
        be continuous if more than one partition is involved. It assume each partition has 8589934592 rows.  Ids 
        in the second partition will be 8589934593, 8589934594 and so on.
        
        As its name imply, zipWithUniqueId will generate unique Ids but no guarantee to be continuous incremental.
        
        zipWithIndex always has correct implementation: continuous incremental Ids.  However, it has the most overhead.
        It requires to start a new Spark job.  zipWithIndex is RDD function and requires conversion as the followings:
        
            val rddWithIdx = repartRdd.zipWithIndex().map {
                case (r: Row, idx: Long) => Row.fromSeq((idx+1) +: r.toSeq)
            }
            spark.createDataFrame(rddWithIdx, StructType(StructField("id", LongType, false) +: empDF.schema.fields)).show(false)
        
      
      
   
      
          
         
        


