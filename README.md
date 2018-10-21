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
          
   4. HousingSpark
      ml-spark-sklearn-tensor https://github.com/threecuptea/ml-spark-sklearn-tensor migrates a Scikit-Learn 
      (plus Panda, matplotlib and Numpy) project based upon California Housing data to Spark written in Jupyter 
      Notebook.   This further refactors, streamline and modulize it into a standalone Spark application.
      
   5. MovieLensALS groups.
   
      MovieLensALS uses Spark-ML ALS (alternating least squares) algorithm directly.
      
      MovieLensALSCv uses Spark CrossValidator (dividing traing data into n fold and use one fold in term as 
      validation data set and the rest as training data set) that use ALS as its estimator..
      
      MovieLenALSMongo use Spark-ML ALS use MongoDB collections as both source and sink.
      
      - MovieLensALS application is reading both rating and movie data set.  Split rating into training, validation 
        and test data set. Apply ALS process: fit training set, transform validation data set and calculate RMSE. 
        I used param grid of 6 (3 reg-params x 2 latent factors) to find the best parameters and model.
        This means 6 full ALS cycles and each cycle running maxIter 20.  I apply the result to test data set to make 
        sure the best model is not over-fitting or under-fitting.  Then I refit the best parameters to full rating set.       
        I called the result augmented model.  Then I used augmented model to get recommendation for a test userId=6001.
        There are two approaches and both require join with movie partially.  Finally I stored 
        recommendForAllUsers of 25 movies to file in parquet format.  See the details in MovieLensALSEmr. 
      
        
      
      
      
   
      
          
         
        


