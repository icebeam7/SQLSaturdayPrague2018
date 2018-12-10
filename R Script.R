#Connection string to connect to SQL Server. Don't forget to replace MyServer with the name of your SQL Server instance
connStr <- paste("Driver=SQL Server;Server=", "DESKTOP-CF2TNBV", ";Database=" , "tpcxbb_1gb" , ";Trusted_Connection=true;" , sep="" );

#The query that we are using to select data from SQL Server
input_query <- "
SELECT
ss_customer_sk AS customer,
round(CASE WHEN ((orders_count = 0) OR (returns_count IS NULL) OR (orders_count IS NULL) OR ((returns_count / orders_count) IS NULL) ) THEN 0.0 ELSE (cast(returns_count as nchar(10)) / orders_count) END, 7) AS orderRatio,
round(CASE WHEN ((orders_items = 0) OR(returns_items IS NULL) OR (orders_items IS NULL) OR ((returns_items / orders_items) IS NULL) ) THEN 0.0 ELSE (cast(returns_items as nchar(10)) / orders_items) END, 7) AS itemsRatio,
round(CASE WHEN ((orders_money = 0) OR (returns_money IS NULL) OR (orders_money IS NULL) OR ((returns_money / orders_money) IS NULL) ) THEN 0.0 ELSE (cast(returns_money as nchar(10)) / orders_money) END, 7) AS monetaryRatio,
round(CASE WHEN ( returns_count IS NULL                                                                        ) THEN 0.0 ELSE  returns_count                 END, 0) AS frequency
--cast(round(cast(CASE WHEN (returns_count IS NULL) THEN 0.0 ELSE  returns_count END as double)) as integer) AS frequency
FROM
(
  SELECT
  ss_customer_sk,
  -- return order ratio
  COUNT(distinct(ss_ticket_number)) AS orders_count,
  -- return ss_item_sk ratio
  COUNT(ss_item_sk) AS orders_items,
  -- return monetary amount ratio
  SUM( ss_net_paid ) AS orders_money
  FROM store_sales s
  GROUP BY ss_customer_sk
) orders
  LEFT OUTER JOIN
  (
  SELECT
  sr_customer_sk,
  -- return order ratio
  count(distinct(sr_ticket_number)) as returns_count,
  -- return ss_item_sk ratio
  COUNT(sr_item_sk) as returns_items,
  -- return monetary amount ratio
  SUM( sr_return_amt ) AS returns_money
  FROM store_returns
  GROUP BY sr_customer_sk
  ) returned ON ss_customer_sk=sr_customer_sk
  "

  
  
  #Querying SQL Server using the query above and and getting the results back to data frame customer_returns
  #This is also where we define the type for columns we want to select (using colClasses), to make sure that the types are correctly transferred to R
  customer_returns <- RxSqlServerData(sqlQuery=input_query,colClasses=c(customer ="numeric" , orderRatio="numeric" , itemsRatio="numeric" , monetaryRatio="numeric" , frequency="numeric" ),connectionString=connStr);
  
  # Transform the data from an input dataset to an output dataset
  customer_data <- rxDataStep(customer_returns);
  
  #Look at the data we just loaded from SQL Server
  head(customer_data, n = 5);
  
  ### DETERMINE NUMBER OF CLUSTERS

    #Using a plot of the within groups sum of squares, by number of clusters extracted, can help determine the appropriate number of clusters
  #We are looking for a bend in the plot. It is at this "elbow" in the plot that we have the appropriate number of clusters
  wss <- (nrow(customer_data) - 1) * sum(apply(customer_data, 2, var))
  for (i in 2:20)
    wss[i] <- sum(kmeans(customer_data, centers = i)$withinss)
  plot(1:20, wss, type = "b", xlab = "Number of Clusters", ylab = "Within groups sum of squares")
  
  ### PERFORM CLUSTERING
  
  # Output table to hold the customer group mappings. This is a table where the cluster mappings will be saved in the database.
  # This table is generated from R
  return_cluster = RxSqlServerData(table = "return_cluster", connectionString = connStr);
  # Set.seed for random number generator for predictability
  set.seed(10);
  # Generate clusters using rxKmeans and output key / cluster to a table in SQL Server called return_cluster
  clust <- rxKmeans( ~ orderRatio + itemsRatio + monetaryRatio + frequency, customer_returns, numClusters=4
                     , outFile=return_cluster, outColName="cluster" , extraVarsToWrite=c("customer"), overwrite=TRUE);
  
  # Read the customer returns cluster table from the DB
  customer_cluster <- rxDataStep(return_cluster);
  
  ### ANALYZE RESULTS - PLOT CLUSTERS
  
  #Plot the clusters (you need to install R library "cluster". If you don't have that installed, just uncomment the next line of code)
  #install.packages("cluster")
  library("cluster");
  clusplot(customer_data, customer_cluster$cluster, color=TRUE, shade=TRUE, labels=4, lines=0, plotchar = TRUE);
  
  
  ### ANALYZE CLUSTER MEANS
  
  #Look at the clustering details and analyze results
  clust
  
  ## cluster 1 (the largest cluster) seems to be a group of customers that are not active. All values are zero.
  ## cluster 3 seems to be a group that stands out in terms of return behaviour.
  
  
  
  
  
  
  