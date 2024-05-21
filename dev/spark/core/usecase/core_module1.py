import hashlib
import sys
def main(argv):
    print(argv)
    from pyspark.sql.session import SparkSession
    spark=SparkSession.builder.master("local").appName("Rafeeq").getOrCreate()
    izsc=spark.sparkContext
    '''
    customerData = [
        (1, "John", 1000, "Chennai"),
        (2, "Mary", 2000, "Bangalore"),
        (3, "Tom", 1500, "Chennai"),
        (4, "Emma", 3000, "Mumbai"),
        (5, "Peter", 2500, "Chennai")
    ]
    customerRDD = izsc.parallelize(customerData)
    print(customerRDD.collect())
    
    #1. Create a hdfs filerdd from the file in a location /user/hduser/videolog/youtube_videos.tsv
    rdd_hdfs = izsc.textFile("/user/hduser/videolog/youtube_videos.tsv")
    #2. Split the rows using tab ("\t") delimiter
    rdd_split=rdd_hdfs.map(lambda x:x.split('\t'))
    print(rdd_split.take(1))
    #3. Remove the header record by filtering the first column value does not contains "id" into an rdd splitrdd or try using take/first/zipWithIndex function to remove the header
    rdd4=rdd_split.zipWithIndex().filter(lambda x:x[1]>1).map(lambda x:x[0])
    #4. Display only first 10 rows in the screen from splitrdd.
    #print(rdd4.take(10))
    
    #5. Filter only Music category data from splitrdd into an rdd called musicrdd
    #musicrdd=rdd4.map(lambda x:x[9])
    musicrdd=rdd4.filter(lambda x:x[9]=='Music')
    print('musicrdd output is :',musicrdd.take(2))
    #print(spark.createDataFrame(musicrdd).toDF('id','duration','bitrate','bitrate','height','width','framerate','frame-rate','codec','category','url').show(10))
    #6. Filter only duration>100 data from splitrdd into an rdd called longdurrdd
    longdurrdd=rdd4.filter(lambda x:int(x[1])>100)
    print('longdurrdd output is :',longdurrdd.take(2))
    #print(spark.createDataFrame(longdurrdd).toDF('id','duration','bitrate','bitrate','height','width','framerate','frame-rate','codec','category','url').show(10))
    #7. Union musicrdd with longdurrdd then convert to tuple and get only the deduplicated (distinct) records into an rdd music_longdur
    unionrdd=musicrdd.union(longdurrdd)
    print('unionrdd out put is :',unionrdd.take(10))
    #print(spark.createDataFrame(unionrdd).toDF('id','duration','bitrate','bitrate','height','width','framerate','frame-rate','codec','category','url').show(10))
    tuplerdd=unionrdd.map(lambda x:(tuple(x),tuple(x)))
    print('tuplerdd output is :',tuplerdd.take(10))
    music_longdur=tuplerdd.distinct()
    print('music_longdur output is :',music_longdur.take(10))
    print(spark.createDataFrame(music_longdur).show(10))
    
    #8. Select only id, duration, codec and category by re-ordering the fields like id,category,codec,duration into an rdd mapcolsrdd
    print(spark.createDataFrame(rdd4).toDF('id','duration','bitrate','bitrate','height','width','framerate','frame-rate','codec','category','url').show(10))
    mapcolsrdd=rdd4.map(lambda x:(x[0],x[9],x[8],x[1]))
    print('mapcolsrdd output is : ',mapcolsrdd.take(2))
    
    #9. Select only duration column from mapcolsrdd and find max duration by using max function.
    select_rdd=mapcolsrdd.map(lambda x:(x[3]))
    print('duration column output is :',select_rdd.take(2))
    max_rdd=select_rdd.max()
    print('max duration output is :',max_rdd)
    
    #10. Select only codec from mapcolsrdd, convert to upper case and print distinct of it in the screen.
    select_codec_rdd=mapcolsrdd.map(lambda x:(x[2]))
    upper_rdd=select_codec_rdd.map(lambda x: x.upper()).distinct()
    print(upper_rdd.take(2))
    
    #11. Create an rdd called filerdd4part from filerdd created in step1 by increasing the number of partitions to 4
    # (Execute this step anywhere in the code where ever appropriate)
    print(rdd_hdfs.getNumPartitions())
    filerdd4part=rdd_hdfs.repartition(4)
    print(filerdd4part.getNumPartitions())
    
    #12. Persist the filerdd4part data into memory and disk with replica of 2,
    # (Execute this step anywhere in the code where ever appropriate)
    from pyspark.storagelevel import StorageLevel
    #persist_memory_rdd=filerdd4part.persist(StorageLevel.MEMORY_ONLY_2)
    #print(persist_memory_rdd)
    persist_memory_disk_rdd=filerdd4part.persist(StorageLevel.MEMORY_AND_DISK_2)
    print(persist_memory_disk_rdd)
    persist_memory_disk_rdd.unpersist()
    print(persist_memory_disk_rdd)
    
    
    #13. Calculate and print the overall total, max, min duration for Comedy category
    filerdd=izsc.textFile("/user/hduser/videolog/youtube_videos.tsv").map(lambda x:x.split('\t')).filter(lambda x:x[9]=='Comedy')
    print(filerdd.take(10))
    total_dur=filerdd.map(lambda x:int(x[1]))
    print('duration col is:',total_dur.take(2))
    max_dur=total_dur.max()
    print('max duration output is :',max_dur)
    min_dur=total_dur.min()
    print('min duration output is :',min_dur)
    overall_total_dur=total_dur.sum()
    print('over all total output is :',overall_total_dur)
    
    #14. Print the codec wise count and minimum duration not by using min rather try to use reduce function function.
    
    #15. Print the distinct category of videos
    file_rdd=izsc.textFile("/user/hduser/videolog/youtube_videos.tsv").map(lambda x:x.split('\t')).filter(lambda x:x[9]=='Video').distinct()
    print(file_rdd.take(10))
    
    #16. Print only the id, duration, height, category and width sorted by duration.
    sort_rdd=izsc.textFile("/user/hduser/videolog/youtube_videos.tsv").map(lambda x:x.split('\t'))
    hdr_rmv=sort_rdd.zipWithIndex().filter(lambda x:x[1]>1).map(lambda x:x[0])
    sel_col_rdd=hdr_rmv.map(lambda x:(x[0],x[1],x[4],x[9],x[5]))
    print('selected col output is',sel_col_rdd.take(10))
    sort_dur=sel_col_rdd.sortBy(lambda x:x[1])
    print('sort by duration output is :',sort_dur.take(10))
    '''
    #17. Create a python function called masking which should take the string as input and returns the hash value of the input string.

def masking(input_string):
    hash_object=hashlib.sha256()
    hash_object.update(input_string.encode('utf-8'))
    hash_value=hash_object.hexdigest()
    return hash_value

    #input_str=input('Enter any string :\n')
    #print(masking(input_str))

    #18. Call the masking function created in the above step and pass category column and get the hashed value of category.
    #sort_rdd=izsc.textFile("/user/hduser/videolog/youtube_videos.tsv").map(lambda x:x.split('\t'))
    #hdr_rmv=sort_rdd.zipWithIndex().filter(lambda x:x[1]>1).map(lambda x:x[0])
    #sel_col_rdd=hdr_rmv.map(lambda x:(x[9],masking(x[9])))
    #print('selected col output is',sel_col_rdd.take(10))

    #19. Store the step 18 result in a hdfs location in a single file with data delimited as | with the id, duration, height, masking(category) and width columns.
    sort_rdd=izsc.textFile("/user/hduser/videolog/youtube_videos.tsv").map(lambda x:x.split('\t'))
    hdr_rmv=sort_rdd.zipWithIndex().filter(lambda x:x[1]>1).map(lambda x:x[0])
    sel_col_rdd=hdr_rmv.map(lambda x:(x[0],x[1],x[4],masking(x[9]),x[5]))
    print(sel_col_rdd.take(10))
    formatted_rdd=sel_col_rdd.map(lambda x:f"{x[0]}|{x[1]}|{x[2]}|{x[3]}|{x[4]}")
    print(formatted_rdd.take(10))
    formatted_rdd.coalesce(1).saveAsTextFile("/user/hduser/videolog/masking_file/maskoutput")

    #20. Try to implement few performance optimization factors like partitioning, caching and broadcast-ing (where ever applicable)
if __name__=="__main__":
    print("I am running a spark Application")
    if len(sys.argv)>=1:
        main(sys.argv)#gateway (entry point)
    else:
        print("usage python.py file:///path/datafile.csv")
        exit(0)