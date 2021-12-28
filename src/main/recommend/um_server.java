import com.clearspring.analytics.util.Lists;
import com.entity.*;
import com.mongodb.client.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.examples.recommend.*;
import io.grpc.stub.StreamObserver;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import com.redislabs.provider.redis.*;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.sql.types.StructType;
import com.mongodb.spark.MongoSpark;
import org.jblas.DoubleMatrix;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import scala.Function0;
import scala.Function1;
import scala.Some;
import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.Traversable;
import scala.collection.TraversableLike;
import scala.collection.TraversableOnce;
import scala.collection.generic.CanBuildFrom;
import scala.collection.generic.Growable;
import scala.collection.mutable.ArrayBuffer;
import scala.collection.mutable.Builder;
import scala.reflect.ClassTag;

import java.io.IOException;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.lang.Math.sqrt;


public class um_server implements Serializable

{

    private static final Logger logger = Logger.getLogger(um_server.class.getName());
    private MongoDatabase mongoDatabase;
    private Server server;
    List<movie> movieList;
    private List<TopRecommendation> topRecommendations=new ArrayList<>();
    private void start() throws IOException {
//        Read_Csv();
        movieList=new ArrayList<>();
        System.setProperty("hadoop.home.dir","D:\\hadoop-2.7.7");
        SparkSession spark = SparkSession.builder()
                .master("local")
//                .config("spark.storage.blockManagerSlaveTimeoutMs","1000000000")
//                .config("spark.executor.heartbeatInterval","1000000000")
//                .config("spark.network.timeout","1000000000")
//                .config("spark.executor,memory","15G")
//                .config("spark.executor.cores","3")
//                .config("spark.cores.max","21")
//                .config("spark.executor.processTreeMetrics.enabled","false")
//                .config("spark.sql.shuffle.partitions","300")
//                .config("spark.driver.maxResultSize", "20g")
                .appName("MongoSparkConnectorIntro").getOrCreate();


        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

//        Statistics1(spark,jsc);
//        Statistics2(spark,jsc);
//        Statistics3(spark,jsc);
//        offLineRecommender(spark,jsc);
//        recommendList(spark,jsc);/
//        saveRecentforUser(spark);
//        io.grpc.examples.recommend.Rating testRating= io.grpc.examples.recommend.Rating.newBuilder().setUserId(1).setMovieId(301).setRating((float)3.7).build();
//        OnlineRecommend(spark,testRating);
        getRmse(spark,jsc);
        int port = 50051;
        server = ServerBuilder.forPort(port)
                .addService(new RecommendImpl())
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                um_server.this.stop();
                System.err.println("*** server shut down");

            }
        });
    }
    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        final um_server server = new um_server();
        server.start();
        server.blockUntilShutdown();
    }
     class RecommendImpl extends RecommendGrpc.RecommendImplBase{
        @Override
        public void movieRecommend(UserRequest req, StreamObserver<MovieResponse> responseObserver) {
            String result=readRedis(String.valueOf(req.getUserId()));
            String[] movieList=result.split(",");
            findMovie(movieList);
            responseObserver.onNext(getResponse());
            responseObserver.onCompleted();
        }
    }


//获得电影历史总评分数排名
    public void Statistics1(SparkSession spark,JavaSparkContext jsc){

        Dataset<Row> implicitDS = spark.read().option("uri","mongodb://127.0.0.1/local").option("collection","rating").format("com.mongodb.spark.sql")
                .load().toDF();
        implicitDS.printSchema();
        implicitDS.show();

        implicitDS.createOrReplaceTempView("rating");
        Dataset<Row> result=spark.sql("SELECT movieId,count(movieId) AS count FROM rating GROUP BY movieId ORDER BY count desc");
        MongoSpark.write(result).option("uri","mongodb://127.0.0.1/local").option("collection", "rating_more_history").mode("overwrite").save();
    }

    //获得电影最近获得评分个数排名
    public void Statistics2(SparkSession spark,JavaSparkContext jsc){
        Dataset<Row> implicitDS = spark.read().option("uri","mongodb://127.0.0.1/local").option("collection","rating").format("com.mongodb.spark.sql")
                .load().toDF();
        implicitDS.printSchema();
        implicitDS.show();
        SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyyMM");
        spark.udf().register("changeDate",
                (UDF1<Long,Integer>)(time)->{return Integer.valueOf(simpleDateFormat.format(time*((long)1000)).toString());}, DataTypes.IntegerType);
        implicitDS.createOrReplaceTempView("rating");
        Dataset<Row> centenarians = spark.sql("SELECT  movieId ,rating,changeDate(timestamp) AS yearmonth FROM rating");
        centenarians.createOrReplaceTempView("rating_recently");
        Dataset<Row> result=spark.sql("SELECT movieId,count(movieId) AS count,yearmonth FROM rating_recently GROUP BY yearmonth,movieId ORDER BY yearmonth desc,count desc");
        MongoSpark.write(result).option("uri","mongodb://127.0.0.1/local").option("collection", "rating_more_recently").mode("overwrite").save();


    }

    //获得每种类型前10电影
    public void Statistics3(SparkSession spark,JavaSparkContext jsc) {

        List<String> genres = Arrays.asList("Action", "Adventure", "Animation", "Children's", "Comedy"
                , "Crime", "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "Musical"
                , "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western","no genres listed");


        Dataset<Row> movieDS = spark.read().option("uri","mongodb://127.0.0.1/local").option("collection","movie").format("com.mongodb.spark.sql")
                .load().toDF();

        Dataset<Row> rating = spark.read().option("uri","mongodb://127.0.0.1/local").option("collection","rating").format("com.mongodb.spark.sql")
                .load().toDF();;
        rating.printSchema();
        rating.show();
        rating.createOrReplaceTempView("rating");
        Dataset<Row> average = spark.sql("SELECT movieId,avg(rating) AS avg FROM rating GROUP BY movieId");
        MongoSpark.write(average).option("uri","mongodb://127.0.0.1/local").option("collection", "rating_average").mode("overwrite").format("com.mongodb.spark.sql").save();

        movieDS.printSchema();
        movieDS.show();

        Dataset<Row> ratingDS=spark.read().option("uri","mongodb://127.0.0.1/local").option("collection","rating_average").format("com.mongodb.spark.sql")
                .load().toDF();;
        ratingDS.printSchema();
        ratingDS.show();;
        Dataset<Row> MovieWithRating = movieDS.join(ratingDS, "movieId");

        MovieWithRating.printSchema();
        MovieWithRating.show();
        MovieWithRating.createOrReplaceTempView("MovieWithRating");

        JavaRDD<String> genresRDD = jsc.parallelize(genres);
        JavaRDD<GenresRecommendation> Top10MoviesGenres=genresRDD.cartesian(MovieWithRating.javaRDD()).filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> stringRowTuple2) throws Exception {
                if(stringRowTuple2._2.getAs("genres").toString().toLowerCase().contains(stringRowTuple2._1.toLowerCase()))
                    return true;
                else
                    return false;
            }
        }).mapToPair(new PairFunction<Tuple2<String, Row>, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Tuple2<String, Row> stringRowTuple2) throws Exception {
                ArrayList<StructField> fields = new ArrayList<StructField>();
                StructField field = null;
                field = DataTypes.createStructField("movieId", DataTypes.IntegerType, true);
                fields.add(field);
                field = DataTypes.createStructField("avg", DataTypes.DoubleType, true);
                fields.add(field);
                StructType schema = DataTypes.createStructType(fields);
                Object[] array=new Object[]{stringRowTuple2._2.getAs("movieId"),stringRowTuple2._2.getAs("avg")};
                Row row=new GenericRowWithSchema(array,schema);
                return new Tuple2<String,Row>(stringRowTuple2._1,row);
            }
        }).groupByKey().map(new Function<Tuple2<String, Iterable<Row>>, GenresRecommendation>() {
            @Override
            public GenresRecommendation call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {
                List<Recommendation> list= Lists.newArrayList();
                for(Row row:stringIterableTuple2._2){
                    Recommendation recommendation=new Recommendation(row.getAs("movieId"),row.getAs("avg"));
                    list.add(recommendation);
                }
                Collections.sort(list);
                List<Recommendation> sublist=list.subList(0,9);
                Recommendation[] recommendations=new Recommendation[10];
                for(int i=0;i<10;i++){
                    recommendations[i]=list.get(i);
                }

                return new GenresRecommendation(stringIterableTuple2._1, recommendations);
            }
        });
        List<GenresRecommendation> topList=Top10MoviesGenres.collect();
        for(GenresRecommendation genresRecommendation:topList){
            for(int i=0;i<genresRecommendation.getRecs().length;i++){
                topRecommendations.add(new TopRecommendation(genresRecommendation.getGenres(),
                    genresRecommendation.getRecs()[i].getMovieId(),genresRecommendation.getRecs()[i].getAvg()));
                System.out.println(topRecommendations.get(i).getGenres()+","+topRecommendations.get(i).getAvg()+","+topRecommendations.get(i).getGenres());
            }
        }
        Dataset<Row> top10=spark.createDataFrame(topRecommendations,TopRecommendation.class);

      MongoSpark.write(top10).option("uri","mongodb://127.0.0.1/local").option("collection", "top10").format("com.mongodb.spark.sql").mode("overwrite").save();

    }

    public static void Read_Csv(){
        SparkConf conf=new SparkConf().setMaster("local").setAppName("Read Csv");
        SparkSession sparkSession=SparkSession.builder().config(conf)
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/local")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/local")

                .getOrCreate();
        ArrayList<StructField> ratingFields = new ArrayList<StructField>();
        StructField ratingField = null;
        ratingField = DataTypes.createStructField("userId", DataTypes.IntegerType, true);
        ratingFields.add(ratingField);
        ratingField = DataTypes.createStructField("movieId", DataTypes.IntegerType, true);
        ratingFields.add(ratingField);
        ratingField = DataTypes.createStructField("rating", DataTypes.DoubleType, true);
        ratingFields.add(ratingField);
        ratingField = DataTypes.createStructField("timestamp", DataTypes.LongType, true);
        ratingFields.add(ratingField);
        StructType ratingStruct=DataTypes.createStructType(ratingFields);
        Dataset<Row> movieDF=sparkSession.read().format("csv")
                .option("header","true")
                .option("inferSchema","true")
                .load("src/main/resources/movies.csv");
        MongoSpark.write(movieDF).option("collection","movie").mode("overwrite").format("com.mongodb.spark.sql").save();
        Dataset<Row> ratingDF=sparkSession.read().format("csv")
                .option("header","true")
                .option("inferSchema","true")
                .schema(ratingStruct)
                .load("src/main/resources/ratings.csv");
        MongoSpark.write(ratingDF).option("collection","rating").mode("overwrite").format("com.mongodb.spark.sql").save();
        ArrayList<StructField> tagFields = new ArrayList<StructField>();
        StructField tagField = null;
        tagField = DataTypes.createStructField("userId", DataTypes.IntegerType, true);
        tagFields.add(tagField);
        tagField = DataTypes.createStructField("movieId", DataTypes.IntegerType, true);
        tagFields.add(tagField);
        tagField = DataTypes.createStructField("tag", DataTypes.StringType, true);
        tagFields.add(tagField);
        tagField = DataTypes.createStructField("timestamp", DataTypes.LongType, true);
        tagFields.add(tagField);
        StructType tagStruct=DataTypes.createStructType(tagFields);
        Dataset<Row> tagDF=sparkSession.read().format("csv")
                .option("header","true")
                .option("inferSchema","true")
                .schema(tagStruct)
                .load("src/main/resources/tags.csv");
        MongoSpark.write(tagDF).option("collection","tag").mode("overwrite").format("com.mongodb.spark.sql").save();
        Dataset<Row> linkDF=sparkSession.read().format("csv")
                .option("header","true")
                .option("inferSchema","true")
                .load("src/main/resources/links.csv");
        MongoSpark.write(linkDF).option("collection","links").mode("overwrite").format("com.mongodb.spark.sql").save();
        Dataset<Row> gsDF=sparkSession.read().format("csv")
                .option("header","true")
                .option("inferSchema","true")
                .load("src/main/resources/genome-scores.csv");
        MongoSpark.write(gsDF).option("collection","genome-score").mode("overwrite").format("com.mongodb.spark.sql").save();
        Dataset<Row> gtDF=sparkSession.read().format("csv")
                .option("header","true")
                .option("inferSchema","true")
                .load("src/main/resources/genome-tags.csv");
        MongoSpark.write(gtDF).option("collection","genome-tag").mode("overwrite").format("com.mongodb.spark.sql").save();
        sparkSession.close();
    }

    /**
     * 推荐电影寻找
     *
     */
    public void findMovie(String[] movies) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro").getOrCreate();
        Dataset<Row> implicitDS = spark.read().option("uri", "mongodb://127.0.0.1/local").option("collection", "movie").format("com.mongodb.spark.sql")
                .load().toDF();

        implicitDS.createOrReplaceTempView("movies");
        Movie[] mv = new Movie[5];
        Dataset<Row> result = spark.sql("SELECT movieId,title,genres FROM movies WHERE movieId=" + movies[0] + " OR movieId=" + movies[1] +
                " OR movieId=" + movies[2] + " OR movieId=" + movies[3] + " OR movieId=" + movies[4]);
        List<Row>rows=result.collectAsList();
//        JavaRDD<movie> ml = result.javaRDD().map(new Function<Row, movie>() {
//            @Override
//            public movie call(Row row) throws Exception {
//                String genre = row.getAs("genres");
//                String[] genres = genre.split("|");
//                return new movie(row.getAs("movieId"), row.getAs("title"), genres);
//            }
//        });
        for(Row row:rows){
            String genre = row.getAs("genres");
            String[] genres = genre.split("\\|");
            movieList.add(new movie(row.getAs("movieId"),row.getAs("title"),genres));
        }
    }

    /**
     * 构建电影返回包
     *
     */
    public MovieResponse getResponse() {
        MovieResponse.Builder resbuilder = MovieResponse.newBuilder();
        for (movie mm : movieList) {
            Movie.Builder builder = Movie.newBuilder().setMovieId(mm.getMovieId()).setTitle(mm.getTitle());
            String[] ss = mm.getGenres();
            for (String str : ss) {
                builder.addGenres(str);
            }
            Movie resMovie = builder.build();
            resbuilder.addMovie(resMovie);
        }
        return resbuilder.build();
    }

    /**
     * 分数据并进行预测评分
     */
    public void offLineRecommender(SparkSession spark,JavaSparkContext jsc) {
        JavaRDD<Rating> ratingRDD = spark.read().option("uri", "mongodb://127.0.0.1/local").option("collection", "trainData").format("com.mongodb.spark.sql")
                .load().javaRDD().map(new Function<Row, Rating>() {
                    @Override
                    public Rating call(Row v1) throws Exception {
                        return new Rating(v1.getAs("userId"), v1.getAs("movieId"), v1.getAs("rating"));
                    }
                });
//        JavaRDD<Rating> testData = spark.read().option("uri", "mongodb://127.0.0.1/local").option("collection", "testData").format("com.mongodb.spark.sql")
//                .load().javaRDD().map(new Function<Row, Rating>() {
//                    @Override
//                    public Rating call(Row v1) throws Exception {
//                        return new Rating(v1.getAs("userId"), v1.getAs("movieId"), v1.getAs("rating"));
//                    }
//                });
        double[] splitArray={0.8,0.2};
        JavaRDD<Rating>[] rdds=ratingRDD.randomSplit(splitArray);

        MatrixFactorizationModel model = new ALS().setCheckpointInterval(2).setRank(10).setIterations(15).setLambda(0.01).setImplicitPrefs(false)
                .run(rdds[0]);
        JavaPairRDD<Integer, Integer> userProducts = rdds[1].mapToPair(new PairFunction<Rating, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Rating rating) throws Exception {
                return new Tuple2<>(rating.user(), rating.product());
            }
        });

        JavaRDD<Rating> result = model.predict(userProducts);
        JavaPairRDD<Tuple2<Integer, Integer>, Double> observe = rdds[1].mapToPair(new PairFunction<Rating, Tuple2<Integer, Integer>, Double>() {
            @Override
            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating rating) throws Exception {
                return new Tuple2<>(new Tuple2<>(rating.user(), rating.product()), rating.rating());
            }
        });
        JavaPairRDD<Tuple2<Integer, Integer>, Double> predict = result.mapToPair(new PairFunction<Rating, Tuple2<Integer, Integer>, Double>() {
            @Override
            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating rating) throws Exception {
                return new Tuple2<>(new Tuple2<>(rating.user(), rating.product()), rating.rating());
            }
        });

        JavaRDD<score>testResult = observe.join(predict).map(new Function<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>, score>() {
            @Override
            public score call(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> tuple2Tuple2Tuple2) throws Exception {
                return new score(tuple2Tuple2Tuple2._1._1,tuple2Tuple2Tuple2._1._2,tuple2Tuple2Tuple2._2._1,tuple2Tuple2Tuple2._2._2);
            }
        }).sortBy(new Function<score, Integer>() {
            @Override
            public Integer call(score score) throws Exception {
                return score.getUserId();
            }
        },true,10);
        Dataset<Row> df = spark.createDataFrame(testResult, score.class);
        MongoSpark.write(df).option("uri","mongodb://127.0.0.1/local").option("collection", "TestResult").format("com.mongodb.spark.sql").mode("overwrite").save();
    }

    /**
     *   求两个向量的余弦相似度
     */
    public Double consinSim(DoubleMatrix d1,DoubleMatrix d2){
        return d1.dot(d2)/(d1.norm2()*d2.norm2());
    }

    /**
     * 保存用户最近评分
     */
    public void saveRecentforUser(SparkSession spark){
        JavaPairRDD<Integer, Iterable<RatingWithTime>> ratingWithTimeJavaRDD=spark.read().option("uri", "mongodb://127.0.0.1/local").option("collection", "rating").format("com.mongodb.spark.sql")
                .load().javaRDD().mapToPair(new PairFunction<Row, Integer,RatingWithTime>() {
                    @Override
                    public Tuple2<Integer, RatingWithTime> call(Row row) throws Exception {
                        return new Tuple2<>(row.getAs("userId"),new RatingWithTime(row.getAs("movieId"),
                                row.getAs("rating"),row.getAs("timestamp")));
                    }
                }).groupByKey();
        ratingWithTimeJavaRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<Integer, Iterable<RatingWithTime>>>>() {
            @Override
            public void call(Iterator<Tuple2<Integer, Iterable<RatingWithTime>>> tuple2Iterator) throws Exception {
                Jedis jd = new Jedis("127.0.0.1", 6379);
                jd.auth("cjn11234");
                jd.select(0);
                Pipeline pipe = jd.pipelined();
                while (tuple2Iterator.hasNext()) {
                    Tuple2<Integer, Iterable<RatingWithTime>> tuple2 = tuple2Iterator.next();
                    String redisKey = tuple2._1.toString();
                    Iterator<RatingWithTime> i = tuple2._2.iterator();
                    List<RatingWithTime> ilist = new ArrayList<>();
                    String redisValue = "";
                    while (i.hasNext()) {
                        ilist.add(i.next());
                    }
                    Collections.sort(ilist);
                    for (int j = 0; j < 5; j++) {
                        redisValue += ilist.get(j).getMovieId() + ":" + ilist.get(j).getRating();
                        if (j!=4) {
                            redisValue += ",";
                        }
                    }
                    pipe.set(redisKey, redisValue);
                    pipe.expire(redisKey, 3600 * 24);
                }
            }
        });
    }

    /**
     * 实时推荐
     * @param spark
     * @param newRating
     */
    public void OnlineRecommend(SparkSession spark, io.grpc.examples.recommend.Rating newRating){
        String[][] recentArray=getUserRecently(newRating.getUserId());
        List<Recommendation> SimList=getTopSimMovies(spark, newRating.getUserId(), newRating.getMovieId());
        Tuple2<Integer,Double>[] res=computeMovieScores(recentArray, SimList,spark);
        for(int i=0;i<res.length;i++){
            System.out.println(res[i]._1+" "+res[i]._2);
        }


    }
    /**
     * 从Redis里取出用户最近评分
     */
    public String[][] getUserRecently(Integer uid){
        Jedis jd = new Jedis("127.0.0.1", 6379);
        jd.auth("cjn11234");
        jd.select(0);
        String uidstr=uid.toString();
        jd.watch(uidstr);
        String movies=jd.get(uidstr);
        String[] marray=movies.split(",");
        String[][] mlist=new String[marray.length][];
        for(int i=0;i<marray.length;i++){
            mlist[i]=marray[i].split(":");
        }
        return mlist;
    }

    /**
     * 根据电影相似度计算电影推荐评分
     */
    public Tuple2<Integer,Double>[] computeMovieScores(String[][] recentArray,List<Recommendation> SimList,SparkSession spark) {

        scala.collection.mutable.ArrayBuffer<Tuple2<Integer,Double>> scores=new scala.collection.mutable.ArrayBuffer<>();
        HashMap<Integer,Integer> increMap=new HashMap<>();
        HashMap<Integer,Integer> decreMap=new HashMap<>();

        for (Recommendation candidateMovie : SimList) {
            for (int i = 0; i < recentArray.length; i++) {
                Double simScore =getSimScore(candidateMovie.getMovieId(),Integer.valueOf(recentArray[i][0]),spark);
                if(simScore>0.7){
                    scores.$plus$eq(new Tuple2<Integer,Double>(candidateMovie.getMovieId(),(Double.valueOf(recentArray[i][1])*simScore)));
                    if(Double.valueOf(recentArray[i][1])>3.0){
                        increMap.put(candidateMovie.getMovieId(),increMap.getOrDefault(candidateMovie.getMovieId(),0)+1);
                    }
                    else{
                        decreMap.put(candidateMovie.getMovieId(),increMap.getOrDefault(candidateMovie.getMovieId(),0)+1);
                    }

                }
            }
        }
        Tuple2<Integer,Double>[] finalres=new Tuple2 [scores.length()];
        scores.groupBy(Tuple2::_1)
                .mapValues(new Function1<Traversable<Tuple2<Integer, Double>>, Double>() {
                    @Override
                    public Double apply(Traversable<Tuple2<Integer, Double>> v1) {
                        scala.collection.Iterator<Tuple2<Integer, Double>> iterator = v1.toIterator();
                        Double sum = 0.0;
                        Integer mid;
                        Tuple2<Integer, Double> tuple = iterator.next();
                        sum += tuple._2;
                        mid = tuple._1;
                        Double length = (double) iterator.length();
                        while (iterator.hasNext()) {
                            tuple = iterator.next();
                            sum += tuple._2;
                        }
                        Double result = sum / length + log(increMap.getOrDefault(tuple._1, 1)) - log(decreMap.getOrDefault(tuple._1, 1));
                        return result;
                    }
                }).copyToArray(finalres);
        return finalres;



    }
    /**
     * 对数函数
     */
    public Double log(Integer m){
        return log(m)/log(10);
    }
    public Double getSimScore(Integer mid1,Integer mid2,SparkSession spark) {
        Double score=0.0;
        Dataset<Row> movieDF=spark.read().option("uri", "mongodb://127.0.0.1/local").option("collection", "MovieRecs").format("com.mongodb.spark.sql")
                .load().toDF();
        movieDF.createOrReplaceTempView("movies");
        Dataset<Row> sqlResult=spark.sql("SELECT similarity FROM movies WHERE movieId="+mid1.toString()+" AND filmId="+mid2.toString());
        if(!sqlResult.isEmpty()){
            List<Row> reslist=sqlResult.collectAsList();
            for(Row r:reslist) {
                score = r.getAs("similarity");
            }
        }
        return score;
    }

    /**
     * 获取最相似电影列表
     */
    public List<Recommendation> getTopSimMovies(SparkSession spark,Integer uid,Integer mid){
        Dataset<Row> movieDF=spark.read().option("uri", "mongodb://127.0.0.1/local").option("collection", "MovieRecs").format("com.mongodb.spark.sql")
                .load().toDF();
        movieDF.createOrReplaceTempView("movies");
        Dataset<Row> simMovieDF=spark.sql("SELECT movieId,filmId,similarity FROM movies WHERE movieId="+mid.toString());
        List<Row> simMovieList=simMovieDF.collectAsList();
        List<Recommendation> simMovie=new ArrayList<>();
        for(Row r:simMovieList){
            simMovie.add(new Recommendation(r.getAs("filmId"),r.getAs("similarity")));
        }

        List<Integer> exist=spark.read().option("uri", "mongodb://127.0.0.1/local").option("collection", "rating").format("com.mongodb.spark.sql")
                .load().toJavaRDD().filter(new Function<Row, Boolean>() {
                    @Override
                    public Boolean call(Row row) throws Exception {
                        return row.getAs("userId").equals(uid);
                    }
                }).map(new Function<Row, Integer>() {
                    @Override
                    public Integer call(Row row) throws Exception {
                        return row.getAs("movieId");
                    }
                }).take(5);

        List<Recommendation> finList=simMovie.stream().filter(new Predicate<Recommendation>() {
            @Override
            public boolean test(Recommendation recommendation) {
                return !exist.contains(recommendation.getMovieId());
            }
        }).sorted().collect(Collectors.toList());
        return finList;
    }

    /**
     *
     * 获取离线推荐列表并存到redis中
     *
     */
    public void recommendList(SparkSession spark,JavaSparkContext jsc){
//        JavaRDD<Rating> ratingRDD = spark.read().option("uri", "mongodb://127.0.0.1/local").option("collection", "rating").format("com.mongodb.spark.sql")
//                .load().javaRDD().map(new Function<Row, Rating>() {
//                    @Override
//                    public Rating call(Row v1) throws Exception {
//                        return new Rating(v1.getAs("userId"), v1.getAs("movieId"), v1.getAs("rating"));
//                    }
//                });
//        MatrixFactorizationModel model=new ALS().setCheckpointInterval(2).setRank(25).setIterations(15).setLambda(0.01).setImplicitPrefs(false)
//                .run(ratingRDD);                  //得到用户特征矩阵
       /**
//        JavaRDD<Tuple2<Integer,DoubleMatrix>> movieFeatures=model.productFeatures().toJavaRDD().map(new Function<Tuple2<Object,double[]>, Tuple2<Integer,DoubleMatrix>>() {
//            @Override
//            public Tuple2<Integer, DoubleMatrix> call(Tuple2<Object, double[]> v1) {
//                Integer mid=(Integer)v1._1;
//                return new Tuple2<>(mid,new DoubleMatrix(v1._2));
//            }
//        });
//        JavaRDD<MovieRecs> movieRecs=movieFeatures.cartesian(movieFeatures).filter(new Function<Tuple2<Tuple2<Integer, DoubleMatrix>, Tuple2<Integer, DoubleMatrix>>, Boolean>() {
//            @Override
//            public Boolean call(Tuple2<Tuple2<Integer, DoubleMatrix>, Tuple2<Integer, DoubleMatrix>> tuple2Tuple2Tuple2) throws Exception {
//                return !tuple2Tuple2Tuple2._1._1.equals(tuple2Tuple2Tuple2._2._1);
//            }
//        }).mapToPair(new PairFunction<Tuple2<Tuple2<Integer, DoubleMatrix>, Tuple2<Integer, DoubleMatrix>>, Integer, Tuple2<Integer,Double>>() {
//            @Override
//            public Tuple2<Integer, Tuple2<Integer,Double>> call(Tuple2<Tuple2<Integer, DoubleMatrix>, Tuple2<Integer, DoubleMatrix>> tuple2Tuple2Tuple2) throws Exception {
//                    double simScore=consinSim(tuple2Tuple2Tuple2._1._2,tuple2Tuple2Tuple2._2._2);
//                    return new Tuple2<>(tuple2Tuple2Tuple2._1._1,new Tuple2<>(tuple2Tuple2Tuple2._2._1,simScore));
//            }
//        }).filter(new Function<Tuple2<Integer, Tuple2<Integer, Double>>, Boolean>() {
//            @Override
//            public Boolean call(Tuple2<Integer, Tuple2<Integer, Double>> integerTuple2Tuple2) throws Exception {
//                return integerTuple2Tuple2._2._2>0.6;
//            }
//        }).map(new Function<Tuple2<Integer, Tuple2<Integer, Double>>, MovieRecs>() {
//            @Override
//            public MovieRecs call(Tuple2<Integer, Tuple2<Integer, Double>> integerTuple2Tuple2) throws Exception {
//                return new MovieRecs(integerTuple2Tuple2._1,integerTuple2Tuple2._2._1,integerTuple2Tuple2._2._2);
//            }
//        });
//        Dataset<Row> df=spark.createDataFrame(movieRecs,MovieRecs.class);
//        MongoSpark.write(df).option("uri","mongodb://127.0.0.1/local").option("collection", "MovieRecs").format("com.mongodb.spark.sql").mode("overwrite").save();
        电影（特征）相似矩阵计算**/
        //
//        RDD<Tuple2<Object,Rating[]>> rdd=model.recommendProductsForUsers(5); //取5个相似度最高电影作为用户推荐结果
//        JavaRDD<rating> rjr=rdd.toJavaRDD().flatMap(new FlatMapFunction<Tuple2<Object, Rating[]>, rating>() {
//            @Override
//            public Iterator<rating> call(Tuple2<Object, Rating[]> objectTuple2) throws Exception {
//                List<Rating> rlist=new ArrayList<>(Arrays.asList(objectTuple2._2));
//                int user=(Integer)objectTuple2._1;
//                return rlist.stream().map(x->new rating(user, x.product(), x.rating())).collect(Collectors.toList()).iterator();
//            }
//        }).sortBy(new Function<rating, Integer>() {
//            @Override
//            public Integer call(rating rating) throws Exception {
//                return rating.getUserId();
//            }
//        },true,10);
//        JavaPairRDD<Integer,Iterable<Integer>> rateRDD=rjr.mapToPair(new PairFunction<rating, Integer, Integer>() {
//                    @Override
//                    public Tuple2<Integer, Integer> call(rating rate) throws Exception {
//                        return new Tuple2<>(rate.getUserId(),rate.getMovieId());
//                    }
//                }).groupByKey(); //实时运算


        JavaPairRDD<Integer,Iterable<Integer>> rateRDD=spark.read().option("uri", "mongodb://127.0.0.1/local").option("collection", "userRecs").format("com.mongodb.spark.sql")
                .load().javaRDD().mapToPair(new PairFunction<Row, Integer, Integer>() {
                    @Override
                    public Tuple2<Integer, Integer> call(Row row) throws Exception {
                        return new Tuple2<>(row.getAs("userId"),row.getAs("movieId"));
                    }
                }).groupByKey();  //测试直接调用

        rateRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<Integer, Iterable<Integer>>>>() {
            @Override
            public void call(Iterator<Tuple2<Integer, Iterable<Integer>>> tuple2Iterator) throws Exception {
                Jedis jd = new Jedis("127.0.0.1", 6379);
                jd.auth("cjn11234");
                jd.select(1);
                Pipeline pipe = jd.pipelined();
                while (tuple2Iterator.hasNext()) {
                    Tuple2<Integer, Iterable<Integer>> tuple2 = tuple2Iterator.next();
                    String redisKey = tuple2._1.toString();
                    Iterator<Integer> i = tuple2._2.iterator();
                    String redisValue = "";
                    while (i.hasNext()) {
                        redisValue = redisValue + i.next().toString();
                        if (i.hasNext()) {
                            redisValue += ",";
                        }
                    }
                    pipe.set(redisKey, redisValue);
                    pipe.expire(redisKey, 3600 * 24);
                }
            }
        });
//        MongoSpark.write(df).option("uri","mongodb://127.0.0.1/local").option("collection", "userRecs").format("com.mongodb.spark.sql").mode("overwrite").save();
    }

    /**
     *
     * 根据离线预测评分结果计算误差
     *
     */
    public void getRmse(SparkSession spark,JavaSparkContext jsc){

        JavaRDD<Double>dvalue= spark.read().option("uri", "mongodb://127.0.0.1/local").option("collection", "TestResult").format("com.mongodb.spark.sql")
                .load().javaRDD().map(new Function<Row, Double>() {
                    @Override
                    public Double call(Row v1) throws Exception {
                        Double b=v1.getAs("observe");
                        Double c=v1.getAs("predict");
                        Double a= b-c;
                        return a*a;
                    }
                });
        List<Double> dlist=dvalue.collect();
        Double result=0.0;
        for(Double d:dlist){
            result+=d;
        }
        result=result/dlist.size();
        System.out.println(sqrt(result));
        System.out.println(sqrt(result));
        System.out.println(sqrt(result));

    }

    /**
     *
     * 从redis里得到离线电影推荐列表
     *
     */
    public String readRedis(String userId){
        Jedis jd = new Jedis("127.0.0.1", 6379);
        jd.auth("cjn11234");
        jd.select(1);
        jd.watch(userId);
        String movies=jd.get(userId);
        String[] movieList=movies.split(",");
        for(int i=0;i<movieList.length;i++) {
            System.out.println(movieList[i]);
        }
        return movies;
    }

}
