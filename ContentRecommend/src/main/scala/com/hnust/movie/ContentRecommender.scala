package com.hnust.movie

import com.hnust.movie.entity.CaseClasses.{Mid2Score, Mid2SimilarScore, MovieInfo_DB, MovieResc, Movie_MongoDB, SimilarMovieRecommendation}
import com.hnust.movie.entity.Constant
import com.hnust.movie.utils.DateUtil
import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.jblas.DoubleMatrix

/**
  * @Title:基于内容的离线推荐
  * @Author: ggh
  * @Date: 2020/6/4 15:07
  */
object ContentRecommender {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("").setMaster("local[*]")
//      .set("spark.default.parallelism", "10")
//      .set("spark.sql.shuffle.partitions", "20")


    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val sparkContext: SparkContext = spark.sparkContext

    import spark.implicits._

    //读取数据库中电影数据
    val movieInfoDF: DataFrame = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", Constant.URL_DB)
      .option("dbtable", Constant.MOVIE_INFO_DB)
      .option("user", Constant.USER_DB)
      .option("password", Constant.PASSWORD_DB)
      .load()
      .as[MovieInfo_DB]
      .filter(
        info => info.mid!=null &&
          info.movie_name != null &&
          info.directors != null &&
          info.main_actors!=null &&
          info.categories!=null &&
          info.img_urls != null &&
          info.language!=null &&
          info.location != null)
      .map(info => {
            ( info.mid,
              info.movie_name,
              info.directors.replace("|", " "),
              info.main_actors.replace("|", " "),
              info.categories.replace("|", " "),
              info.img_urls,
              info.language.replace("|", " "),
              info.location.replace("|", " "),
              info.rating_num
            )
      })
      .toDF("mid", "movie_name", "directors", "main_actors", "categories", "img_urls", "language", "location","rating_num")
      .cache()

    //转换成map
    val movieInfoMap: collection.Map[Long, Row] = movieInfoDF.rdd.map(info => {
      (info.getAs[Long]("mid"), info)
    }).collectAsMap()


    //对电影信息进行转换，得到：(mid,可以表示电影特征的单词字符串)

    val mid2MultipleWordsDF: DataFrame = movieInfoDF.map(info => {
      (info.getAs[Long]("mid"),
        info.getAs[String]("directors")+","+
        info.getAs[String]("main_actors")+","+
        info.getAs[String]("categories")+","+
        info.getAs[String]("language")+","+
        info.getAs[String]("location")
      )
    }).toDF("mid", "multipleWords")
      .cache()


    //定义分词器，对multipleWords进行分词
    val tokenizer: Tokenizer = new Tokenizer().setInputCol("multipleWords").setOutputCol("words")

    val wordsDF: DataFrame = tokenizer.transform(mid2MultipleWordsDF)

//    wordsDF.show(truncate = false)

    //定义一个hashTF根据，计算每个单词的词频
    val hashingTF: HashingTF = new HashingTF().setInputCol("words").setOutputCol("rowFeatures").setNumFeatures(500)

    //进行转换，得到特征化的数据
    val featurizedDataDF: DataFrame = hashingTF.transform(wordsDF)

//    featurizedDataDF.show(truncate = false)

    //定义一个IDF根据，进行分析，得到模型
    val idf: IDF = new IDF().setInputCol("rowFeatures").setOutputCol("features")
    val iDFModel: IDFModel = idf.fit(featurizedDataDF)

    //转换得到特征列(features)
    val rescaledData: DataFrame = iDFModel.transform(featurizedDataDF)

    rescaledData.show(truncate = false)

    //从rescaledData中提取出mid的特征向量,得到每个电影对应的特征向量：（mid，特征向量）
    val movieFeatures: RDD[(Long, DoubleMatrix)] = rescaledData.map(row => {
      //SparseVector:稀疏向量矩阵
      (row.getAs[Long]("mid"), row.getAs[SparseVector]("features").toArray)
    }).rdd.map {
      //转换成DoubleMatrix
      case (mid, features) => (mid, new DoubleMatrix(features))
    }.cache()

    val date: String = DateUtil.getCurrentTime("yyyy-MM-dd")

    //做笛卡尔积

    //计算每个电影之间的相似度
    val mid2SimilarMovies: RDD[(Long, (Long, Double))] = movieFeatures.cartesian(movieFeatures)
      .filter {
        //过滤掉自己和自己的笛卡尔积
        case (a, b) => a._1 != b._1
      }
      .coalesce(200)
      .map {
        case (m1, m2) => {
          //根据特征向量计算m1和m2电影之间的相似度
          val score: Double = consinSim(m1._2, m2._2)

          //转换成kv形式
          (m1._1, (m2._1, score))
        }
      }.cache()

    val mid2SimilarScoreDF: DataFrame = mid2SimilarMovies.groupByKey().map {
      case (mid, items) => {
        Mid2SimilarScore(date, mid, items.toList.map(x => Mid2Score(x._1, x._2)))
      }
    }.toDF()

    //将电影相似度矩阵保存到mongodb
    mid2SimilarScoreDF.write
      .format("com.mongodb.spark.sql")
      .option("uri",Constant.MONGO_URI)
      .option("collection",Constant.MID_2_SIMILAR_SCORE)
      .mode(SaveMode.Append)
      .save()


    val similarMoviesDF: DataFrame = mid2SimilarMovies.filter(_._2._2 > 0.1)
      .coalesce(200) //filter后进行重分区
      .groupByKey()
      .map {
        case (mid, items) => {
          SimilarMovieRecommendation(date, mid, items.toList.sortWith(_._2 > _._2).take(30).map(info => {
            MovieResc(info._1,
              movieInfoMap.get(info._1).get.getAs[String]("movie_name"),
              0.0,
              movieInfoMap.get(info._1).get.getAs[String]("img_urls"),
              movieInfoMap.get(info._1).get.getAs[Double]("rating_num"),
              movieInfoMap.get(info._1).get.getAs[String]("categories").replace(" ","|"),
              info._2
            )
          }))
        }
      }.toDF()

    //保存到mongodb
    similarMoviesDF.write
        .format("com.mongodb.spark.sql")
        .option("uri",Constant.MONGO_URI)
        .option("collection",Constant.SIMILAR_MOVIE_RECOMMENDATION_MONGODB)
        .mode(SaveMode.Append)
        .save()
    

    spark.stop()

  }

  //计算余弦相似度
  def consinSim(product1: DoubleMatrix, product2: DoubleMatrix):Double = {
    product1.dot(product2) / (product1.norm2() * product2.norm2())
  }

}
