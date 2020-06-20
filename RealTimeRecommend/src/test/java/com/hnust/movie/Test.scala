/*
package com.hnust.movie

import com.hnust.movie.entity.Constant
import com.mongodb.{BasicDBObject, DBCursor, DBObject}
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoCollection, MongoDB}
import com.mongodb.client.model.Projections
import org.bson.conversions.Bson

/**
  * @Title:
  * @Author: ggh
  * @Date: 2020/6/14 22:05
  */
object Test {


  def main(args: Array[String]): Unit = {
    //获取mongo连接
    val mongoClient = MongoClient("localhost")
    val mongoDB: MongoDB = mongoClient(Constant.MONGODB_NAME)
    val collection1 = mongoDB.getCollection(Constant.MID_2_SIMILAR_SCORE)
//    val collection: MongoCollection = mongoDB(Constant.MID_2_SIMILAR_SCORE)
//"mid"->26325320,
//    val bObject = MongoDBObject("mid2Score.mid"->1291545)

    val basicDBObject = new BasicDBObject()
    basicDBObject.put("mid",26325320l)

    val fileds = new BasicDBObject()
    fileds.put("mid2Score.score",true)

//    val bson: Bson = Projections.include("mid2Score.score")


    val cursor: DBCursor = collection1.find(basicDBObject,fileds)

    println(cursor.next())

//
//    collection.find(basicDBObject)
//
//
//    val cursorType = collection.find(basicDBObject)
//
//    println(cursorType.next())

  }

}
*/
