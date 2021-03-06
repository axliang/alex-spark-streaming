/*
 * Project: alex-spark-streaming
 * 
 * File Created at 2016年8月29日
 * 
 * Copyright 2016 CMCC Corporation Limited.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * ZYHY Company. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license.
 */
package com.dt.spark.SparkApps.cores;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * @Type WordCount.java
 * @Desc 
 * @author alex
 * @date 2016年8月29日 上午9:04:02
 * @version 
 */
public class WordCount {

    public static void main(String[] args) {

        /**
         * 第1步：创建Spark的配置对象SparkConf，设置Spark程序的运行时的配置信息，
         * 例如说通过setMaster来设置程序要链接的Spark集群的Master的URL,如果设置
         * 为local，则代表Spark程序在本地运行，特别适合于机器配置条件非常差（例如
         * 只有1G的内存）的初学者       *
         */
        SparkConf conf = new SparkConf().setAppName("Spark WordCount written by java");
//        conf.setMaster("spark://quickstart.cloudera:7077");
        conf.setMaster("yarn-client");
        conf.set("spark.testing.memory", "5147480000");;

        /**
         * 第2步：创建SparkContext对象
         * SparkContext是Spark程序所有功能的唯一入口，无论是采用Scala、Java、Python、R等都必须有一个SparkContext(不同的语言具体的类名称不同，如果是java 的为javaSparkContext)
         * SparkContext核心作用：初始化Spark应用程序运行所需要的核心组件，包括DAGScheduler、TaskScheduler、SchedulerBackend
         * 同时还会负责Spark程序往Master注册程序等
         * SparkContext是整个Spark应用程序中最为至关重要的一个对象
         */
        JavaSparkContext sc = new JavaSparkContext(conf); //其底层就是scala的sparkcontext

        /**
         * 第3步：根据具体的数据来源（HDFS、HBase、Local FS、DB、S3等）通过SparkContext来创建RDD
         * JavaRDD的创建基本有三种方式：根据外部的数据来源（例如HDFS）、根据Scala集合、由其它的RDD操作
         * 数据会被JavaRDD划分成为一系列的Partitions，分配到每个Partition的数据属于一个Task的处理范畴
         */
        JavaRDD<String> lines = sc.textFile("E:\29bigdataStudy\09.spark\testData.txt");

        /**
         * 第4步：对初始的JavaRDD进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
         * 第4.1步：讲每一行的字符串拆分成单个的单词
         */
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() { //如果是scala由于Sam转化所以可以写成一行代码
            @Override
            public Iterable<String> call(String line) throws Exception {
                // TODO Auto-generated method stub
                return Arrays.asList(line.split(" "));
            }
        });

        /**
         * 第4步：对初始的JavaRDD进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
         * 第4.2步：在单词拆分的基础上对每个单词实例计数为1，也就是word => (word, 1)
         */
        JavaPairRDD<String, Integer> pairs = words
                .mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        // TODO Auto-generated method stub
                        return new Tuple2<String, Integer>(word, 1);
                    }
                });
        
        /**
         * 第4步：对初始的RDD进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
         * 第4.3步：在每个单词实例计数为1基础之上统计每个单词在文件中出现的总次数
         */
        JavaPairRDD<String, Integer> wordsCount = pairs
                .reduceByKey(new Function2<Integer, Integer, Integer>() { //对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        // TODO Auto-generated method stub
                        return v1 + v2;
                    }
                });

        wordsCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> pairs) throws Exception {
                // TODO Auto-generated method stub
                System.out.println(pairs._1 + " : " + pairs._2);
            }
        });
        sc.close();
    }

}

/**
 * Revision history
 * -------------------------------------------------------------------------
 * 
 * Date Author Note
 * -------------------------------------------------------------------------
 * 2016年8月29日 alex creat
 */
