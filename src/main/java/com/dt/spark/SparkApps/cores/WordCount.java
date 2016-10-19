/*
 * Project: alex-spark-streaming
 * 
 * File Created at 2016��8��29��
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
 * @date 2016��8��29�� ����9:04:02
 * @version 
 */
public class WordCount {

    public static void main(String[] args) {

        /**
         * ��1��������Spark�����ö���SparkConf������Spark���������ʱ��������Ϣ��
         * ����˵ͨ��setMaster�����ó���Ҫ���ӵ�Spark��Ⱥ��Master��URL,�������
         * Ϊlocal�������Spark�����ڱ������У��ر��ʺ��ڻ������������ǳ������
         * ֻ��1G���ڴ棩�ĳ�ѧ��       *
         */
        SparkConf conf = new SparkConf().setAppName("Spark WordCount written by java");
//        conf.setMaster("spark://quickstart.cloudera:7077");
        conf.setMaster("yarn-client");
        conf.set("spark.testing.memory", "5147480000");;

        /**
         * ��2��������SparkContext����
         * SparkContext��Spark�������й��ܵ�Ψһ��ڣ������ǲ���Scala��Java��Python��R�ȶ�������һ��SparkContext(��ͬ�����Ծ���������Ʋ�ͬ�������java ��ΪjavaSparkContext)
         * SparkContext�������ã���ʼ��SparkӦ�ó�����������Ҫ�ĺ������������DAGScheduler��TaskScheduler��SchedulerBackend
         * ͬʱ���Ḻ��Spark������Masterע������
         * SparkContext������SparkӦ�ó�������Ϊ������Ҫ��һ������
         */
        JavaSparkContext sc = new JavaSparkContext(conf); //��ײ����scala��sparkcontext

        /**
         * ��3�������ݾ����������Դ��HDFS��HBase��Local FS��DB��S3�ȣ�ͨ��SparkContext������RDD
         * JavaRDD�Ĵ������������ַ�ʽ�������ⲿ��������Դ������HDFS��������Scala���ϡ���������RDD����
         * ���ݻᱻJavaRDD���ֳ�Ϊһϵ�е�Partitions�����䵽ÿ��Partition����������һ��Task�Ĵ�����
         */
        JavaRDD<String> lines = sc.textFile("E:\29bigdataStudy\09.spark\testData.txt");

        /**
         * ��4�����Գ�ʼ��JavaRDD����Transformation����Ĵ�������map��filter�ȸ߽׺����ȵı�̣������о�������ݼ���
         * ��4.1������ÿһ�е��ַ�����ֳɵ����ĵ���
         */
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() { //�����scala����Samת�����Կ���д��һ�д���
            @Override
            public Iterable<String> call(String line) throws Exception {
                // TODO Auto-generated method stub
                return Arrays.asList(line.split(" "));
            }
        });

        /**
         * ��4�����Գ�ʼ��JavaRDD����Transformation����Ĵ�������map��filter�ȸ߽׺����ȵı�̣������о�������ݼ���
         * ��4.2�����ڵ��ʲ�ֵĻ����϶�ÿ������ʵ������Ϊ1��Ҳ����word => (word, 1)
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
         * ��4�����Գ�ʼ��RDD����Transformation����Ĵ�������map��filter�ȸ߽׺����ȵı�̣������о�������ݼ���
         * ��4.3������ÿ������ʵ������Ϊ1����֮��ͳ��ÿ���������ļ��г��ֵ��ܴ���
         */
        JavaPairRDD<String, Integer> wordsCount = pairs
                .reduceByKey(new Function2<Integer, Integer, Integer>() { //����ͬ��Key������Value���ۼƣ�����Local��Reducer����ͬʱReduce��
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
 * 2016��8��29�� alex creat
 */
