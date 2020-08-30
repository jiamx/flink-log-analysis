package com.jmx.analysis;

import com.jmx.bean.AccessLogRecord;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;

/**
 *  @Created with IntelliJ IDEA.
 *  @author : jmx
 *  @Date: 2020/8/24
 *  @Time: 22:19
 *  
 */
public class LogAnalysis {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启checkpoint，时间间隔为毫秒
        senv.enableCheckpointing(5000L);
        // 选择状态后端
        // 本地测试
        // senv.setStateBackend(new FsStateBackend("file:///E://checkpoint"));
        // 集群运行
        senv.setStateBackend(new FsStateBackend("hdfs://kms-1:8020/flink-checkpoints"));
        // 重启策略
        senv.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(3, Time.of(2, TimeUnit.SECONDS) ));

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(senv, settings);
        // kafka参数配置
        Properties props = new Properties();
        // kafka broker地址
        props.put("bootstrap.servers", "kms-2:9092,kms-3:9092,kms-4:9092");
        // 消费者组
        props.put("group.id", "log_consumer");
        // kafka 消息的key序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // kafka 消息的value序列化器
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(
                "user_access_logs",
                new SimpleStringSchema(),
                props);

        DataStreamSource<String> logSource = senv.addSource(kafkaConsumer);
        // 获取有效的日志数据
        DataStream<AccessLogRecord> availableAccessLog = LogAnalysis.getAvailableAccessLog(logSource);
        // 获取[clienIP,accessDate,sectionId,articleId]
        DataStream<Tuple4<String, String, Integer, Integer>> fieldFromLog = LogAnalysis.getFieldFromLog(availableAccessLog);
        //从DataStream中创建临时视图,名称为logs
        // 添加一个计算字段:proctime,用于维表JOIN
        tEnv.createTemporaryView("logs",
                fieldFromLog,
                $("clientIP"),
                $("accessDate"),
                $("sectionId"),
                $("articleId"),
                $("proctime").proctime());

        // 统计热门板块
        LogAnalysis.getHotSection(tEnv);
        // 统计热门文章
       LogAnalysis.getHotArticle(tEnv);
        // 统计不同客户端ip对版块和文章的总访问量
       LogAnalysis.getClientAccess(tEnv);

        senv.execute("log-analysisi");

    }

    private static void getClientAccess(StreamTableEnvironment tEnv) {
        // sink表
        // [client_ip,client_access_cnt,statistic_time]
        // [客户端ip,访问次数,统计时间]
        String client_ip_access_ddl = "" +
                "CREATE TABLE client_ip_access (\n" +
                "    client_ip STRING ,\n" +
                "    client_access_cnt BIGINT,\n" +
                "    statistic_time STRING,\n" +
                "    PRIMARY KEY (client_ip) NOT ENFORCED\n" +
                ")WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://kms-4:3306/statistics?useUnicode=true&characterEncoding=utf-8',\n" +
                "    'table-name' = 'client_ip_access', \n" +
                "    'driver' = 'com.mysql.jdbc.Driver',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123qwe'\n" +
                ") ";

        tEnv.executeSql(client_ip_access_ddl);

        String client_ip_access_sql = "" +
                "INSERT INTO client_ip_access\n" +
                "SELECT\n" +
                "    clientIP,\n" +
                "    count(1) AS access_cnt,\n" +
                "    FROM_UNIXTIME(UNIX_TIMESTAMP()) AS statistic_time\n" +
                "FROM\n" +
                "    logs \n" +
                "WHERE\n" +
                "    articleId <> 0 \n" +
                "    OR sectionId <> 0 \n" +
                "GROUP BY\n" +
                "    clientIP "
               ;
        tEnv.executeSql(client_ip_access_sql);

    }

    private static void getHotArticle(StreamTableEnvironment tEnv) {
        // JDBC数据源
        // 文章id及标题对应关系的表,[tid, subject]分别为：文章id和标题
        String pre_forum_post_ddl = "" +
                "CREATE TABLE pre_forum_post (\n" +
                "    tid INT,\n" +
                "    subject STRING,\n" +
                "    PRIMARY KEY (tid) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://kms-4:3306/ultrax',\n" +
                "    'table-name' = 'pre_forum_post', \n" +
                "    'driver' = 'com.mysql.jdbc.Driver',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123qwe'\n" +
                ")";
        // 创建pre_forum_post数据源
        tEnv.executeSql(pre_forum_post_ddl);
        // 创建MySQL的sink表
        // [article_id,subject,article_pv,statistic_time]
        // [文章id,标题名称,访问次数,统计时间]
        String hot_article_ddl = "" +
                "CREATE TABLE hot_article (\n" +
                "    article_id INT,\n" +
                "    subject STRING,\n" +
                "    article_pv BIGINT ,\n" +
                "    statistic_time STRING,\n" +
                "    PRIMARY KEY (article_id) NOT ENFORCED\n" +
                ")WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://kms-4:3306/statistics?useUnicode=true&characterEncoding=utf-8',\n" +
                "    'table-name' = 'hot_article', \n" +
                "    'driver' = 'com.mysql.jdbc.Driver',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123qwe'\n" +
                ")";
        tEnv.executeSql(hot_article_ddl);
        // 向MySQL目标表insert数据
        String hot_article_sql = "" +
                "INSERT INTO hot_article\n" +
                "SELECT \n" +
                "    a.articleId,\n" +
                "    b.subject,\n" +
                "    count(1) as article_pv,\n" +
                "    FROM_UNIXTIME(UNIX_TIMESTAMP()) AS statistic_time\n" +
                "FROM logs a \n" +
                "  JOIN pre_forum_post FOR SYSTEM_TIME AS OF a.proctime as b ON a.articleId = b.tid\n" +
                "WHERE a.articleId <> 0\n" +
                "GROUP BY a.articleId,b.subject\n" +
                "ORDER BY count(1) desc\n" +
                "LIMIT 10";

        tEnv.executeSql(hot_article_sql);

    }

    /**
     * 统计热门板块
     *
     * @param tEnv
     */
    public static void getHotSection(StreamTableEnvironment tEnv) {

        // 板块id及其名称对应关系表,[fid, name]分别为：版块id和板块名称
        String pre_forum_forum_ddl = "" +
                "CREATE TABLE pre_forum_forum (\n" +
                "    fid INT,\n" +
                "    name STRING,\n" +
                "    PRIMARY KEY (fid) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://kms-4:3306/ultrax',\n" +
                "    'table-name' = 'pre_forum_forum', \n" +
                "    'driver' = 'com.mysql.jdbc.Driver',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123qwe',\n" +
                "    'lookup.cache.ttl' = '10',\n" +
                "    'lookup.cache.max-rows' = '1000'" +
                ")";
        // 创建pre_forum_forum数据源
        tEnv.executeSql(pre_forum_forum_ddl);

        // 创建MySQL的sink表
        // [section_id,name,section_pv,statistic_time]
        // [板块id,板块名称,访问次数,统计时间]
        String hot_section_ddl = "" +
                "CREATE TABLE hot_section (\n" +
                "    section_id INT,\n" +
                "    name STRING ,\n" +
                "    section_pv BIGINT,\n" +
                "    statistic_time STRING,\n" +
                "    PRIMARY KEY (section_id) NOT ENFORCED  \n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://kms-4:3306/statistics?useUnicode=true&characterEncoding=utf-8',\n" +
                "    'table-name' = 'hot_section', \n" +
                "    'driver' = 'com.mysql.jdbc.Driver',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123qwe'\n" +
                ")";

        // 创建sink表:hot_section
        tEnv.executeSql(hot_section_ddl);

        //统计热门板块
        // 使用日志流与MySQL的维表数据进行JOIN
        // 从而获取板块名称
        String hot_section_sql = "" +
                "INSERT INTO hot_section\n" +
                "SELECT\n" +
                "    a.sectionId,\n" +
                "    b.name,\n" +
                "    count(1) as section_pv,\n" +
                "    FROM_UNIXTIME(UNIX_TIMESTAMP()) AS statistic_time \n" +
                "FROM\n" +
                "    logs a\n" +
                "    JOIN pre_forum_forum FOR SYSTEM_TIME AS OF a.proctime as b ON a.sectionId = b.fid \n" +
                "WHERE\n" +
                "    a.sectionId <> 0 \n" +
                "GROUP BY a.sectionId, b.name\n" +
                "ORDER BY count(1) desc\n" +
                "LIMIT 10";
        // 执行数据insert
        tEnv.executeSql(hot_section_sql);

    }

    /**
     * 获取[clienIP,accessDate,sectionId,articleId]
     * 分别为客户端ip,访问日期,板块id,文章id
     *
     * @param logRecord
     * @return
     */
    public static DataStream<Tuple4<String, String, Integer, Integer>> getFieldFromLog(DataStream<AccessLogRecord> logRecord) {
        DataStream<Tuple4<String, String, Integer, Integer>> fieldFromLog = logRecord.map(new MapFunction<AccessLogRecord, Tuple4<String, String, Integer, Integer>>() {
            @Override
            public Tuple4<String, String, Integer, Integer> map(AccessLogRecord accessLogRecord) throws Exception {
                LogParse parse = new LogParse();

                String clientIpAddress = accessLogRecord.getClientIpAddress();
                String dateTime = accessLogRecord.getDateTime();
                String request = accessLogRecord.getRequest();
                String formatDate = parse.parseDateField(dateTime);
                Tuple2<String, String> sectionIdAndArticleId = parse.parseSectionIdAndArticleId(request);
                if (formatDate == "" || sectionIdAndArticleId == Tuple2.of("", "")) {

                    return new Tuple4<String, String, Integer, Integer>("0.0.0.0", "0000-00-00 00:00:00", 0, 0);
                }
                Integer sectionId = (sectionIdAndArticleId.f0 == "") ? 0 : Integer.parseInt(sectionIdAndArticleId.f0);
                Integer articleId = (sectionIdAndArticleId.f1 == "") ? 0 : Integer.parseInt(sectionIdAndArticleId.f1);
                return new Tuple4<>(clientIpAddress, formatDate, sectionId, articleId);
            }
        });


        return fieldFromLog;
    }

    /**
     * 筛选可用的日志记录
     *
     * @param accessLog
     * @return
     */
    public static DataStream<AccessLogRecord> getAvailableAccessLog(DataStream<String> accessLog) {
        final LogParse logParse = new LogParse();
        //解析原始日志，将其解析为AccessLogRecord格式
        DataStream<AccessLogRecord> filterDS = accessLog.map(new MapFunction<String, AccessLogRecord>() {
            @Override
            public AccessLogRecord map(String log) throws Exception {
                return logParse.parseRecord(log);
            }
        }).filter(new FilterFunction<AccessLogRecord>() {
            //过滤掉无效日志
            @Override
            public boolean filter(AccessLogRecord accessLogRecord) throws Exception {
                return !(accessLogRecord == null);
            }
        }).filter(new FilterFunction<AccessLogRecord>() {
            //过滤掉状态码非200的记录，即保留请求成功的日志记录
            @Override
            public boolean filter(AccessLogRecord accessLogRecord) throws Exception {
                return !accessLogRecord.getHttpStatusCode().equals("200");
            }
        });
        return filterDS;

    }


}
