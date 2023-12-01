package com.alibaba.datax.plugin.reader.mongodbreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.util.CollectionSplitUtil;
import com.alibaba.datax.plugin.reader.mongodbreader.util.MongoUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.*;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 * Modified by mingyan.zc on 2016/6/13.
 * Modified by mingyan.zc on 2017/7/5.
 */
public class MongoDBReader extends Reader {

    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        @Override
        public List<Configuration> split(int adviceNumber) {
            return CollectionSplitUtil.doSplit(originalConfig,adviceNumber,mongoClient);
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.userName = originalConfig.getString(KeyConstant.MONGO_USER_NAME, originalConfig.getString(KeyConstant.MONGO_USERNAME));
            this.password = originalConfig.getString(KeyConstant.MONGO_USER_PASSWORD, originalConfig.getString(KeyConstant.MONGO_PASSWORD));
            String database =  originalConfig.getString(KeyConstant.MONGO_DB_NAME, originalConfig.getString(KeyConstant.MONGO_DATABASE));
            String authDb =  originalConfig.getString(KeyConstant.MONGO_AUTHDB, database);
            //只走这一个认证方式
            this.mongoClient = MongoUtil.initCredentialMongoClient(originalConfig,userName,password,authDb);
//            if(!Strings.isNullOrEmpty(this.userName) && !Strings.isNullOrEmpty(this.password)) {
//                this.mongoClient = MongoUtil.initCredentialMongoClient(originalConfig,userName,password,authDb);
//            } else {
//                this.mongoClient = MongoUtil.initMongoClient(originalConfig);
//            }
        }

        @Override
        public void destroy() {

        }
    }


    public static class Task extends Reader.Task {

        //如果源数据的时间小于1970年 那么修复为2023年1月1日
        private static final Long FIX_EPOCH_TIMESTAMP = 1672531200L;

        //一个伟大的人诞生了
        private static final Long START_TIME = 868435688L;

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration readerSliceConfig;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        private Integer batchSize = null;

        private String authDb = null;
        private String database = null;
        private String collection = null;

        private String query = null;

        private String updateField = null;

        private JSONArray mongodbColumnMeta = null;
        private Object lowerBound = null;
        private Object upperBound = null;
        private boolean isObjectId = true;

        //only for read sink database
        private String chPrefix = null;
        private String chUrl = null;
        private String chUser = null;
        private String chPwd = null;

        private int get_db_app_id(String str){
            String[] split = str.split(this.chPrefix);
            return Integer.parseInt(split[1]);
        }

        private Date getLastTimeFromClickhouse() throws SQLException {
            String url = this.chUrl;
//            Properties properties = new Properties();
//            properties.put("username",this.chUser);
//            properties.put("password",this.chPwd);

            try {
                Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                throw new RuntimeException("ClickHouse JDBC driver not found");
            }

            int  dbAppId = get_db_app_id(this.database);

            Connection conn = DriverManager.getConnection(url, this.chUser,this.chPwd);
//            String sql = "SELECT `oid` FROM ? WHERE db_app_id = ? ORDER BY `oid` DESC LIMIT 1";
            //SELECT created_at,oid FROM log_user_transactions_all WHERE db_app_id = 30101 ORDER BY created_at DESC  LIMIT 1
            String sql = String.format("SELECT %s FROM %s WHERE db_app_id = %d ORDER BY %s DESC LIMIT 1",this.updateField,this.collection ,dbAppId,this.updateField);

            LOG.info(">>>>>>>{}<<<<<<",sql);
            PreparedStatement statement = conn.prepareStatement(sql);
//            statement.setString(1,"log_user_transactions");
//            statement.setInt(2,dbAppId);
            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()){
                java.sql.Date date = resultSet.getDate(1);
                return new Date(date.getTime());
            }
            return null;
        }

        @Override
        public void startRead(RecordSender recordSender) {



            if(lowerBound== null || upperBound == null ||
                mongoClient == null || database == null ||
                collection == null  || mongodbColumnMeta == null) {
                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                    MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
            }
            MongoDatabase db = mongoClient.getDatabase(database);
            MongoCollection<Document> col = db.getCollection(this.collection);

            MongoCursor<Document> dbCursor = null;

            //根据查询条件判断是否需要Filter
            if (this.query == null){
                Date latest_date = null;
                try {
                    latest_date = this.getLastTimeFromClickhouse();
                } catch (SQLException e) {
                    e.printStackTrace();
                    LOG.error("Can't connect to clickhouse !!! {}",e.getMessage());
                    System.exit(1);
                }



                if (latest_date == null){
                    LOG.warn("Not found data from sink db");
                    //设置一个初始值
                    Instant instant = Instant.ofEpochSecond(START_TIME).atZone(ZoneId.of("UTC")).toInstant();
                    Date start_time = Date.from(instant);

                    //当前时间需要晚一分钟
                    Instant utc_now = Instant.now().atZone(ZoneId.of("UTC")).toInstant().minus(1L, ChronoUnit.MINUTES);
                    Date end_time = Date.from(utc_now);

                    Document query = new Document(this.updateField, new Document("$gt", start_time).append("$lte", end_time));

                    LOG.info(">>>>Init Query:[{}]",query.toJson());


                    //第一次全量更新
                    dbCursor = col.find(query).batchSize(batchSize).cursor();
                }else {

                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    format.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));
                    String utcTimeStr = format.format(latest_date);

                    LOG.info("Found data from sink db,latest {} is {}",this.updateField,utcTimeStr);

//                    Document query = new Document(this.updateField, new Document("$gt", latest_date).append("$lte", end_time));
                    Document query = new Document(this.updateField, new Document("$gt", latest_date));

//                    dbCursor = col.find(new Document("_id",new Document("$gt",new ObjectId(latest_objectId) ))).sort(new Document("_id", 1)).limit(batchSize).cursor();

                    LOG.info(">>>>Current Query:[{}]",query.toJson());

                    //这里一定需要使用Limit 而不是batchSize
                    dbCursor = col.find(query).sort(new Document(this.updateField,"1")).limit(batchSize).cursor();
                }
            }else {
                Document filter = new Document();
                if (lowerBound.equals("min")) {
                    if (!upperBound.equals("max")) {
                        filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$lt", isObjectId ? new ObjectId(upperBound.toString()) : upperBound));
                    }
                } else if (upperBound.equals("max")) {
                    filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$gt", isObjectId ? new ObjectId(lowerBound.toString()) : lowerBound));
                } else {
                    filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$gte", isObjectId ? new ObjectId(lowerBound.toString()) : lowerBound).append("$lt", isObjectId ? new ObjectId(upperBound.toString()) : upperBound));
                }

                //已经拆分出object id了 时间查询条件已经不需要了
                //如果只有一个线程 前面没有走拆分 还是需要加上过滤条件
                if (filter.isEmpty()){
                    Document queryFilter = Document.parse(query);
                    //这里是单线程 只过滤查询条件
                    dbCursor = col.find(queryFilter).batchSize(batchSize).iterator();
                    LOG.info(">>>query filter:{}",queryFilter.toString());
                }else {
                    //这里是过滤拆分的条件
                    dbCursor = col.find(filter).batchSize(batchSize).iterator();
                    LOG.info(">>>query filter:{}",filter.toString());
                }
            }



            while (dbCursor.hasNext()) {
                Document item = dbCursor.next();
                Record record = recordSender.createRecord();
                Iterator columnItera = mongodbColumnMeta.iterator();
                while (columnItera.hasNext()) {
                    JSONObject column = (JSONObject)columnItera.next();
                    Object tempCol = item.get(column.getString(KeyConstant.COLUMN_NAME));

                    if (column.getString(KeyConstant.COLUMN_TYPE).equals("json")){
//                        Document json = (Document) tempCol;
                        if (tempCol != null){
//                            tempCol = json.toJson();
                            tempCol = com.mongodb.util.JSON.serialize(tempCol);
                        }
                    }

                    if (tempCol == null) {
                        if (KeyConstant.isDocumentType(column.getString(KeyConstant.COLUMN_TYPE))) {
                            String[] name = column.getString(KeyConstant.COLUMN_NAME).split("\\.");
                            if (name.length > 1) {
                                Object obj;
                                Document nestedDocument = item;
                                for (String str : name) {
                                    obj = nestedDocument.get(str);
                                    if (obj instanceof Document) {
                                        nestedDocument = (Document) obj;
                                    }
                                }

                                if (null != nestedDocument) {
                                    Document doc = nestedDocument;
                                    tempCol = doc.get(name[name.length - 1]);
                                }
                            }
                        }
                    }

                    if (tempCol == null) {
                        //根据类型和默认值来匹配
                        String default_val = column.getString(KeyConstant.COLUMN_DEFAULT);
                        if ( default_val == null){
                            //没有设置默认值->设置为0
                            record.addColumn(new StringColumn("0"));
                        }else {
                            if ("date".equals(column.getString(KeyConstant.COLUMN_TYPE))){
                                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
                                LocalDateTime localDateTime = LocalDateTime.parse(default_val, formatter);
                                Date date = Date.from(localDateTime.atZone(ZoneId.of("UTC")).toInstant());
                                record.addColumn(new DateColumn(date));
                            }else {
                                //有默认值就设置为默认值
                                record.addColumn(new StringColumn(default_val));
                            }
                        }

                        //continue; 这个不能直接continue会导致record到目的端错位
//                        record.addColumn(new StringColumn("0"));
                    }else if (tempCol instanceof Double) {
                        //TODO deal with Double.isNaN()
                        record.addColumn(new DoubleColumn((Double) tempCol));
                    } else if (tempCol instanceof Boolean) {
                        record.addColumn(new BoolColumn((Boolean) tempCol));
                    } else if (tempCol instanceof Date) {
                        Date old = (Date) tempCol;
                        //1970年1月1日ThursdayAM12点00分
                        if (old.getTime() <= 0){
                            Instant instant = Instant.ofEpochSecond(FIX_EPOCH_TIMESTAMP).atZone(ZoneId.of("UTC")).toInstant();
                            Date date = Date.from(instant);
                            record.addColumn(new DateColumn(date));
                        }else {
                            record.addColumn(new DateColumn(old));
                        }
                    } else if (tempCol instanceof Integer) {
                        record.addColumn(new LongColumn((Integer) tempCol));
                    }else if (tempCol instanceof Long) {
                        Long timestamp = (Long) tempCol;
//                        System.out.println("+++++++++++++LONG+++++++++++++++");
//                        System.out.println(timestamp);
                        if ("date".equals(column.getString(KeyConstant.COLUMN_TYPE))){
                            Instant instant = Instant.ofEpochSecond(timestamp).atZone(ZoneId.of("UTC")).toInstant();
                            Date date = Date.from(instant);
                            record.addColumn(new DateColumn(date));
                        }else {
                            record.addColumn(new LongColumn((Long) tempCol));
                        }
                    } else {
                        if(KeyConstant.isArrayType(column.getString(KeyConstant.COLUMN_TYPE))) {
                            String splitter = column.getString(KeyConstant.COLUMN_SPLITTER);
                            if(Strings.isNullOrEmpty(splitter)) {
                                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                                    MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
                            } else {
                                ArrayList array = (ArrayList)tempCol;
                                String tempArrayStr = Joiner.on(splitter).join(array);
                                record.addColumn(new StringColumn(tempArrayStr));
                            }
                        } else {
                            record.addColumn(new StringColumn(tempCol.toString()));
                        }
                    }
                }
                //update cache
                //System.out.println("UPDATE CACHE");
                recordSender.sendToWriter(record);
            }
        }

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.userName = readerSliceConfig.getString(KeyConstant.MONGO_USER_NAME, readerSliceConfig.getString(KeyConstant.MONGO_USERNAME));
            this.password = readerSliceConfig.getString(KeyConstant.MONGO_USER_PASSWORD, readerSliceConfig.getString(KeyConstant.MONGO_PASSWORD));
            this.database = readerSliceConfig.getString(KeyConstant.MONGO_DB_NAME, readerSliceConfig.getString(KeyConstant.MONGO_DATABASE));



            this.authDb = readerSliceConfig.getString(KeyConstant.MONGO_AUTHDB, this.database);
//            if(!Strings.isNullOrEmpty(userName) && !Strings.isNullOrEmpty(password)) {
//                mongoClient = MongoUtil.initCredentialMongoClient(readerSliceConfig,userName,password,authDb);
//            } else {
//                mongoClient = MongoUtil.initMongoClient(readerSliceConfig);
//            }
            mongoClient = MongoUtil.initCredentialMongoClient(readerSliceConfig,userName,password,authDb);

            this.collection = readerSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
            this.query = readerSliceConfig.getString(KeyConstant.MONGO_QUERY);
            this.mongodbColumnMeta = JSON.parseArray(readerSliceConfig.getString(KeyConstant.MONGO_COLUMN));
            this.lowerBound = readerSliceConfig.get(KeyConstant.LOWER_BOUND);
            this.upperBound = readerSliceConfig.get(KeyConstant.UPPER_BOUND);
            this.isObjectId = readerSliceConfig.getBool(KeyConstant.IS_OBJECTID);
            //批量读取数据大小
            this.batchSize = (Integer) readerSliceConfig.get(KeyConstant.BATCH_SIZE);
            this.chPrefix = readerSliceConfig.getString("chPrefix");
            this.chUrl = readerSliceConfig.getString("chUrl");
            this.chUser = readerSliceConfig.getString("chUser");
            this.chPwd = readerSliceConfig.getString("chPwd");
            this.updateField = readerSliceConfig.getString("update_field");
        }

        @Override
        public void destroy() {

        }

    }
}
