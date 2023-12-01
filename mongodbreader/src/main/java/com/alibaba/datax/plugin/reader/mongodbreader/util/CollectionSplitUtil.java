package com.alibaba.datax.plugin.reader.mongodbreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.KeyConstant;
import com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReaderErrorCode;
import com.google.common.base.Strings;
import com.mongodb.MongoClient;
import com.mongodb.MongoQueryException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 * Modified by mingyan.zc on 2016/6/13.
 * Modified by mingyan.zc on 2017/7/5.
 */
public class CollectionSplitUtil extends Reader {

    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;

        private String rowQuery = null;

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            rowQuery = readerSliceConfig.getString(KeyConstant.MONGO_QUERY);
        }

        @Override
        public void destroy() {

        }

        @Override
        public void startRead(RecordSender recordSender) {

        }
    }

    private static final Logger LOG = LoggerFactory
            .getLogger(CollectionSplitUtil.class);

    public static List<Configuration> doSplit(
            Configuration originalSliceConfig, int adviceNumber, MongoClient mongoClient) {

        List<Configuration> confList = new ArrayList<Configuration>();

        String dbName = originalSliceConfig.getString(KeyConstant.MONGO_DB_NAME, originalSliceConfig.getString(KeyConstant.MONGO_DATABASE));

        String collName = originalSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);

        if(Strings.isNullOrEmpty(dbName) || Strings.isNullOrEmpty(collName) || mongoClient == null) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
        }

        boolean isObjectId = isPrimaryIdObjectId(mongoClient, dbName, collName);

        List<Range> rangeList = doSplitCollection(adviceNumber, mongoClient, dbName, collName, isObjectId,originalSliceConfig);
        for(Range range : rangeList) {
            Configuration conf = originalSliceConfig.clone();
            conf.set(KeyConstant.LOWER_BOUND, range.lowerBound);
            conf.set(KeyConstant.UPPER_BOUND, range.upperBound);
            conf.set(KeyConstant.IS_OBJECTID, isObjectId);
            confList.add(conf);
        }
        return confList;
    }


    private static boolean isPrimaryIdObjectId(MongoClient mongoClient, String dbName, String collName) {

        System.err.println("****************************************");
        System.err.println("dbName:"+dbName);
        System.err.println("collName:"+collName);
        System.err.println("****************************************");
        MongoDatabase database = mongoClient.getDatabase(dbName);
        MongoCollection<Document> col = database.getCollection(collName);

        try {
            Document doc = col.find().limit(1).first();
            if (doc == null){
                LOG.error("Can't found your table,please check your config");
                System.exit(1);
            }
            Object id = doc.get(KeyConstant.MONGO_PRIMARY_ID);
            return id instanceof ObjectId;
        } catch (MongoQueryException e) {
            LOG.warn("mongo query error,will stop current process with exit code 0");
            System.exit(0);
        }
        return true;
    }

    // split the collection into multiple chunks, each chunk specifies a range
    private static List<Range> doSplitCollection(int adviceNumber, MongoClient mongoClient,
                                          String dbName, String collName, boolean isObjectId,
                                          Configuration originalSliceConfig) {

        MongoDatabase database = mongoClient.getDatabase(dbName);
        List<Range> rangeList = new ArrayList<Range>();
        if (adviceNumber == 1) {
            Range range = new Range();
            range.lowerBound = "min";
            range.upperBound = "max";
            return Collections.singletonList(range);
        }
        //TODO 这里拆分不太合理
        String query = originalSliceConfig.getString(KeyConstant.MONGO_QUERY);
        LOG.info("==============={}===============",query);

        MongoCollection<Document> collection = null;
        try {
            collection = database.getCollection(collName);
        } catch (Exception e) {
            LOG.error("Do split error,{}",e.getMessage());
        }

        //拆分失败
        if (collection == null){
            LOG.error("Do split error will exit with code 0");
            System.exit(0);
        }


        Document queryFilter = Document.parse(query);
        //获取查询区间的数据条数
        long docCount = collection.countDocuments(queryFilter);

        LOG.info(">>>>>>>>>{} record will be sync<<<<<<<<<<<<<<",docCount);

        if (docCount == 0){
            LOG.warn("Not found new data will exit with code 0");
            System.exit(0);
        }


        //获取首尾的object id
//        ObjectId beginObjectId = Objects.requireNonNull(collection.find(queryFilter).sort(new Document("_id", 1)).limit(1).first()).getObjectId("_id");
//        ObjectId endObjectId = Objects.requireNonNull(collection.find(queryFilter).sort(new Document("_id", -1)).limit(1).first()).getObjectId("_id");
//
//        if (beginObjectId == null || endObjectId == null){
//            //没有新数据
//            LOG.warn("Not found new data will exit with code 0");
//            System.exit(0);
//        }

//        LOG.info(" >= {} < {}",beginObjectId.toHexString(),endObjectId.toHexString());


        int splitPointCount = adviceNumber - 1;
        int chunkDocCount = (int) (docCount / adviceNumber);
        ArrayList<Object> splitPoints = new ArrayList<Object>();


        System.out.println("###########################");
        System.out.println("没有权限手动拆分");

        int skipCount = chunkDocCount;
        MongoCollection<Document> col = database.getCollection(collName);


        for (int i = 0; i < splitPointCount; i++) {
            Document doc = col.find(queryFilter).skip(skipCount).limit(chunkDocCount).first();
            Object id = doc.get(KeyConstant.MONGO_PRIMARY_ID);
            if (isObjectId) {
                ObjectId oid = (ObjectId)id;
                splitPoints.add(oid.toHexString());
            } else {
                splitPoints.add(id);
            }
            skipCount += chunkDocCount;
        }

        Object lastObjectId = "min";
//        Object lastObjectId = beginObjectId.toHexString();
        for (Object splitPoint : splitPoints) {
            Range range = new Range();
            range.lowerBound = lastObjectId;
            lastObjectId = splitPoint;
            range.upperBound = lastObjectId;
            rangeList.add(range);
        }
        Range range = new Range();
        range.lowerBound = lastObjectId;
        range.upperBound = "max";
//        range.upperBound = endObjectId.toHexString();
        rangeList.add(range);

        return rangeList;
    }

}

class Range {
    Object lowerBound;
    Object upperBound;
}
