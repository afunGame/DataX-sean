package com.alibaba.datax.plugin.reader.mongodbreader.util;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.HostUtils;
import com.alibaba.datax.plugin.reader.mongodbreader.KeyConstant;
import com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReaderErrorCode;

import com.mongodb.*;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jianying.wcj on 2015/3/17 0017.
 * Modified by mingyan.zc on 2016/6/13.
 */
public class MongoUtil {

    private static final Logger log = LoggerFactory.getLogger(MongoUtil.class);


//    public static MongoClient initMongoClient(Configuration conf) {
//
//        List<Object> addressList = conf.getList(KeyConstant.MONGO_ADDRESS);
//        if(addressList == null || addressList.size() <= 0) {
//            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,"不合法参数");
//        }
//        try {
//            return new MongoClient(parseServerAddress(addressList));
//        } catch (UnknownHostException e) {
//            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_ADDRESS,"不合法的地址");
//        } catch (NumberFormatException e) {
//            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,"不合法参数");
//        } catch (Exception e) {
//            throw DataXException.asDataXException(MongoDBReaderErrorCode.UNEXCEPT_EXCEPTION,"未知异常");
//        }
//    }

    public static MongoClient initCredentialMongoClient(Configuration conf, String userName, String password, String database) {

        System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
        System.out.println("create mongo client");

        //认证指定admin库
        database = "admin";
        List<Object> addressList = conf.getList(KeyConstant.MONGO_ADDRESS);
//        if(!isHostPortPattern(addressList)) {
//            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,"不合法参数");
//        }

        if(addressList == null || addressList.size() < 2) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,"不合法参数");
        }

        //获取两个连接地址 一个本地 一个线上的，如果本地超时了 就使用线上的
        String localConnStr = (String) addressList.get(0);
        String remoteConnStr = (String) addressList.get(1);

        MongoClient mongoClient = null;

        //如果不需要手动跳转服务器
        if (!conf.getBool(KeyConstant.CONNECTION_REDIRECT)){
            return getRemoteMongoClient(remoteConnStr);
        }

        try {
            log.info("**********Trying to connect to local server**********");
            MongoCredential credential = MongoCredential.createCredential(userName, database, password.toCharArray());
//            MongoClientOptions options = MongoClientOptions.builder()
//                    .readPreference(ReadPreference.secondary())
//                    .retryReads(true)
//                    .build();
//            log.info("Connection options is:{}",options);
            mongoClient = new MongoClient(parseServerAddressByString(localConnStr), Arrays.asList(credential));
            MongoDatabase adminDB = mongoClient.getDatabase("admin");

            try {
                // Send a ping to confirm a successful connection
                Bson command = new BsonDocument("ping", new BsonInt64(1));
                adminDB.runCommand(command);
                log.info("**********Connected to local server**********");
            } catch (MongoException me) {
                log.warn("local server timeout will redirect to remote server,镜像库连接超时,即将自动切换到线上数据库");
                //超时切换到线上
                mongoClient = getRemoteMongoClient(remoteConnStr);
            }

            return mongoClient;
        } catch (UnknownHostException e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_ADDRESS,"不合法的地址");
        } catch (NumberFormatException e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,"不合法参数");
        } catch (Exception e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.UNEXCEPT_EXCEPTION,"未知异常");
        }
    }

    private static MongoClient getRemoteMongoClient(String remoteConnStr) {
        MongoClient mongoClient;
        boolean atlas = isAtlas(remoteConnStr);
        if (!atlas){
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,"请输入正确的Mongo Atlas地址");
        }
        MongoClientURI uri = new MongoClientURI(remoteConnStr);
        mongoClient = new  MongoClient(uri);
        MongoDatabase adminDB2 = mongoClient.getDatabase("admin");
        try {
            Bson command = new BsonDocument("ping", new BsonInt64(1));
            adminDB2.runCommand(command);
        } catch (MongoException e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.CONNECTION_EXCEPTION,"Can't connect to mongo server");
        }
        return mongoClient;
    }

    /**
     * 是否是mongo Atlas
     * @param connStr
     * @return
     */
    private static boolean isAtlas(String connStr){
        return connStr.startsWith("mongodb+srv");
    }

    /**
     * 判断地址类型是否符合要求
     * @param addressList
     * @return
     */
    private static boolean isHostPortPattern(List<Object> addressList) {
        for(Object address : addressList) {
            String regex = "(\\S+):([0-9]+)";
            if(!((String)address).matches(regex)) {
                return false;
            }
        }
        return true;
    }
    /**
     * 转换为mongo地址协议
     * @param rawAddressList
     * @return
     */
    private static List<ServerAddress> parseServerAddress(List<Object> rawAddressList) throws UnknownHostException{
        List<ServerAddress> addressList = new ArrayList<ServerAddress>();
        for(Object address : rawAddressList) {
            String[] tempAddress = ((String)address).split(":");
            try {
                ServerAddress sa = new ServerAddress(tempAddress[0],Integer.valueOf(tempAddress[1]));
                addressList.add(sa);
            } catch (Exception e) {
                throw new UnknownHostException();
            }
        }
        return addressList;
    }

    private static List<ServerAddress> parseServerAddressByString(String rawAddress) throws UnknownHostException{
        List<ServerAddress> addressList = new ArrayList<ServerAddress>();
        String[] tempAddress = rawAddress.split(":");
        try {
            ServerAddress sa = new ServerAddress(tempAddress[0],Integer.valueOf(tempAddress[1]));
            addressList.add(sa);
        } catch (Exception e) {
            throw new UnknownHostException();
        }
        return addressList;
    }
}