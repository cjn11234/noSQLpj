import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;
import java.util.List;

public class MongoDBUtil {
    public static MongoDatabase getConnect(){
        //连接到 mongodb 服务
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");

        //连接到数据库
        MongoDatabase mongoDatabase = mongoClient.getDatabase("local");

        //返回连接数据库对象
        return mongoDatabase;
    }

}
