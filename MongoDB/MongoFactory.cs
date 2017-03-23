using MongoDB.Driver;
using MongoDB.Entity;
using MongoDB.Util;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoDB
{
    public interface IMongoFactory
    {
        IMongoDatabase CreateDatabase(string dbName);

    }

    public class MongoFactory : IMongoFactory
    {

        private string DB_NAME = Config.ConfigAccess<MongoConfig>.GetConfig().DefaultDbName;

        private MongoClient Client { get; set; }

        public MongoFactory(string dbName = "")
        {
            if (!String.IsNullOrWhiteSpace(dbName))
                DB_NAME = dbName;
        }

        public IMongoDatabase CreateDatabase(string dbName)
        {
            if (!string.IsNullOrEmpty(dbName))
                DB_NAME = dbName;

            MongoConfig config = Config.ConfigAccess<MongoConfig>.GetConfig();

            if (config == null || config.ConnectionList == null || config.ConnectionList.Count == 0)
            {
                throw new Exception("MongoConfig为空");
            }

            MongoConnection connection = config.ConnectionList.FindLast(x => x.DBName == DB_NAME);
            if (connection == null)
            {
                throw new Exception("connection为空");
            }
            
            MongoClientSettings mongoSetting = new MongoClientSettings();
            //设置连接超时时间
            mongoSetting.ConnectTimeout = new TimeSpan(config.ConnectionTimeout * TimeSpan.TicksPerSecond);
            var CONNECT_USER_ID = connection.UserName;
            var CONNECT_PWD = string.Empty;
            if (!string.IsNullOrWhiteSpace(connection.PassWord))
            {
                DESHelper des = new DESHelper();
                // connection.PassWord DES 解密
                CONNECT_PWD = des.Decrypt(connection.PassWord);
            }
            //账号密码
            if (CONNECT_USER_ID.Length > 0 && CONNECT_PWD.Length > 0)
            {
                mongoSetting.Credentials = new List<MongoCredential>()
                {
                    MongoCredential.CreateMongoCRCredential(DB_NAME,CONNECT_USER_ID, CONNECT_PWD)
                };
            }
            var lstMongoConnHost = connection.Host.Split(new char[] { ',' });
            var lstMongoConnPort = connection.Port.Split(new char[] { ',' });
            var lstService = lstMongoConnHost.Select(
                (t, i) => new MongoServerAddress(t, lstMongoConnPort.Length > i ? int.Parse(lstMongoConnPort[i])
                    : int.Parse(lstMongoConnPort[0]))).ToList();
            mongoSetting.Servers = lstService;
            //读写分离
            mongoSetting.ReadPreference = new ReadPreference(ReadPreferenceMode.Secondary);
            //设置最大连接数
            mongoSetting.MaxConnectionPoolSize = 50;

            //创建Mongo的客户端
            Client = new MongoClient(mongoSetting);

            //得到服务器端并且生成数据库实例 
            return Client.GetDatabase(DB_NAME);
        }
    }



}
