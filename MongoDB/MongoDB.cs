using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Config;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Util;
using MongoDB.Entity;

namespace MongoDB
{
    /// <summary>
    /// MongoDB的简单配置
    /// </summary>
    public class MongoDB
    {
        ///// <summary>
        ///// 数据库所在主机
        ///// </summary>
        //private readonly string MONGO_CONN_HOST = ConfigurationSettings.AppSettings["hostName"];

        ///// <summary>
        ///// 数据库所在主机的端口
        ///// </summary>
        //private readonly int MONGO_CONN_PORT = int.Parse(ConfigurationSettings.AppSettings["port"]);

        ///// <summary>
        ///// 连接超时设置 秒
        ///// </summary>
        //private readonly int CONNECT_TIME_OUT = int.Parse(ConfigurationSettings.AppSettings["connectionTimeout"]);

        /// <summary>
        /// 数据库的名称
        /// </summary>
        private readonly string DB_NAME = Config.ConfigAccess<MongoConfig>.GetConfig().DefaultDbName;

        public MongoDB(string dbName = "")
        {
            if (!String.IsNullOrWhiteSpace(dbName))
                DB_NAME = dbName;
        }


        private MongoClient Client { get; set; }
        /// <summary>
        /// 得到数据库实例
        /// </summary>
        /// <returns></returns>
        public IMongoDatabase GetDataBase()
        {

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
                mongoSetting.Credentials = new List<MongoCredential>()
                {
                    MongoCredential.CreateMongoCRCredential(DB_NAME,CONNECT_USER_ID, CONNECT_PWD)
                };

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

        public IMongoDatabase GetDataBase(string dbName)
        {
            dbName = string.IsNullOrEmpty(dbName) ? DB_NAME : dbName;
            MongoConfig config = Config.ConfigAccess<MongoConfig>.GetConfig();

            if (config == null || config.ConnectionList == null || config.ConnectionList.Count == 0)
            {
                throw new Exception("MongoConfig为空");
            }

            MongoConnection connection = config.ConnectionList.FindLast(x => x.DBName == dbName);
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
                mongoSetting.Credentials = new List<MongoCredential>()
                {
                    MongoCredential.CreateMongoCRCredential(dbName,CONNECT_USER_ID, CONNECT_PWD)
                };

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
            return Client.GetDatabase(dbName);
        }

        public void DisConnected()
        {
            //            Client.GetServer().Disconnect();
        }
    }
}
