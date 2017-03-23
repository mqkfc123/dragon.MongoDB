using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Entity;
using MongoDB.Util;
using MongoDB.Util.Log4net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoDB
{

    public interface IMongoDBHelper
    {
        /// <summary>
        /// 插入数据
        /// </summary>
        /// <typeparam name="T">需要插入数据库的实体类型</typeparam>
        /// <param name="t">需要插入数据库的具体实体</param>
        /// <param name="collectionName">指定插入的集合</param>
        void Insert<T>(T t, string collectionName = "");

        /// <summary>
        /// 批量插入
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list"></param>
        /// <param name="collectionName"></param>
        void Insert<T>(IEnumerable<T> list, string collectionName = "");

        /// <summary>
        /// 查询一个集合中的所有数据
        /// </summary>
        /// <typeparam name="T">该集合数据的所属类型</typeparam>
        /// <param name="collectionName">指定集合的名称</param>
        /// <param name="fields">查询的资料</param>
        /// <returns>返回一个List列表</returns>
        IEnumerable<T> FindAll<T>(string collectionName = "", BsonDocument fd = null);

        /// <summary>
        /// 查询一条记录
        /// </summary>
        /// <typeparam name="T">该数据所属的类型</typeparam>
        /// <param name="query">查询的条件 可以为空</param>
        /// <param name="collectionName">去指定查询的集合</param>
        /// <returns>返回一个实体类型</returns>
        FilterDefinition<BsonDocument> FindOne<T>(FilterDefinition<BsonDocument> query, string collectionName = "");

        /// <summary>
        /// 根据指定条件查询集合中的数据
        /// </summary>
        /// <typeparam name="T">该集合数据的所属类型</typeparam>
        /// <param name="query">指定的查询条件 比如Query.And(Query.EQ("username","admin"),Query.EQ("password":"admin"))</param>
        /// <param name="collectionName">指定的集合的名称</param>
        /// <param name="sortBy"></param>
        /// <param name="fields"></param>
        /// <returns>返回一个List列表</returns>
        List<BsonDocument> Find<T>(FilterDefinition<BsonDocument> query, string collectionName, SortDefinition<BsonDocument> sortBy = null, string[] fields = null);

        /// <summary>
        /// 分页查询 PageIndex和PageSize模式  在页数PageIndex大的情况下 效率明显变低
        /// </summary>
        /// <typeparam name="T">所需查询的数据的实体类型</typeparam>
        /// <param name="query">查询的条件</param>
        /// <param name="pageIndex">当前的页数</param>
        /// <param name="pageSize">当前的尺寸</param>
        /// <param name="sortBy">排序方式</param>
        /// <param name="collectionName">集合名称</param>
        /// <returns>返回List列表</returns>
        List<BsonDocument> Find<T>(FilterDefinition<BsonDocument> query, int pageIndex, int pageSize, BsonDocument sortBy, string collectionName);

        /// <summary>
        /// 更新数据
        /// </summary>
        /// <typeparam name="T">更新的数据 所属的类型</typeparam>
        /// <param name="query">更新数据的查询</param>
        /// <param name="update">需要更新的文档</param>
        /// <param name="collectionName">指定更新集合的名称</param>
        bool Update<T>(FilterDefinition<BsonDocument> query, UpdateDefinition<BsonDocument> update, string collectionName);

        /// <summary>
        /// 移除指定的数据
        /// </summary>
        /// <typeparam name="T">移除的数据类型</typeparam>
        /// <param name="query">移除的数据条件</param>
        /// <param name="collectionName">指定的集合名词</param>
        void Remove<T>(FilterDefinition<BsonDocument> query, string collectionName);

        /// <summary>
        /// 获取集合的存储大小
        /// </summary>
        /// <param name="collectionName">该集合对应的名称</param>
        /// <returns>返回一个long型</returns>
        long GetDataSize<T>(string collectionName);

    }

    public class MongoDBHelper : IMongoDBHelper, IDisposable
    {
        private static readonly ILog _Logger = LogHelper.GetLogger(typeof(MongoDBHelper));

        #region Field
        /// <summary>
        /// 数据库的实例
        /// </summary>
        private IMongoDatabase _db;

        /// <summary>
        /// ObjectId的键
        /// </summary>
        private readonly string OBJECTID_KEY = "_id";
        #endregion

        #region Constructor


        public MongoDBHelper()
        {
            this._db = new global::MongoDB.MongoFactory().CreateDatabase("");
        }

        public MongoDBHelper(IMongoDatabase db)
        {
            this._db = db;
        }

        public MongoDBHelper(string dbName)
        {
            this._db = new global::MongoDB.MongoFactory().CreateDatabase(dbName);
        }

        #endregion

        #region Methods

        public IMongoDatabase GetMongoDatabase()
        {
            return _db;
        }

        public void Insert<T>(T t, string collectionName = "")
        {
            if (string.IsNullOrEmpty(collectionName))
            {
                //集合名称
                collectionName = typeof(T).Name;
            }
            OpenDb();
            var mc = this._db.GetCollection<BsonDocument>(collectionName);
            
            BsonDocument bd = t.ToBsonDocument();
            try
            {
                mc.InsertOne(bd);
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        public void Insert<T>(IEnumerable<T> list, string collectionName = "")
        {
            if (string.IsNullOrEmpty(collectionName))
            {
                //集合名称
                collectionName = typeof(T).Name;
            }

            IMongoCollection<BsonDocument> mc = this._db.GetCollection<BsonDocument>(collectionName);
            //创建一个空间bson集合
            List<BsonDocument> bsonList = new List<BsonDocument>();
            //批量将数据转为bson格式 并且放进bson文档
            list.ToList().ForEach(t => bsonList.Add(t.ToBsonDocument()));
            //批量插入数据
            mc.InsertMany(bsonList);
        }

        public IEnumerable<T> FindAll<T>(string collectionName = "", BsonDocument fd = null)
        {

            if (string.IsNullOrEmpty(collectionName))
                collectionName = typeof(T).Name;

            var mc = this._db.GetCollection<T>(collectionName);

            //直接转化为List返回
            if (fd == null)
            {
                fd = new BsonDocument() { { "_id", 0 } };
            }
            else
            {
                fd.Add("_id", 0);
            }

            //以实体方式取出其数据集合  
            return mc.Find(fd).ToList();
        }

        public FilterDefinition<BsonDocument> FindOne<T>(FilterDefinition<BsonDocument> query, string collectionName = "")
        {
            if (string.IsNullOrEmpty(collectionName))
            {
                collectionName = typeof(T).Name;
            }
            var mc = this._db.GetCollection<BsonDocument>(collectionName);
            query = this.InitQuery(query);
            var t = mc.Find(query).FirstOrDefault();
            return t;
        }

        public List<BsonDocument> Find<T>(FilterDefinition<BsonDocument> query, string collectionName, SortDefinition<BsonDocument> sortBy = null, string[] fields = null)
        {
            if (string.IsNullOrEmpty(collectionName))
                collectionName = typeof(T).Name;

            var mc = this._db.GetCollection<BsonDocument>(collectionName);
            query = this.InitQuery(query);
            if (sortBy == null)
                sortBy = this.InitSortBy(sortBy);

            //MongoCursor<T> mongoCursor = mc.Find(query);
            //var sort = Builders<BsonDocument>.Sort.Ascending("borough").Ascending("address.zipcode");
            var result = mc.Find(query).Sort(sortBy).ToList();
            return result;
        }

        public List<BsonDocument> Find<T>(FilterDefinition<BsonDocument> query, int pageIndex, int pageSize, BsonDocument sortBy, string collectionName)
        {
            if (string.IsNullOrEmpty(collectionName))
                collectionName = typeof(T).Name;

            var mc = this._db.GetCollection<BsonDocument>(collectionName);
            query = this.InitQuery(query);
            var sortBys = this.InitSortBy(sortBy);
            //如页序号为0时初始化为1
            pageIndex = pageIndex == 0 ? 1 : pageIndex;
            //按条件查询 排序 跳数 取数
            var mongoCursor = mc.Find(query).Sort(sortBy).Skip((pageIndex - 1) * pageSize).Limit(pageSize);
            return mongoCursor.ToList<BsonDocument>();
        }

        public bool Update<T>(FilterDefinition<BsonDocument> query, UpdateDefinition<BsonDocument> update, string collectionName)
        {
            if (string.IsNullOrEmpty(collectionName))
                collectionName = typeof(T).Name;

            var mc = this._db.GetCollection<BsonDocument>(collectionName);

            query = this.InitQuery(query);
            //更新数据
            var result = mc.UpdateOne(query, update);
            return result.IsAcknowledged;
        }

        public void Remove<T>(FilterDefinition<BsonDocument> query, string collectionName)
        {
            if (string.IsNullOrEmpty(collectionName))
                collectionName = typeof(T).Name;

            var mc = this._db.GetCollection<BsonDocument>(collectionName);
            query = this.InitQuery(query);
            //根据指定查询移除数据
            var writeConcernResult = mc.DeleteOne(query);
        }

        #endregion

        #region Private Methods

        /// <summary>
        /// 初始化查询记录 主要当该查询条件为空时 会附加一个恒真的查询条件，防止空查询报错
        /// </summary>
        /// <param name="query">查询的条件</param>
        /// <returns></returns>
        private FilterDefinition<BsonDocument> InitQuery(FilterDefinition<BsonDocument> query)
        {
            if (query == null)
            {
                //当查询为空时 附加恒真的条件 类似SQL：1=1的语法
                //new BsonDocument() { { "_id", 0 } }
                query = Builders<BsonDocument>.Filter.Eq(OBJECTID_KEY, 0);
            }
            return query;
        }

        /// <summary>
        /// 初始化排序条件  主要当条件为空时 会默认以ObjectId递增的一个排序
        /// </summary>
        /// <param name="sortBy"></param>
        /// <returns></returns>
        private SortDefinition<BsonDocument> InitSortBy(SortDefinition<BsonDocument> sortBy)
        {
            if (sortBy == null)
            {
                //默认ObjectId 递增
                var sortBys = Builders<BsonDocument>.Sort.Descending(OBJECTID_KEY); // new BsonDocument(OBJECTID_KEY, 1);
            }
            return sortBy;
        }

        private void OpenDb()
        {
            // 激活 Timer
            //mongoTimer.ActivateTimer();  
            var bson = _db.RunCommand<BsonDocument>(new BsonDocument() { { "serverStatus", 1 } });

            var run = _db.RunCommand<BsonDocument>(new BsonDocument() { { "dbStats", 0 }, { "scale", 1 } });
            if (Config.ConfigAccess<MongoConfig>.GetConfig().TestUserPass == 1)
            {
                try
                {
                    // 尝试连接，测试用户名密码是否正确
                    _db.RunCommand<BsonDocument>(new BsonDocument() { { "dbStats", 1 }, { "scale", 1 } });
                }
                catch (Exception ex)
                {
                }
            }
        }

        #endregion

        public long GetDataSize<T>(string collectionName)
        {

            if (string.IsNullOrEmpty(collectionName))
                collectionName = typeof(T).Name;

            var mc = this._db.GetCollection<BsonDocument>(collectionName);
            //return mc.GetTotalStorageSize();
            return 0;
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
       
    }


  
}
