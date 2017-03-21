using MongoDB.Bson;
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
    /// <summary>
    /// Mongo db的数据库帮助类 
    /// </summary>
    public class MongoDBHelper : IDisposable
    {
        /// <summary>
        /// 数据库的实例
        /// </summary>
        private IMongoDatabase _db;

        /// <summary>
        /// ObjectId的键
        /// </summary>
        private readonly string OBJECTID_KEY = "_id";

        //private static MongoTimer mongoTimer = new MongoTimer();

        public MongoDBHelper()
        {
            //mongoTimer.ActivateTimer();
            this._db = new global::MongoDB.MongoDB().GetDataBase();
        }

        public MongoDBHelper(IMongoDatabase db)
        {
            //mongoTimer.ActivateTimer();
            this._db = db;
        }

        public MongoDBHelper(string dbName)
        {
            this._db = new global::MongoDB.MongoDB().GetDataBase(dbName);
        }

        public IMongoDatabase GetMongoDatabase()
        {
            return _db;
        }

        #region 插入数据

        /// <summary>
        /// 将数据插入进数据库
        /// </summary>
        /// <typeparam name="T">需要插入数据的类型</typeparam>
        /// <param name="t">需要插入的具体实体</param>
        public void Insert<T>(T t)
        {
            //集合名称
            string collectionName = typeof(T).Name;
            Insert<T>(t, collectionName);
        }

        /// <summary>
        /// 将数据插入进数据库
        /// </summary>
        /// <typeparam name="T">需要插入数据库的实体类型</typeparam>
        /// <param name="t">需要插入数据库的具体实体</param>
        /// <param name="collectionName">指定插入的集合</param>
        public void Insert<T>(T t, string collectionName)
        {
            //IMongoClient
            OpenDb();
            IMongoCollection<BsonDocument> mc = this._db.GetCollection<BsonDocument>(collectionName);
            //将实体转换为bson文档
            BsonDocument bd = t.ToBsonDocument();
            //进行插入操作
            try
            {
                mc.InsertOne(bd);
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }

        }

        /// <summary>
        /// 批量插入数据
        /// </summary>
        /// <typeparam name="T">需要插入数据库的实体类型</typeparam>
        /// <param name="list">需要插入数据的列表</param>
        public void Insert<T>(List<T> list)
        {
            //集合名称
            string collectionName = typeof(T).Name;
            this.Insert<T>(list, collectionName);

        }

        /// <summary>
        /// 批量插入数据
        /// </summary>
        /// <typeparam name="T">需要插入数据库的实体类型</typeparam>
        /// <param name="list">需要插入数据的列表</param>
        /// <param name="collectionName">指定要插入的集合</param>
        public void Insert<T>(List<T> list, string collectionName)
        {
            OpenDb();
            IMongoCollection<BsonDocument> mc = this._db.GetCollection<BsonDocument>(collectionName);
            //创建一个空间bson集合
            List<BsonDocument> bsonList = new List<BsonDocument>();
            //批量将数据转为bson格式 并且放进bson文档
            list.ForEach(t => bsonList.Add(t.ToBsonDocument()));
            //批量插入数据
            mc.InsertMany(bsonList);
        }

        public void InsertLog<T>(List<T> list, string collectionName)
        {
            MongoUrlBuilder builder = new MongoUrlBuilder();
           
            OpenDb();
            //if (!_db.CollectionExists(collectionName))
            //{
            //    _db.CreateCollection(collectionName, ICollectionOptions
            //        .SetCapped(true)
            //        .SetMaxSize(100000)
            //        .SetMaxDocuments(100)
            //    );

            //    //Insert an empty document as without this 'cursor.IsDead' is always true
            //    //var coll = _db.GetCollection("Queue");
            //    //coll.Insert(new BsonDocument(new Dictionary<string, object> { { "PROCESSED", true }, }), WriteConcern.Unacknowledged);
            //}
            this.Insert<T>(list, collectionName);
        }


        #endregion

        #region 查询数据

        #region 查询所有记录

        /// <summary>
        /// 查询一个集合中的所有数据
        /// </summary>
        /// <typeparam name="T">该集合数据的所属类型</typeparam>
        /// <param name="collectionName">指定集合的名称</param>
        /// <param name="fields">查询的资料</param>
        /// <returns>返回一个List列表</returns>
        public List<T> FindAll<T>(string collectionName, BsonDocument fd)
        {
            
            
            OpenDb();
            IMongoCollection<T> mc = this._db.GetCollection<T>(collectionName);
            //以实体方式取出其数据集合 
            //MongoCursor<T> mongoCursor = mc.Find();
            var cursor = mc.Find(fd);
            //直接转化为List返回
            if (fd == null)
            {
                fd = new BsonDocument() { { "_id", 0 } };
            }
            else
                fd.Add("_id", 0);
            return cursor.ToList();
        }

        /// <summary>
        /// 查询一个集合中的所有数据 其集合的名称为T的名称
        /// </summary>
        /// <typeparam name="T">该集合数据的所属类型</typeparam>
        /// <returns>返回一个List列表</returns>
        public List<T> FindAll<T>(BsonDocument fd = null)
        {
            
            string collectionName = typeof(T).Name;
            return FindAll<T>(collectionName, fd);
        }

        #endregion

        #region 查询一条记录

        /// <summary>
        /// 查询一条记录
        /// </summary>
        /// <typeparam name="T">该数据所属的类型</typeparam>
        /// <param name="query">查询的条件 可以为空</param>
        /// <param name="collectionName">去指定查询的集合</param>
        /// <returns>返回一个实体类型</returns>
        public FilterDefinition<BsonDocument> FindOne<T>(FilterDefinition<BsonDocument> query, string collectionName)
        {
            OpenDb();
            var mc = this._db.GetCollection<BsonDocument>(collectionName);
            query = this.InitQuery(query);
            var t = mc.Find(query).FirstOrDefault();
            return t;
        }


        /// <summary>
        /// 查询一条记录
        /// </summary>
        /// <typeparam name="T">该数据所属的类型</typeparam>
        /// <param name="query">查询的条件 可以为空</param>
        /// <returns>返回一个实体类型</returns>
        public FilterDefinition<BsonDocument> FindOne<T>(FilterDefinition<BsonDocument> query)
        {
           //List<FilterDefinition<BsonDocument>> QueryConditionList = new List<FilterDefinition<BsonDocument>>();
            //IMongoQuery
            string collectionName = typeof(T).Name;
            return FindOne<BsonDocument>(query, collectionName);
        }

        #endregion

        #region 普通的条件查询

        /// <summary>
        /// 根据指定条件查询集合中的数据
        /// </summary>
        /// <typeparam name="T">该集合数据的所属类型</typeparam>
        /// <param name="query">指定的查询条件 比如Query.And(Query.EQ("username","admin"),Query.EQ("password":"admin"))</param>
        /// <param name="collectionName">指定的集合的名称</param>
        /// <param name="sortBy"></param>
        /// <param name="fields"></param>
        /// <returns>返回一个List列表</returns>
        public List<BsonDocument> Find<T>(FilterDefinition<BsonDocument> query, string collectionName, BsonDocument sortBy = null, string[] fields = null)
        {
            OpenDb();
            var mc = this._db.GetCollection<BsonDocument>(collectionName);
            query = this.InitQuery(query);
            var sortBys = this.InitSortBy(sortBy);
            //MongoCursor<T> mongoCursor = mc.Find(query);
            var sort = Builders<BsonDocument>.Sort.Ascending("borough").Ascending("address.zipcode");
            var result = mc.Find(query).Sort(sort).ToList();
            return result;
        }

        /// <summary>
        /// 根据指定条件查询集合中的数据
        /// </summary>
        /// <typeparam name="T">该集合数据的所属类型</typeparam>
        /// <param name="query">指定的查询条件 比如Query.And(Query.EQ("username","admin"),Query.EQ("password":"admin"))</param>
        /// <param name="collectionName">指定的集合的名称</param>
        /// <param name="sortBy"></param>
        /// <param name="fd"></param>
        /// <returns>返回一个List列表</returns>
        public List<BsonDocument> FindFilterKey<T>(FilterDefinition<BsonDocument> query, string collectionName, BsonDocument sortBy = null)
        {
            OpenDb();
            var mc = this._db.GetCollection<BsonDocument>(collectionName);
            query = this.InitQuery(query);
            var sortBys = this.InitSortBy(sortBy);
            var cursor = mc.Find(query);
            return cursor.Sort(sortBy).ToList();
        }


        /// <summary>
        /// 根据指定条件查询集合中的数据
        /// </summary>
        /// <typeparam name="T">该集合数据的所属类型</typeparam>
        /// <param name="query">指定的查询条件 比如Query.And(Query.EQ("username","admin"),Query.EQ("password":"admin"))</param>
        /// <param name="sortBy"></param>
        /// <param name="fields"></param>
        /// <returns>返回一个List列表</returns>
        public List<BsonDocument> Find<T>(FilterDefinition<BsonDocument> query)
        {
            string collectionName = typeof(T).Name;
            
            return this.Find<BsonDocument>(query, collectionName, null);
        }

        /// <summary>
        /// 根据指定条件查询集合中的数据
        /// </summary>
        /// <typeparam name="T">该集合数据的所属类型</typeparam>
        /// <param name="query">指定的查询条件 比如Query.And(Query.EQ("username","admin"),Query.EQ("password":"admin"))</param>
        /// <param name="sortBy"></param>
        /// <param name="fields"></param>
        /// <returns>返回一个List列表</returns>
        public List<BsonDocument> Find<T>(FilterDefinition<BsonDocument> query, BsonDocument sortBy)
        {
            string collectionName = typeof(T).Name;
            return this.Find<BsonDocument>(query, collectionName, sortBy);
        }

        /// <summary>
        /// 根据指定条件查询集合中的数据
        /// </summary>
        /// <typeparam name="T">该集合数据的所属类型</typeparam>
        /// <param name="query">指定的查询条件 比如Query.And(Query.EQ("username","admin"),Query.EQ("password":"admin"))</param>
        /// <param name="sortBy"></param>
        /// <param name="fields"></param>
        /// <returns>返回一个List列表</returns>
        public List<BsonDocument> FindFilterKey<T>(FilterDefinition<BsonDocument> query, BsonDocument sortBy = null)
        {
            string collectionName = typeof(T).Name;
            return this.FindFilterKey<BsonDocument>(query, collectionName, sortBy);
        }

        #endregion

        #region 分页查询 PageIndex和PageSize模式  在页数PageIndex大的情况下 效率明显变低

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
        public List<BsonDocument> Find<T>(FilterDefinition<BsonDocument> query, int pageIndex, int pageSize, BsonDocument sortBy, string collectionName)
        {
            OpenDb();
            var mc = this._db.GetCollection<BsonDocument>(collectionName);
            query = this.InitQuery(query);
            var sortBys = this.InitSortBy(sortBy);
            //如页序号为0时初始化为1
            pageIndex = pageIndex == 0 ? 1 : pageIndex;
            //按条件查询 排序 跳数 取数
            var mongoCursor = mc.Find(query).Sort(sortBy).Skip((pageIndex - 1) * pageSize).Limit(pageSize);
            return mongoCursor.ToList<BsonDocument>();
        }


        /// <summary>
        /// 分页查询 PageIndex和PageSize模式  在页数PageIndex大的情况下 效率明显变低
        /// </summary>
        /// <typeparam name="T">所需查询的数据的实体类型</typeparam>
        /// <param name="query">查询的条件</param>
        /// <param name="pageIndex">当前的页数</param>
        /// <param name="pageSize">当前的尺寸</param>
        /// <param name="sortBy">排序方式</param>
        /// <returns>返回List列表</returns>
        public List<BsonDocument> Find<T>(FilterDefinition<BsonDocument> query, int pageIndex, int pageSize, BsonDocument sortBy)
        {
            string collectionName = typeof(T).Name;
            return this.Find<BsonDocument>(query, pageIndex, pageSize, sortBy, collectionName);
        }

        #endregion


        #endregion

        #region 更新数据

        /// <summary>
        /// 更新数据
        /// </summary>
        /// <typeparam name="T">更新的数据 所属的类型</typeparam>
        /// <param name="query">更新数据的查询</param>
        /// <param name="update">需要更新的文档</param>
        /// <param name="collectionName">指定更新集合的名称</param>
        public bool Update<T>(FilterDefinition<BsonDocument> query, UpdateDefinition<BsonDocument> update ,string collectionName)
        {
            OpenDb();
            var mc = this._db.GetCollection<BsonDocument>(collectionName);

            query = this.InitQuery(query); 
            //更新数据
            var result = mc.UpdateOne(query, update);
            return result.IsAcknowledged;
        }

        /// <summary>
        /// 更新数据
        /// </summary>
        /// <typeparam name="T">更新的数据 所属的类型</typeparam>
        /// <param name="query">更新数据的查询</param>
        /// <param name="update">需要更新的文档</param>
        public bool Update<T>(FilterDefinition<BsonDocument> query, UpdateDefinition<BsonDocument> update)
        {
            string collectionName = typeof(T).Name;
            return this.Update<T>(query, update, collectionName);

        }

        #endregion

        #region 移除/删除数据

        /// <summary>
        /// 移除指定的数据
        /// </summary>
        /// <typeparam name="T">移除的数据类型</typeparam>
        /// <param name="query">移除的数据条件</param>
        /// <param name="collectionName">指定的集合名词</param>
        public void Remove<T>(FilterDefinition<BsonDocument> query, string collectionName)
        {
            OpenDb();
            var mc = this._db.GetCollection<BsonDocument>(collectionName);
            query = this.InitQuery(query);
            //根据指定查询移除数据
            var writeConcernResult = mc.DeleteOne(query);
        }

        /// <summary>
        /// 移除指定的数据
        /// </summary>
        /// <typeparam name="T">移除的数据类型</typeparam>
        /// <param name="query">移除的数据条件</param>
        public void Remove<T>(FilterDefinition<BsonDocument> query)
        {
            string collectionName = typeof(T).Name;
            this.Remove<T>(query, collectionName);
        }

        /// <summary>
        /// 移除实体里面所有的数据
        /// </summary>
        /// <typeparam name="T">移除的数据类型</typeparam>
        public void RemoveAll<T>()
        {
            string collectionName = typeof(T).Name;
            this.Remove<T>(null, collectionName);
        }

        /// <summary>
        /// 移除实体里面所有的数据
        /// </summary>
        /// <typeparam name="T">移除的数据类型</typeparam>
        /// <param name="collectionName">指定的集合名称</param>
        public void RemoveAll<T>(string collectionName)
        {
            this.Remove<T>(null, collectionName);

        }

        #endregion

        #region 创建索引

        /// <summary>
        /// 创建索引 ，不再依赖代码去做，交给dba。   
        /// </summary>
        /// <typeparam name="T">需要创建索引的实体类型</typeparam>
        [Obsolete("已过时，不再依赖代码去做，交给dba")]
        public void CreateIndex<T>()
        {//代码级不做创建索引操作
            return;
            //            OpenDb();
            //            string collectionName = typeof (T).Name;
            //            MongoCollection<BsonDocument> mc = this._db.GetCollection<BsonDocument>(collectionName);

            //            PropertyInfo[] propertys =
            //                typeof (T).GetProperties(BindingFlags.IgnoreCase | BindingFlags.Instance | BindingFlags.Public |
            //                                         BindingFlags.SetProperty);
            //            //得到该实体类型的属性
            //            foreach (PropertyInfo property in propertys)
            //            {
            //                //在各个属性中得到其特性
            //                foreach (object obj in property.GetCustomAttributes(true))
            //                {
            //                    MongoDBFieldAttribute mongoField = obj as MongoDBFieldAttribute;
            //                    if (mongoField != null)
            //                    {
            //// 此特性为mongodb的字段属性

            //                        IndexKeysBuilder indexKey;
            //                        if (mongoField.Ascending)
            //                        {
            //                            //升序 索引
            //                            indexKey = IndexKeys.Ascending(property.Name);
            //                        }
            //                        else
            //                        {
            //                            //降序索引
            //                            indexKey = IndexKeys.Ascending(property.Name);
            //                        }
            //                        //创建该属性
            //                        mc.CreateIndex(indexKey, IndexOptions.SetUnique(mongoField.Unique));
            //                    }
            //                }
            //            }
        }

        #endregion

        #region 获取集合的存储大小

        /// <summary>
        /// 获取集合的存储大小
        /// </summary>
        /// <typeparam name="T">该集合对应的实体类</typeparam>
        /// <returns>返回一个long型</returns>
        public long GetDataSize<T>()
        {
            string collectionName = typeof(T).Name;
            return GetDataSize(collectionName);
        }

        /// <summary>
        /// 获取集合的存储大小
        /// </summary>
        /// <param name="collectionName">该集合对应的名称</param>
        /// <returns>返回一个long型</returns>
        public long GetDataSize(string collectionName)
        {
            OpenDb();
            var mc = this._db.GetCollection<BsonDocument>(collectionName);
            //return mc.GetTotalStorageSize();

            return 0;
        }


        #endregion

        #region 私有的一些辅助方法

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

        #endregion

        public void OpenDb()
        {
            // 激活 Timer
            //mongoTimer.ActivateTimer();
          
            //if (_db.Server.State == MongoServerState.Disconnected)
            //{
            //    _db.Settings.ReadConcern;
            //    try
            //    {
            //        _db.Server.Connect();
            //    }
            //    catch (Exception ex)
            //    {
            //        MongoTimerAsyncHelper timerAsync = new MongoTimerAsyncHelper();
            //        timerAsync.BeginAddBabyVaccinationTime(null, null);
            //    }

            //}

            if (Config.ConfigAccess<MongoConfig>.GetConfig().TestUserPass == 1)
            {
                //FileHelper.SysLog("TestUserPass", "mongo");
                try
                {
                    // 尝试连接，测试用户名密码是否正确
                    //_db.GetStats();
                }
                catch (Exception ex)
                {
                    MongoTimerAsyncHelper timerAsync = new MongoTimerAsyncHelper();
                    timerAsync.BeginAddBabyVaccinationTime(null, null);
                }
            }

        }

        public void Dispose()
        {
            //_db.Server.Disconnect();
        }
    }
}
