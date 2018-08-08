using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Entity;
using MongoDB.Util;
using MongoDB.Util.Log4net;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
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
        T FindOne<T>(BsonDocument fd, string collectionName = "");

        /// <summary>
        /// 根据指定条件查询集合中的数据
        /// </summary>
        /// <typeparam name="T">该集合数据的所属类型</typeparam>
        /// <param name="fd">指定的查询条件 </param>
        /// <param name="collectionName">指定的集合的名称</param>
        /// <param name="sortBy"></param>
        /// <param name="fields"></param>
        /// <returns>返回一个List列表</returns>
        IEnumerable<T> Find<T>(BsonDocument fd, string collectionName, string sortBy = null, params string[] fields);

        /// <summary>
        /// 分页查询 PageIndex和PageSize模式  在页数PageIndex大的情况下 效率明显变低
        /// </summary>
        /// <typeparam name="T">所需查询的数据的实体类型</typeparam>
        /// <param name="fd">查询的条件</param>
        /// <param name="pageIndex">当前的页数</param>
        /// <param name="pageSize">当前的尺寸</param>
        /// <param name="collectionName">集合名称</param>
        /// <param name="sortBy">排序方式</param>
        /// <param name="fields">排序方式</param>
        /// <returns>返回List列表</returns>
        IEnumerable<T> Find<T>(BsonDocument fd, int pageIndex, int pageSize, string collectionName, string sortBy = null, params string[] fields);

        /// <summary>
        /// 更新数据
        /// </summary>
        /// <typeparam name="T">更新的数据 所属的类型</typeparam>
        /// <param name="query">更新数据的查询</param>
        /// <param name="update">需要更新的文档</param>
        /// <param name="collectionName">指定更新集合的名称</param>
        bool Update<T>(BsonDocument fd, BsonDocument update, string collectionName);

        /// <summary>
        /// 移除指定的数据
        /// </summary>
        /// <typeparam name="T">移除的数据类型</typeparam>
        /// <param name="query">移除的数据条件</param>
        /// <param name="collectionName">指定的集合名词</param>
        void Remove<T>(BsonDocument fd, string collectionName);

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

        /// <summary>
        /// 插入单条数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="t"></param>
        /// <param name="collectionName"></param>
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

        /// <summary>
        /// 插入集合数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list"></param>
        /// <param name="collectionName"></param>
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

        /// <summary>
        /// 查询一个集合中的所有数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="fd"></param>
        /// <returns></returns>
        public IEnumerable<T> FindAll<T>(string collectionName = "", BsonDocument fd = null)
        {

            if (string.IsNullOrEmpty(collectionName))
                collectionName = typeof(T).Name;

            var mc = this._db.GetCollection<BsonDocument>(collectionName);

            //直接转化为List返回
            if (fd == null)
            {
                fd = new BsonDocument() { { "_id", 0 } };
            }
            else
            {
                fd.Add("_id", 0);
            }

            FilterDefinitionBuilder<BsonDocument> builderFilter = Builders<BsonDocument>.Filter;
            //约束条件
            FilterDefinition<BsonDocument> filter = builderFilter.Eq("stuname", "dragon");
            //Builders<BsonDocument>.Filter.Eq("stuname", "dragon");

            //获取数据
            var result = mc.Find<BsonDocument>(filter).ToList();

            var list = new List<T>();
            foreach (var m in result)
            {
                var bsElements = m.Elements.Skip(1).Take(m.Elements.Count() - 1).ToList();
                var bsDocs = new BsonDocument();
                for (int i = 0; i < bsElements.Count(); i++)
                {
                    bsDocs.SetElement(bsElements[i]);
                }
                string json = bsDocs.ToJson();
                var user = JsonConvert.DeserializeObject<T>(json);
                list.Add(user);
            }
            return list.ToArray();
        }

        /// <summary>
        /// 查询单个实体
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="collectionName"></param>
        /// <returns></returns>
        public T FindOne<T>(BsonDocument fd, string collectionName = "")
        {
            if (string.IsNullOrEmpty(collectionName))
                collectionName = typeof(T).Name;

            var mc = this._db.GetCollection<BsonDocument>(collectionName);
            //约束条件
            //FilterDefinitionBuilder<BsonDocument> builderFilter = Builders<BsonDocument>.Filter;
            //约束条件
            //FilterDefinition<BsonDocument> filter = Builders<BsonDocument>.Filter.Eq("fieId", "value");
            FilterDefinitionBuilder<BsonDocument> builder = Builders<BsonDocument>.Filter;
            FilterDefinition<BsonDocument> filter = builder.Empty;
            var fieId = "";
            var value = "";
            var dict = fd.ToDictionary();
            foreach (string key in dict.Keys)
            {
                fieId = key;
                value = Convert.ToString(dict[key]);
                if (filter == builder.Empty)
                {
                    filter = builder.Eq(fieId, value);
                }
                else
                {
                    filter = filter & builder.Eq(fieId, value);
                }
            }

            //获取数据
            var result = mc.Find<BsonDocument>(filter).FirstOrDefault();
            var bsElements = result.Elements.Skip(1).Take(result.Elements.Count() - 1).ToList();
            var bsDoc = new BsonDocument();
            for (int i = 0; i < bsElements.Count(); i++)
            {
                bsDoc.SetElement(bsElements[i]);
            }
            string json = bsDoc.ToJson();
            return JsonConvert.DeserializeObject<T>(json);
        }

        /// <summary>
        /// 根据指定条件查询集合中的数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="collectionName"></param>
        /// <param name="sortBy"></param>
        /// <param name="fields"></param>
        /// <returns></returns>
        public IEnumerable<T> Find<T>(BsonDocument fd, string collectionName, string sortBy = null, params string[] fields)
        {
            if (string.IsNullOrEmpty(collectionName))
                collectionName = typeof(T).Name;

            var mc = this._db.GetCollection<BsonDocument>(collectionName);

            FilterDefinitionBuilder<BsonDocument> builder = Builders<BsonDocument>.Filter;
            FilterDefinition<BsonDocument> filter = builder.Empty;
            var fieId = "";
            var value = "";
            var dict = fd.ToDictionary();
            foreach (string key in dict.Keys)
            {
                fieId = key;
                value = Convert.ToString(dict[key]);
                if (filter == builder.Empty)
                {
                    filter = builder.Eq(fieId, value);
                }
                else
                {
                    filter = filter & builder.Eq(fieId, value);
                }
            }

            #region 排序
            SortDefinitionBuilder<BsonDocument> sortFilter = Builders<BsonDocument>.Sort;
            SortDefinition<BsonDocument> filters = null;
            if (sortBy.ToLower() == "asc")
            {
                foreach (var item in fields)
                {
                    if (filters == null)
                    {
                        filters = sortFilter.Ascending(item);
                    }
                    else
                    {
                        filters = filters.Ascending(item);
                    }
                }
            }
            else if (sortBy.ToLower() == "desc")
            {
                foreach (var item in fields)
                {
                    if (filters == null)
                    {
                        filters = sortFilter.Descending(item);
                    }
                    else
                    {
                        filters = filters.Descending(item);
                    }
                }
            }
            else
            {
                filters = sortFilter.Descending(OBJECTID_KEY);
            }
            #endregion

            var result = mc.Find<BsonDocument>(filter).Sort(filters).ToList();
            var list = new List<T>();
            foreach (var m in result)
            {
                var bsElements = m.Elements.Skip(1).Take(m.Elements.Count() - 1).ToList();
                var bsDocs = new BsonDocument();
                for (int i = 0; i < bsElements.Count(); i++)
                {
                    bsDocs.SetElement(bsElements[i]);
                }
                string json = bsDocs.ToJson();
                var user = JsonConvert.DeserializeObject<T>(json);
                list.Add(user);
            }
            return list.ToArray();
        }

        /// <summary>
        /// 分页查询
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fd"></param>
        /// <param name="pageIndex"></param>
        /// <param name="pageSize"></param>
        /// <param name="collectionName"></param>
        /// <param name="sortBy"></param>
        /// <param name="fields"></param>
        /// <returns></returns>
        public IEnumerable<T> Find<T>(BsonDocument fd, int pageIndex, int pageSize, string collectionName, string sortBy = null, params string[] fields)
        {
            if (string.IsNullOrEmpty(collectionName))
                collectionName = typeof(T).Name;

            var mc = this._db.GetCollection<BsonDocument>(collectionName);

            FilterDefinitionBuilder<BsonDocument> builder = Builders<BsonDocument>.Filter;
            FilterDefinition<BsonDocument> filter = builder.Empty;
            var fieId = "";
            var value = "";
            var dict = fd.ToDictionary();
            foreach (string key in dict.Keys)
            {
                fieId = key;
                value = Convert.ToString(dict[key]);
                if (filter == builder.Empty)
                {
                    filter = builder.Eq(fieId, value);
                }
                else
                {
                    filter = filter & builder.Eq(fieId, value);
                }
            }

            #region 排序
            SortDefinitionBuilder<BsonDocument> sortFilter = Builders<BsonDocument>.Sort;
            SortDefinition<BsonDocument> filters = null;
            if (sortBy.ToLower() == "asc")
            {
                foreach (var item in fields)
                {
                    if (filters == null)
                    {
                        filters = sortFilter.Ascending(item);
                    }
                    else
                    {
                        filters = filters.Ascending(item);
                    }
                }
            }
            else if (sortBy.ToLower() == "desc")
            {
                foreach (var item in fields)
                {
                    if (filters == null)
                    {
                        filters = sortFilter.Descending(item);
                    }
                    else
                    {
                        filters = filters.Descending(item);
                    }
                }
            }
            else
            {
                filters = sortFilter.Descending(OBJECTID_KEY);
            }
            #endregion

            //如页序号为0时初始化为1
            pageIndex = pageIndex == 0 ? 1 : pageIndex;
            //按条件查询 排序 跳数 取数
            var result = mc.Find<BsonDocument>(filter)
                .Sort(filters)
                .Skip((pageIndex - 1) * pageSize)
                .Limit(pageSize)
                .ToList(); 
            var list = new List<T>();
            foreach (var m in result)
            {
                var bsElements = m.Elements.Skip(1).Take(m.Elements.Count() - 1).ToList();
                var bsDocs = new BsonDocument();
                for (int i = 0; i < bsElements.Count(); i++)
                {
                    bsDocs.SetElement(bsElements[i]);
                }
                string json = bsDocs.ToJson();
                var user = JsonConvert.DeserializeObject<T>(json);
                list.Add(user);
            }
            return list.ToArray();
        }

        /// <summary>
        /// 更新数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="update"></param>
        /// <param name="collectionName"></param>
        /// <returns></returns>
        public bool Update<T>(BsonDocument fd, BsonDocument update, string collectionName)
        {
            if (string.IsNullOrEmpty(collectionName))
                collectionName = typeof(T).Name;

            var mc = this._db.GetCollection<BsonDocument>(collectionName);

            FilterDefinitionBuilder<BsonDocument> builder = Builders<BsonDocument>.Filter;
            FilterDefinition<BsonDocument> filter = builder.Empty;
            var fieId = "";
            var value = "";
            var dict = fd.ToDictionary();
            foreach (string key in dict.Keys)
            {
                fieId = key;
                value = Convert.ToString(dict[key]);
                if (filter == builder.Empty)
                {
                    filter = builder.Eq(fieId, value);
                }
                else
                {
                    filter = filter & builder.Eq(fieId, value);
                }
            }

            UpdateDefinitionBuilder<BsonDocument> up_builder = Builders<BsonDocument>.Update;
            UpdateDefinition<BsonDocument> up_filter = null;
            var up_fieId = "";
            var up_value = "";
            var up_dict = update.ToDictionary();
            foreach (string key in up_dict.Keys)
            {
                up_fieId = key;
                up_value = Convert.ToString(up_dict[key]);
                if (up_filter == null)
                {
                    up_filter = up_builder.Set(f => f[key], up_value);
                }
            }
            //更新数据
            var result = mc.UpdateOne(filter, up_filter);
            return result.IsAcknowledged;
        }

        /// <summary>
        /// 移除
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="collectionName"></param>
        public void Remove<T>(BsonDocument fd, string collectionName)
        {
            if (string.IsNullOrEmpty(collectionName))
                collectionName = typeof(T).Name;

            var mc = this._db.GetCollection<BsonDocument>(collectionName);

            FilterDefinitionBuilder<BsonDocument> builder = Builders<BsonDocument>.Filter;
            FilterDefinition<BsonDocument> filter = builder.Empty;
            var fieId = "";
            var value = "";
            var dict = fd.ToDictionary();
            foreach (string key in dict.Keys)
            {
                fieId = key;
                value = Convert.ToString(dict[key]);
                if (filter == builder.Empty)
                {
                    filter = builder.Eq(fieId, value);
                }
                else
                {
                    filter = filter & builder.Eq(fieId, value);
                }
            }
            //根据指定查询移除数据
            var result = mc.DeleteOne(filter);
        }

        /// <summary>
        /// 查询集合总数， 不建议使用，效率慢
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <returns></returns>
        public long GetDataSize<T>(string collectionName)
        {
            if (string.IsNullOrEmpty(collectionName))
                collectionName = typeof(T).Name;

            var mc = this._db.GetCollection<BsonDocument>(collectionName);
            FilterDefinitionBuilder<BsonDocument> builder = Builders<BsonDocument>.Filter;
            FilterDefinition<BsonDocument> filter = builder.Empty;

            var result = mc.Find<BsonDocument>(filter).ToList().Count();
            return result;
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
                    _Logger.Error("MongoDBHelper OpenDb() 异常：", ex);
                }
            }
        }

        #endregion

        public void Dispose()
        {
            throw new NotImplementedException();
        }


    }

}
