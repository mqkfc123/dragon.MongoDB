using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoDB
{

    public class MongoDbConfig
    {
        public static object Obj = new object();
        public static Dictionary<string, MongoDBHelper> LstDbHelper = new Dictionary<string, MongoDBHelper>();

        public static MongoDBHelper GetMongoDbHelper(string dbName)
        {
            
            if (!LstDbHelper.ContainsKey(dbName))
            {
                lock (Obj)
                {
                    if (!LstDbHelper.ContainsKey(dbName))
                    {
                        LstDbHelper.Add(dbName, new MongoDBHelper(new MongoFactory(dbName).CreateDatabase("")));
                    }
                    return LstDbHelper[dbName];
                }
            }
            return LstDbHelper[dbName];
        }
    }
}
