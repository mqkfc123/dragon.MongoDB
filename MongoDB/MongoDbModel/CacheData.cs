using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Bson;

namespace MongoDB.MongoDbModel
{
    public class CacheData
    {
        public ObjectId _id;
        public string Key { get; set; }
        public string Value { get; set; }
        public DateTime CreateDateTime { get; set; }
    }
}
