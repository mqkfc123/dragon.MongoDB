using MongoDB;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoTest
{
    class Program
    {

        private static  MongoDBHelper _db;
        static void Main(string[] args)
        {
            _db = MongoDbConfig.GetMongoDbHelper("school");

            _db.Insert<School>(new School() { name = "厦门大学", stuname = "dragon" });
        }
    }

    public class School
    {
        public string name { get; set; }
        public string stuname { get; set; }
    }
}
