using MongoDB;
using MongoDB.Bson;
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

            //for (var i = 0; i < 5; i++)
            //{
            //    var studentId = 10000 + i;

            //    _db.Insert<School>(new School()
            //    {
            //        SchoolName = "哈佛大学",
            //        StudentId = studentId.ToString(),
            //        StudentName = "dragon" + i
            //    }
            //    , "school");
            //}

            // var stu2 = _db.FindOne<School>(new BsonDocument() { { "StudentId", 10001 } }, "school");


            // var stu = _db.FindOne<School>( new BsonDocument() { { "StudentId", 10001 }, { "StudentName", "dragon1" } }, "school");


            // var sortByStu = _db.Find<School>(new BsonDocument() { { "SchoolName", "哈佛大学" } }, "school", "desc", "StudentId");
            //var sortByStu = _db.Find<School>(new BsonDocument() { { "SchoolName", "哈佛大学" } }, 0, 3, "school", "desc", "StudentId");
            //var sum = _db.GetDataSize<School>("school");

           _db.Remove<School>(new BsonDocument() { { "StudentId", 10001 }, { "StudentName", "dragon1" } },"school");

        }

    }

    public class School
    {
        public string SchoolName { get; set; }

        public string StudentId { get; set; }

        public string StudentName { get; set; }
         
    }
}
