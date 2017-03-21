using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;

namespace MongoDB.Entity
{
    [Serializable]
    [XmlRoot("MongoConfig")]
    public class MongoConfig : IConfigInfo
    { 
        public MongoConfig()
        {
             
        }
        public int Id { get; set; }
        public string ConfigServerUrl { get; set; }

        /// <summary>
        /// 间隔 单位分钟
        /// </summary>
        public int IntervalNum { get; set; }

        /// <summary>
        /// 项目名称
        /// </summary>
        public string ProjectName { get; set; }
        /// <summary>
        /// 数据库链接配置版本号，20150812
        /// </summary>
        public string Version { get; set; }

        /// <summary>
        /// 超时时长，单位分钟
        /// </summary>
        public int ConnectionTimeout { get; set; }
        /// <summary>
        /// 默认数据库
        /// </summary>
        public string DefaultDbName { get; set; }
        /// <summary>
        /// 为1，OpenDb时开起用户名密码测试
        /// </summary>
        public int TestUserPass { get; set; }

        public List<MongoConnection> ConnectionList { get; set; }
    }
}
