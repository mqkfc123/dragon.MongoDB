using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MongoDB.Entity
{
    [Serializable]
    public class MongoConnection : IConfigInfo
    { 
        public MongoConnection()
        { }
        /// <summary>
        /// 主键
        /// </summary>
        public int Id { get; set; }
        /// <summary>
        /// 服务器，支持多台
        /// </summary>
        public string Host { get; set; }
        /// <summary>
        /// 端口，支持多台
        /// </summary>
        public string Port { get; set; }

        public string UserName { get; set; }

        public string PassWord { get; set; }

        public string DBName { get; set; }
        /// <summary>
        /// 配置ID，关联外键
        /// </summary>
        public int CfgId { get; set; }

        /// <summary>
        /// 链接字符串   mongodb://[username:password@]hostname[:port][/database]
        /// </summary>
        // public string ConnectionString { get; set; }

    }
}
