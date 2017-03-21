using System;
using System.Collections.Generic;
using System.Configuration;
using System.Text;
using System.Web;

using MongoDB.Config.XML;
using MongoDB.Entity;

namespace MongoDB.Config
{
    public sealed class ConfigAccess<T> where T : IConfigInfo, new()
    {
        private static T _instance;
        private static object lockHelper = new object();

        public static T GetConfig()
        {
            if (_instance == null)
            {
                lock (lockHelper)
                {
                    Type type = typeof(T); 
                    _instance = (T)SerializerXML.Load(type, GetPath(type.Name));
                    return _instance;
                }
            }
            return _instance;
        }

        public static void SaveConfig(IConfigInfo configInfo)
        {
            SerializerXML.Save((T)configInfo, GetPath(typeof(T).Name));
        }

        /// <summary>
        /// 获取配置文件位置
        /// </summary>
        /// <param name="configName"></param>
        /// <returns></returns>
        private static string GetPath(string configName)
        {
            string path = System.AppDomain.CurrentDomain.BaseDirectory;
            if (!path.ToLower().Contains("bin"))
            {
                path = path + "\\Bin\\";
            }
            switch (configName)
            {
                case "MongoConfig":
                    return path + ConfigPath.CFG_APPCONFIG;
                default:
                    return "";
            }
        }
    }
}
