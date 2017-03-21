using MongoDB.Entity;
using MongoDB.Util;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoDB
{

    public class MongoTimer
    {
       
        //private static System.Threading.Timer timers = null;

        private static DateTime _cacheTime;

        public MongoTimer()
        {
            _cacheTime = DateTime.Now;

        }

         
        //public void ActivateTimer()
        //{
        //    if (timers == null)
        //    {
        //        try
        //        {
        //            int intarnal = Config.ConfigAccess<MongoConfig>.GetConfig().IntervalNum;
        //            timers = new System.Threading.Timer(CallBack, "定时执行", 0, 1000 * 60 * intarnal);
        //        }
        //        catch (Exception ex)
        //        {
        //            FileHelper.SysLog(ex.Message, "mongo");
        //        }


        //    }
        //}


        /// <summary>
        /// 回调-发送
        /// </summary>
        /// <param name="o"></param>
        public void CallBack(object obj = null)
        {
            // "执行同步" 
            string msg = obj.ToString();

            FileHelper.SysLog(msg + "，_cacheTime=" + _cacheTime.ToShortTimeString(), "mongo");

            string xml = string.Empty;
            string url = string.Empty;
            string param = string.Empty;
            try
            {
                url = Config.ConfigAccess<MongoConfig>.GetConfig().ConfigServerUrl;
                param = string.Format("projectname={0}&version={1}", Config.ConfigAccess<MongoConfig>.GetConfig().ProjectName
                   , Config.ConfigAccess<MongoConfig>.GetConfig().Version);

                if (msg == "同步执行")
                {
                    // 强制同步
                    param += "&forcedsyn=1";
                }

                xml = HttpHelper.GetResponse(HttpMethod.POST, url, param);
            }
            catch (Exception ex)
            {
                // 请求异常
                FileHelper.SysLog(ex.Message + "  url=" + url + "?" + param, "mongo");
            }

            // 不为空更新本地XML
            if (!string.IsNullOrWhiteSpace(xml))
            {
                try
                {
                    MongoConfig _config = (MongoConfig)Config.XML.SerializerXML.Deserialize(typeof(MongoConfig), xml);
                    if (_config != null && _config.ConnectionList != null && _config.ConnectionList.Count != 0)
                    {
                        // 更新本地XML，会写入Bin下的MongoConfig.xml，应用程序会重启
                        Config.ConfigAccess<MongoConfig>.SaveConfig(_config);
                    }

                }
                catch (Exception ex)
                {
                    FileHelper.SysLog(ex.Message + "  xml:" + xml, "mongo");
                }

            }


        }


    }
}
