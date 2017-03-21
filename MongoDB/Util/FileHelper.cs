using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace MongoDB.Util
{

    public class FileHelper
    {

        public static void SysLog(string msg, string strmenu)
        {
            Stream fileStream = GetLogFileStream(strmenu);
            var writeAdapter = new StreamWriter(fileStream, Encoding.Default);
            writeAdapter.WriteLine("***********" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "************");
            writeAdapter.WriteLine("错误信息::::" + msg);
            writeAdapter.WriteLine("***********End******************************************");
            writeAdapter.WriteLine("");
            writeAdapter.Close();
            fileStream.Close();
        }

        /// <summary>
        /// 获取log文件流
        /// </summary>
        /// <param name="strmenu">log类型</param>
        /// <returns></returns>
        private static Stream GetLogFileStream(string strmenu = "")
        {
            string logfile = AppDomain.CurrentDomain.BaseDirectory + "log/" + DateTime.Now.ToString("yyyyMM") + "/" + DateTime.Now.ToString("yyyy-MM-dd");
            string logurl = string.Empty;
            if (string.IsNullOrEmpty(strmenu))
            {
                logurl = logfile + "/" +
                         DateTime.Now.ToString("yyyyMMddHH") + ".log";
            }
            else
            {
                logurl = logfile + "/" +
                         DateTime.Now.ToString("yyyyMMddHH") + "_" + strmenu + ".log";
            }
            var dir = new DirectoryInfo(logfile);
            if (!dir.Exists)
                dir.Create();
            Stream fileStream = null;
            fileStream = File.Open(logurl, FileMode.Append, FileAccess.Write, FileShare.Write);
            return fileStream;
        }

    }
}
