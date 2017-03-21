using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MongoDB.Util
{

    public class MongoTimerAsyncHelper
    {

        private delegate int MongoTimerDelegate();
        private static MongoTimerDelegate m_DelegateHandler = null;

        public MongoTimerAsyncHelper()
        {
            if (null == m_DelegateHandler)
            {
                m_DelegateHandler = new MongoTimerDelegate(this.LogicHandler);
            }

        }


        private int LogicHandler()
        {
            MongoTimer timer = new MongoTimer();

            timer.CallBack("同步执行");

            return 1;
        }

        public IAsyncResult BeginAddBabyVaccinationTime(AsyncCallback callBack, Object stateObject)
        {

            return m_DelegateHandler.BeginInvoke(callBack, stateObject);

        }

        /// <summary>
        /// 结束更新接种日期(异步)
        /// </summary>
        /// <param name="ar">异步结果对象</param>
        public int EndAddBabyVaccinationTime(IAsyncResult ar)
        {
            if (ar == null)
                throw new NullReferenceException("IAsyncResult 参数不能为空");

            try
            {
                return m_DelegateHandler.EndInvoke(ar);
            }
            catch (Exception e)
            {
                throw e;
            }
        }

    }
}
