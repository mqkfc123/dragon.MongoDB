﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoDB.Util.Log4net
{
    public static class LogHelper
    {
        public static ILog GetLogger(Type type)
        {
            return new LogWrapper(type);
        }
    }
}
