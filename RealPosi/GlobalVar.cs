using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using CTP;

namespace RealPosi
{
    public class GlobalVar
    {
        public static string SQLConnectionString;

        public static Account AccountID = new Account();

        public static SQLi Sqli;
        public static MyEmail Email;

        /// <summary>
        /// 所有可交易合约代码
        /// </summary>
        public static SortedList<string, ThostFtdcInstrumentField> InstrumentAlls = new SortedList<string, ThostFtdcInstrumentField>(0);
    }
}
