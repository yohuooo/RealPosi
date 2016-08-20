using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RealPosi
{
    public class Account
    {
        public int AccountID;
        public string AccountName;
        public string AccountType;
        public string AccountConfig;
        public int ENABLE;

        public string BrokerID;
        public string InvestorID;   //也是 UserID
        public string Password;

        public string MarketDataFrontAddress;   //  tcp://ctp1-front11.citicsf.com:41205    //行情登录地址
        public string TradeFrontAddress;        //交易登录地址


        public string UserID
        {
            get { return InvestorID; }
        }
    }
}
