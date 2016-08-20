using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CTP;

namespace RealPosi
{
    class Program
    {
        public CTPTrader Td = null;

        static void Main(string[] args)
        {
            Program App = new Program();
            App.Run();
        }

        void Run()
        {
            try
            {
                Logger.WriteLog_text("程序启动");
                if (DateTime.Now.DayOfWeek.ToString() == "Sunday" || DateTime.Now.DayOfWeek.ToString() == "Saturday" || DateTime.Now.Hour > 17 || DateTime.Now.Hour < 8)
                {
                    Logger.WriteLog_text("现在不在工作时间（周一至周五，08:00~17:00）。程序退出 \r\n");
                    return;
                }
                Config.Load_ConfigFile();   // Load Config file
                GlobalVar.SQLConnectionString = Config.Load_MySQLConnection();
                GlobalVar.Sqli = new SQLi(GlobalVar.SQLConnectionString);    //要在Load_MySQLConnection后实例化
                List<Account> Account_List = GlobalVar.Sqli.Load_Account();  //读 "SELECT * FROM portfolio.real_account where AccountType='CTP' and ENABLE = 1"
                foreach (Account ac in Account_List)
                {
                    Logger.WriteLog_text("Sync Account InvestorID: " + ac.InvestorID + "   TradeFrontAddress：" + ac.TradeFrontAddress);
                    Task SyncTask = Task.Factory.StartNew(() => SyncPosition(ac));   // 起一个新线程运行SyncTask
                    SyncTask.Wait();    //等待查询完成后，Update
                    if (Td.bTaskComplete)
                    {
                        Logger.WriteLog_text("CTP 所有合约信息查询完毕。 市场合约数： " + GlobalVar.InstrumentAlls.Count);
                        Logger.WriteLog_text("用户持仓查询完毕。持仓数据条数：" + Td.PositionList.Count);
                        //print(Td.PositionList);
                        GlobalVar.Sqli.UpdatePosition(ac.AccountID, Td.TradingDay, Td.PositionList);
                        GlobalVar.Sqli.UpdateAccountMoney(ac.AccountID, Td.TradingDay, Td.TradingAccount);
                        Logger.WriteLog_text("程序任务完成，正常退出\r\n");
                    }
                    else
                    {
                        Logger.WriteLog_text("程序没有正常获取到合约、持仓等数据。自动退出\r\n");
                        MyEmail.DefualtSendMail("程序没有正常获取到合约、持仓等数据。自动退出\r\n");
                    }
                }
                Td = null;
                if (Account_List.Count == 0)
                {
                    Logger.WriteLog_text("No ENABLE Real_Account，自动退出");
                }
                Thread.Sleep(1000);
            }
            catch (Exception e)
            {
                Logger.WriteLog_text("程序异常退出，信息：" + e.Message + "\r\n");
                MyEmail.DefualtSendMail("程序异常退出，信息：" + e.Message + "\r\n");
            }

            Console.ReadKey();
        }


        void SyncPosition(Account Acct)   //同步线程
        {
            Td = new CTPTrader();
            Td.AccountID = Acct;
            Td.Init();
            //Td.ReqQryInvestorPosition();
        }

        void print(List<ThostFtdcInvestorPositionField> PositionList)
        {
            string printStr = "";
            string spc = "\t\t";
            string lineEnd = "\r\n";
            foreach (ThostFtdcInvestorPositionField posi in PositionList)
            {
                printStr += " string BrokerID;                " + spc + posi.BrokerID.ToString() + lineEnd;
                printStr += " double CashIn;                  " + spc + posi.CashIn.ToString() + lineEnd;
                printStr += " double CloseAmount;             " + spc + posi.CloseAmount.ToString() + lineEnd;
                printStr += " double CloseProfit;             " + spc + posi.CloseProfit.ToString() + lineEnd;
                printStr += " double CloseProfitByDate;       " + spc + posi.CloseProfitByDate.ToString() + lineEnd;
                printStr += " double CloseProfitByTrade;      " + spc + posi.CloseProfitByTrade.ToString() + lineEnd;
                printStr += " int CloseVolume;                " + spc + posi.CloseVolume.ToString() + lineEnd;
                printStr += " int CombLongFrozen;             " + spc + posi.CombLongFrozen.ToString() + lineEnd;
                printStr += " int CombPosition;               " + spc + posi.CombPosition.ToString() + lineEnd;
                printStr += " int CombShortFrozen;            " + spc + posi.CombShortFrozen.ToString() + lineEnd;
                printStr += " double Commission;              " + spc + posi.Commission.ToString() + lineEnd;
                printStr += " double ExchangeMargin;//交易所保证金" + spc + posi.ExchangeMargin.ToString() + lineEnd;
                printStr += " double FrozenCash;              " + spc + posi.FrozenCash.ToString() + lineEnd;
                printStr += " double FrozenCommission;        " + spc + posi.FrozenCommission.ToString() + lineEnd;
                printStr += " double FrozenMargin;//冻结保证金" + spc + posi.FrozenMargin.ToString() + lineEnd;
                printStr += " EnumHedgeFlagType HedgeFlag;//对冲类型 " + spc + posi.HedgeFlag.ToString() + lineEnd;
                printStr += " string InstrumentID;//合约ID    " + spc + posi.InstrumentID.ToString() + lineEnd;
                printStr += " string InvestorID;              " + spc + posi.InvestorID.ToString() + lineEnd;
                printStr += " int LongFrozen;//多头冻结数     " + spc + posi.LongFrozen.ToString() + lineEnd;
                printStr += " double LongFrozenAmount;        " + spc + posi.LongFrozenAmount.ToString() + lineEnd;
                printStr += " double MarginRateByMoney;       " + spc + posi.MarginRateByMoney.ToString() + lineEnd;
                printStr += " double MarginRateByVolume;      " + spc + posi.MarginRateByVolume.ToString() + lineEnd;
                printStr += " double OpenAmount;              " + spc + posi.OpenAmount.ToString() + lineEnd;
                printStr += " double OpenCost;//开仓成本      " + spc + posi.OpenCost.ToString() + lineEnd;
                printStr += " int OpenVolume;                 " + spc + posi.OpenVolume.ToString() + lineEnd;
                printStr += " EnumPosiDirectionType PosiDirection;//持仓方向" + spc + posi.PosiDirection.ToString() + lineEnd;
                printStr += " int Position;//持仓量           " + spc + posi.Position.ToString() + lineEnd;
                printStr += " double PositionCost;//持仓成本  " + spc + posi.PositionCost.ToString() + lineEnd;
                printStr += " EnumPositionDateType PositionDate;//持仓日期类型" + spc + posi.PositionDate.ToString() + lineEnd;
                printStr += " double PositionProfit;//持仓利润" + spc + posi.PositionProfit.ToString() + lineEnd;
                printStr += " double PreMargin;               " + spc + posi.PreMargin.ToString() + lineEnd;
                printStr += " double PreSettlementPrice;//昨日结算价格" + spc + posi.PreSettlementPrice.ToString() + lineEnd;
                printStr += " int SettlementID;	//            " + spc + posi.SettlementID.ToString() + lineEnd;
                printStr += " double SettlementPrice;//结算价格" + spc + posi.SettlementPrice.ToString() + lineEnd;
                printStr += " int ShortFrozen;//空头冻结数    " + spc + posi.ShortFrozen.ToString() + lineEnd;
                printStr += " double ShortFrozenAmount;       " + spc + posi.ShortFrozenAmount.ToString() + lineEnd;
                printStr += " int TodayPosition;//“今仓”数量" + spc + posi.TodayPosition.ToString() + lineEnd;
                printStr += " string TradingDay;//日期        " + spc + posi.TradingDay.ToString() + lineEnd;
                printStr += " double UseMargin;//用户保证金   " + spc + posi.UseMargin.ToString() + lineEnd;
                printStr += " int YdPosition;//“昨仓“数量   " + spc + posi.YdPosition.ToString() + lineEnd;
                printStr += "\r\n \r\n";
            }
            Console.WriteLine(printStr);
        }
    }
}
