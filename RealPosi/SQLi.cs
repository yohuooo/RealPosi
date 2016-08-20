using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using MySQL_DBC;
using CTP;
using STradingSystemHelper;
using STradingSystemHelper.CTP2;
using STradingSystemHelper.Utility;


namespace RealPosi
{
    public class SQLi
    {
        public SQLi()
        {
            MySQL_DBC.Const.SQLConnection = GlobalVar.SQLConnectionString;
        }
        public SQLi(string ConnectionString)
        {
            MySQL_DBC.Const.SQLConnection = ConnectionString;
        }

        public List<Account> Load_Account()
        {
            List<Account> RtnAccount = new List<Account>();
            string sqlStr = "SELECT * FROM portfolio.real_account where AccountType='CTP' and ENABLE = 1";
            Logger.WriteLog_text(sqlStr);
            DataTable dt = MySQL_DBC.BaseDA.ExecuteDataSet(sqlStr).Tables[0];
            foreach (DataRow row in dt.Rows)
            {
                Account act = new Account();
                act.AccountID = Convert.ToInt32(row["AccountID"]);
                act.AccountName = row["AccountName"].ToString();
                act.AccountType = row["AccountType"].ToString();
                act.AccountConfig = row["AccountConfig"].ToString();
                act.ENABLE = Convert.ToInt32(row["ENABLE"]);

                //解析 AccountConfig
                string AccountConfig = act.AccountConfig.Replace("\r\n", "|").Replace(" ", "");
                List<string> AccountConfig_StringList = AccountConfig.Split('|').ToList();
                act.BrokerID = AccountConfig_StringList.FirstOrDefault(i => i.StartsWith("BrokerID=")).Split('=')[1];
                act.TradeFrontAddress = AccountConfig_StringList.FirstOrDefault(i => i.StartsWith("FrontAddress=")).Split('=')[1];
                if (!act.TradeFrontAddress.StartsWith("tcp://"))
                    act.TradeFrontAddress = "tcp://" + act.TradeFrontAddress;
                
                act.InvestorID = AccountConfig_StringList.FirstOrDefault(i => i.StartsWith("InvestorID=")).Split('=')[1];
                act.Password = AccountConfig_StringList.FirstOrDefault(i => i.StartsWith("Password=")).Split('=')[1];

                RtnAccount.Add(act);
            }
            return RtnAccount;
        }

        public void UpdateAccountMoney(int AccountID, string TradingDay, ThostFtdcTradingAccountField TradingAccount)
        {
            string Balance = TradingAccount.Balance.ToString();//总资产
            string Available = TradingAccount.Available.ToString();//可用资金
            string CurrMargin = TradingAccount.CurrMargin.ToString();//持仓资金
            string FrozenMargin = TradingAccount.FrozenMargin.ToString();//冻结资金
            string split_Begin = "'";
            string split_Mid = "','";
            string split_End = "')";
            string InsertSQL = "INSERT INTO portfolio.account_position (`DATE`,`ID`,`SYMBOL`,`LONG`,`LONG_AVAIL`) VALUES (";
            InsertSQL += split_Begin;
            InsertSQL += TradingDay + split_Mid;
            InsertSQL += AccountID.ToString() + split_Mid;
            InsertSQL += "CNY" + split_Mid;
            InsertSQL += Balance + split_Mid;
            InsertSQL += Available + split_End;

            string UpdateSQL = "UPDATE portfolio.account_position SET `LONG`='" + Balance;
            UpdateSQL += ",`LONG_AVAIL`=`LONG_AVAIL`+" + Available;
            UpdateSQL += " WHERE `SYMBOL`='" + "CNY" + "' AND `ID`='" + AccountID + "' AND `DATE` = '" + TradingDay + "'";
            if(!IsAccountMoneyExist(AccountID, TradingDay))
            {
                Logger.WriteLog_text(InsertSQL);
                BaseDA.ExecuteNonQuery(InsertSQL);
            }
            else
            {
                Logger.WriteLog_text(UpdateSQL);
                BaseDA.ExecuteNonQuery(UpdateSQL);
            }
        }

        public int UpdatePosition(int AccountID,string TradingDay, List<ThostFtdcInvestorPositionField> PositionDatas)
        {
            string account_position_TableName = "portfolio.account_position";
            if (PositionDatas.Count <= 0)
                return 0;
            
            //删除旧数据
            string DeleteSQL = "DELETE FROM "+ account_position_TableName + " WHERE `DATE` = '"+ TradingDay + "' AND `ID` = '"+ AccountID.ToString() + "'";
            Logger.WriteLog_text(DeleteSQL + "\r\n 删除当天重复数据行数：" + BaseDA.ExecuteNonQuery(DeleteSQL));            

            foreach (ThostFtdcInvestorPositionField Posi in PositionDatas)
            {
                //添加所有合约数据到  common.instrument
                foreach(ThostFtdcInstrumentField instrumentField in GlobalVar.InstrumentAlls.Values)
                {
                    if (instrumentField.InstrumentID.Length <= 6)   //排除 标准套利合约
                    {
                        InsertOnecInstrument(instrumentField);
                    }
                }

                //添加新数据
                string LongPosi = 0.ToString();
                string LongPosi_Today = 0.ToString();
                string LongPosi_YD = 0.ToString();
                string ShortPosi = 0.ToString();
                string ShortPosi_Today = 0.ToString();
                string ShortPosi_YD = 0.ToString();
                string LongPosi_Cost = 0.ToString();
                string ShortPosi_Cost = 0.ToString();
                string LongPosi_Avail = 0.ToString();
                string ShortPosi_Avail = 0.ToString();
                if (Posi.PosiDirection == EnumPosiDirectionType.Long && Posi.Position > 0)
                {
                    LongPosi = Posi.Position.ToString();
                    LongPosi_Today = Posi.TodayPosition.ToString();
                    LongPosi_YD = Posi.YdPosition.ToString();
                    LongPosi_Cost = (Posi.OpenCost / (Posi.Position * GlobalVar.InstrumentAlls[Posi.InstrumentID].VolumeMultiple)).ToString();
                    LongPosi_Avail = (Posi.Position - Posi.LongFrozen).ToString();
                }
                if (Posi.PosiDirection == EnumPosiDirectionType.Short && Posi.Position > 0)
                {
                    ShortPosi = Posi.Position.ToString();
                    ShortPosi_Today = Posi.TodayPosition.ToString();
                    ShortPosi_YD = Posi.YdPosition.ToString();
                    ShortPosi_Cost = (Posi.OpenCost / (Posi.Position * GlobalVar.InstrumentAlls[Posi.InstrumentID].VolumeMultiple)).ToString();
                    ShortPosi_Avail = (Posi.Position - Posi.ShortFrozen).ToString();
                }

                //int ExchangeID = Convert.ToInt32(BaseDA.ExecuteScalar(QuerExchangeID_Sql).ToString());
                string PreNet = GetPREV_NET(AccountID.ToString(), TradingDay, Posi).ToString();   //获取昨日净头寸

                string split_Begin = "'";
                string split_Mid = "','";
                string split_End = "')";

                string InsertSQL = "INSERT INTO portfolio.account_position (`DATE`,`ID`,`SYMBOL`,`LONG`,`LONG_TODAY`,`LONG_YD`,`LONG_COST`,`LONG_AVAIL`,`SHORT`,`SHORT_TODAY`,`SHORT_YD`,`SHORT_COST`,`SHORT_AVAIL`,`PREV_NET`) VALUES (";
                InsertSQL += split_Begin;
                InsertSQL += TradingDay + split_Mid;    //DATE
                InsertSQL += AccountID + split_Mid;
                InsertSQL += Posi.InstrumentID + split_Mid;
                InsertSQL += LongPosi + split_Mid;
                InsertSQL += LongPosi_Today + split_Mid;
                InsertSQL += LongPosi_YD + split_Mid;
                InsertSQL += LongPosi_Cost + split_Mid;
                InsertSQL += LongPosi_Avail + split_Mid;
                InsertSQL += ShortPosi + split_Mid;
                InsertSQL += ShortPosi_Today + split_Mid;
                InsertSQL += ShortPosi_YD + split_Mid;
                InsertSQL += ShortPosi_Cost + split_Mid;
                InsertSQL += ShortPosi_Avail + split_Mid;
                InsertSQL += PreNet + split_End;   // PREV_NET : //昨日净头寸。 上一个交易日的净持仓数量

                string UpdateSQL = "UPDATE portfolio.account_position SET `LONG`=`LONG`+" + LongPosi;
                UpdateSQL += ",`LONG_COST`=`LONG_COST`+" + LongPosi_Cost;
                UpdateSQL += ",`LONG_TODAY`=`LONG_TODAY`+" + LongPosi_Today;
                UpdateSQL += ",`LONG_YD`=`LONG_YD`+" + LongPosi_YD;
                UpdateSQL += ",`LONG_AVAIL`=`LONG_AVAIL`+" + LongPosi_Avail;
                UpdateSQL += ",`SHORT`=`SHORT`+" + ShortPosi;
                UpdateSQL += ",`SHORT_TODAY`=`SHORT_TODAY`+" + ShortPosi_Today;
                UpdateSQL += ",`SHORT_YD`=`SHORT_YD`+" + ShortPosi_YD;
                UpdateSQL += ",`SHORT_COST`=`SHORT_COST`+" + ShortPosi_Cost;
                UpdateSQL += ",`SHORT_AVAIL`=`SHORT_AVAIL`+" + ShortPosi_Avail;
                UpdateSQL += " WHERE `SYMBOL`='" + Posi.InstrumentID + "' AND `ID`='" + AccountID + "' AND `DATE` = '" + TradingDay + "'";

                if (!IsPositionExist(AccountID.ToString(), TradingDay, Posi))
                {
                    Logger.WriteLog_text(InsertSQL);
                    BaseDA.ExecuteNonQuery(InsertSQL);
                }
                else
                { 
                    Logger.WriteLog_text(UpdateSQL);
                    BaseDA.ExecuteNonQuery(UpdateSQL);
                }
                DeleteZeroPosition(TradingDay);
            }
            return PositionDatas.Count;
        }

        public void DeleteZeroPosition(string TradingDay)
        {
            //string CommStr = "DELETE FROM portfolio.account_position WHERE `DATE` = '" + TradingDay + "' AND `LONG`= '0' AND `SHORT` = '0'";
            string CommStr = "DELETE FROM portfolio.account_position WHERE `LONG`= '0' AND `SHORT` = '0'";
            Logger.WriteLog_text(CommStr);
            BaseDA.ExecuteNonQuery(CommStr);
        }

        public bool IsPositionExist(string AccountID, string TradingDay, ThostFtdcInvestorPositionField Posi)
        {
            string CommStr = "SELECT count(*) FROM portfolio.account_position WHERE `SYMBOL`='" + Posi.InstrumentID + "' AND `ID`='" + AccountID + "' AND `DATE` = '" + TradingDay + "'";
            int e = int.Parse(BaseDA.ExecuteScalar(CommStr).ToString());
            if (e > 0)
                return true;
            return false;
        }

        public bool IsAccountMoneyExist(int AccountID, string TradingDay)
        {
            string CommStr = "SELECT count(*) FROM portfolio.account_position WHERE `SYMBOL`= '" + "CNY" + "' AND `ID`= '" + AccountID + "' AND `DATE` = '" + TradingDay + "'";
            int e = int.Parse(BaseDA.ExecuteScalar(CommStr).ToString());
            if (e > 0)
                return true;
            return false;
        }

        public bool IsInstrumentExist(string InstrumentID)
        {
            string CommStr = "SELECT count(*) FROM common.instrument where TICKER='" + InstrumentID + "'";
            int e = int.Parse(BaseDA.ExecuteScalar(CommStr).ToString());
            if (e > 0)
                return true;
            return false;
        }

        public void InsertAllInstrument(List<ThostFtdcInstrumentField> Instrument_List)
        {
            foreach (ThostFtdcInstrumentField Instrument in Instrument_List)
            {
                if (!IsInstrumentExist(Instrument.InstrumentID))
                {
                    InsertOnecInstrument(Instrument);
                }
            }
        }

        public bool InsertOnecInstrument(ThostFtdcInstrumentField Instrument)
        {
            if (!IsInstrumentExist(Instrument.InstrumentID))
            {
                string EXCHANGE = "";
                if(Instrument.ExchangeID == "SHFE")
                    EXCHANGE = "SHF";
                else if (Instrument.ExchangeID == "DCE")
                    EXCHANGE = "DCE";
                else if (Instrument.ExchangeID == "CZCE")
                    EXCHANGE = "ZCE";
                else if (Instrument.ExchangeID == "CFFEX")
                    EXCHANGE = "CFF";
                string QueryExchangeID_Sql = "SELECT `ID` FROM common.exchange WHERE `EXCHANGE`='" + EXCHANGE + "'";
                int ExchangeID = Convert.ToInt32(BaseDA.ExecuteScalar(QueryExchangeID_Sql).ToString());

                //string QueryMaxID_Sql = "select max(ID) from common.instrument";
                //string newID = (Convert.ToInt64(BaseDA.ExecuteScalar(QueryMaxID_Sql))+1).ToString();

                //插入 common.instrument 新合约信息
                string typeid = ((int)InstrumentTypeID.Future).ToString();  // "1";    //InstrumentTypeID.Future;
                if(Instrument.InstrumentID.StartsWith("IF") || Instrument.InstrumentID.StartsWith("IH") || Instrument.InstrumentID.StartsWith("IC"))
                    typeid = ((int)InstrumentTypeID.Index).ToString();
                string markersectorid = ((int)InstrumentMarketSectorID.Comdty).ToString();  // "1"; //InstrumentMarketSectorID.Comdty;
                string round_lot_size = "1";
                string tick_size = "0:" + Instrument.PriceTick.ToString();
                string expire_date = Instrument.ExpireDate.Insert(6, "-").Insert(4, "-");
                string crncy = ((int)CrncyID.CNY).ToString();   // "3"; //CrncyID.CNY;
                string fut_val_pt = Instrument.VolumeMultiple.ToString();
                string split_Begin = "'";
                string split_Mid = "','";
                string split_End = "')";

                string CommStr = "insert into common.instrument (`TICKER`,`NAME`,`EXCHANGE_ID`,`TYPE_ID`, `MARKET_SECTOR_ID`, `ROUND_LOT_SIZE`, `TICK_SIZE_TABLE`,`EXPIRE_DATE`, `CRNCY`, `FUT_VAL_PT` ) values (";
                CommStr += split_Begin;
                //CommStr += newID + split_Mid;
                CommStr += Instrument.InstrumentID + split_Mid;
                CommStr += Instrument.ExchangeID + "." + Instrument.InstrumentID + split_Mid;
                CommStr += ExchangeID + split_Mid;
                CommStr += typeid + split_Mid;
                CommStr += markersectorid + split_Mid;
                CommStr += round_lot_size + split_Mid;
                CommStr += tick_size + split_Mid;
                CommStr += expire_date + split_Mid;
                CommStr += crncy + split_Mid;
                CommStr += fut_val_pt + split_End;
                Logger.WriteLog_text(CommStr);
                BaseDA.ExecuteNonQuery(CommStr);
            }
            return true;
        }

        decimal GetPREV_NET(string AccountID, string TradingDay, ThostFtdcInvestorPositionField Posi) //GetPREV_NET(AccountID.ToString(), TradingDay, Posi).ToString();   //获取昨日净头寸
        {
            string QueryPrePosi_Sql = "SELECT `LONG`,`SHORT` FROM portfolio.account_position WHERE `SYMBOL`='" + Posi.InstrumentID + "' AND `ID`='" + AccountID.ToString() + "' AND `DATE` <> '" + TradingDay + "' ORDER BY `DATE` DESC LIMIT 1";
            DataTable dt = BaseDA.ExecuteDataSet(QueryPrePosi_Sql).Tables[0];
            decimal Net = 0;
            if(dt.Rows.Count >0)
            {
                DataRow row = dt.Rows[0];
                decimal Long = Convert.ToDecimal(row["LONG"]);
                decimal Short = Convert.ToDecimal(row["SHORT"]);
                Net = Long - Short;
            }
            return Net;

        }
    }
}
