using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.Serialization;
using CTP;

namespace RealPosi
{
    public class CTPTrader
    {
        /// <summary>
        /// 会话初始化是否完成 Bool
        /// </summary>
        private bool TdIsInit = false;

        // 会话参数
        public int FRONT_ID;	//前置编号
        public int SESSION_ID;	//会话编号
        public string ORDER_REF;	//报单引用
        int iRequestID = 0;

        public bool ORDER_ACTION_SENT = false;      //是否发送了报单

        /// <summary>
        /// 交易Api
        /// </summary>
        CTPTraderAdapter TraderApi = null;

        //当前帐号
        public Account AccountID = null;
        public ThostFtdcReqUserLoginField User = new ThostFtdcReqUserLoginField { UserProductInfo = "MyTrader" };

        /// <summary>
        /// 合约集合
        /// </summary>
        //public SortedList<string, ThostFtdcInstrumentField> InstrumentAlls = new SortedList<string, ThostFtdcInstrumentField>(0);

        /// <summary>
        /// 持仓数据
        /// </summary>
        public List<ThostFtdcInvestorPositionField> PositionList = new List<ThostFtdcInvestorPositionField>();

        /// <summary>
        /// 帐号资金
        /// </summary>
        public ThostFtdcTradingAccountField TradingAccount = new ThostFtdcTradingAccountField();

        public string TradingDay = "";

        //private AutoResetEvent StopEvent = new AutoResetEvent(false);       //控制阻塞的类实例。
        bool bTaskBlock = true;
        int blockCount = 0;
        public bool bTaskComplete = false;  //任务是否正常完成

        #region 初始化过程
        /// <summary>
        ///  初始化API
        /// </summary>
        public void Init()
        {
            if (TraderApi == null)
            {
                TraderApi = new CTPTraderAdapter();
                try
                {
                    //错误提示
                    TraderApi.OnHeartBeatWarning += new HeartBeatWarning(OnHeartBeatWarning);
                    TraderApi.OnRspError += new RspError(OnRspError);
                    TraderApi.OnFrontDisconnected += new FrontDisconnected(OnFrontDisconnected);

                    //登录
                    TraderApi.OnFrontConnected += new FrontConnected(OnFrontConnected);
                    TraderApi.OnRspUserLogin += new RspUserLogin(OnRspTraderLogin);
                    TraderApi.OnRspUserLogout += new RspUserLogout(OnRspTraderLogout);
                    TraderApi.OnRspSettlementInfoConfirm += new RspSettlementInfoConfirm(OnRspSettlementInfoConfirm);
                    TraderApi.OnRspQryInstrument += new RspQryInstrument(OnRspQryInstrument);
                    TraderApi.OnRspQryInvestorPosition += new RspQryInvestorPosition(OnRspQryInvestorPosition);
                    TraderApi.OnRspQryTradingAccount += new RspQryTradingAccount(OnRspQryTradingAccount);

                    //TraderApi.OnRspQryInstrumentCommissionRate += new RspQryInstrumentCommissionRate(OnRspQryInstrumentCommissionRate);
                    //TraderApi.OnRspQryInstrumentMarginRate += new RspQryInstrumentMarginRate(OnRspQryInstrumentMarginRate);
                    //TraderApi.OnRtnOrder += new RtnOrder(OnRtnOrder);
                    //TraderApi.OnRtnTrade += new RtnTrade(OnRtnTrade);
                    //TraderApi.OnRspOrderAction += new RspOrderAction(OnRspOrderAction);
                    //TraderApi.OnRspOrderInsert += new RspOrderInsert(OnRspOrderInsert);                  

                    // 注册一事件处理的实例
                    //m_pTdApi->RegisterSpi(this);

                    // 订阅私有流
                    //        TERT_RESTART:从本交易日开始重传
                    //        TERT_RESUME:从上次收到的续传
                    //        TERT_QUICK:只传送登录后私有流的内容
                    TraderApi.SubscribePrivateTopic(EnumTeResumeType.THOST_TERT_RESTART);					// 注册私有流
                    // 订阅公共流
                    //        TERT_RESTART:从本交易日开始重传
                    //        TERT_RESUME:从上次收到的续传
                    //        TERT_QUICK:只传送登录后公共流的内容
                    TraderApi.SubscribePublicTopic(EnumTeResumeType.THOST_TERT_RESTART);					// 注册公有流                    

                    TraderApi.RegisterFront(AccountID.TradeFrontAddress);
                    TraderApi.Init();
                    //TraderApi.Join();
                    while(bTaskBlock)
                    {
                        Thread.Sleep(10);
                        blockCount++;
                        if (blockCount >= 1000)  //最多阻塞 10*1000 = 10 秒
                            break;
                    }
                    //StopEvent.WaitOne();
                    {
                        ReqTraderLogout();
                        TraderApi.OnFrontConnected -= new FrontConnected(OnFrontConnected);
                        TraderApi.OnFrontDisconnected -= new FrontDisconnected(OnFrontDisconnected);
                        TraderApi.OnHeartBeatWarning -= new HeartBeatWarning(OnHeartBeatWarning);
                        TraderApi.OnRspError -= new RspError(OnRspError);
                        TraderApi.OnRspUserLogin -= new RspUserLogin(OnRspTraderLogin);
                        TraderApi.OnRspUserLogout -= new RspUserLogout(OnRspTraderLogout);
                        TraderApi.OnRspSettlementInfoConfirm -= new RspSettlementInfoConfirm(OnRspSettlementInfoConfirm);
                        TraderApi.OnRspQryInstrument -= new RspQryInstrument(OnRspQryInstrument);
                        TraderApi.OnRspQryInvestorPosition -= new RspQryInvestorPosition(OnRspQryInvestorPosition);
                        TraderApi.OnRspQryTradingAccount -= new RspQryTradingAccount(OnRspQryTradingAccount);
                        TraderApi.Dispose();
                    }                    
                }
                catch (Exception ex)
                {
                    Logger.WriteLog_text("--->>> Init()异常：" + ex.Message);
                }

            }
        }
        #endregion



        #region 请求发送方法
        /// <summary>
        /// 交易用户登录
        /// </summary>
        /// <returns></returns>
        bool ReqTraderLogin()
        {
            bool isLogin = false;
            User.BrokerID = AccountID.BrokerID;
            User.UserID = AccountID.UserID;
            User.Password = AccountID.Password;
            int result = TraderApi.ReqUserLogin(User, ++iRequestID);
            if (result == 0) isLogin = true;
            String msg = "--->>> 发送用户登录请求: " + ((result == 0) ? "成功" : "失败");
            Logger.WriteLog_text(msg);
            Debug.WriteLine(msg);
            return isLogin;
        }

        bool ReqTraderLogout()
        {
            bool isLogout = false;
            ThostFtdcUserLogoutField pUserLogout = new ThostFtdcUserLogoutField();
            pUserLogout.BrokerID = AccountID.BrokerID;
            pUserLogout.UserID = AccountID.UserID;
            int result = TraderApi.ReqUserLogout(pUserLogout, ++iRequestID);
            if (result == 0) isLogout = true;
            return isLogout;
        }

        /// <summary>
        /// 资金查询
        /// </summary>
        public void ReqQryTradingAccount()
        {
            ThostFtdcQryTradingAccountField accountField = new ThostFtdcQryTradingAccountField();
            accountField.BrokerID = AccountID.BrokerID;
            accountField.InvestorID = "";
            int r = TraderApi.ReqQryTradingAccount(accountField,++iRequestID);
            if (r == 0)
                Console.WriteLine("发送资金查询。");
            else
                Console.WriteLine("发送资金查询失败！");
        }

        /// <summary>
        /// 持仓查询
        /// </summary>
        public void ReqQryInvestorPosition()
        {
            ThostFtdcQryInvestorPositionField positionField = new ThostFtdcQryInvestorPositionField();
            positionField.BrokerID = AccountID.BrokerID;
            positionField.InvestorID = AccountID.UserID;
            positionField.InstrumentID = "";
            //查询持仓
            int r = TraderApi.ReqQryInvestorPosition(positionField, ++iRequestID);
            if (r == 0)
            {
                PositionList.Clear();
                Logger.WriteLog_text("--->>> 发送持仓查询成功。");
            }
            else
                Logger.WriteLog_text("--->>> 发送持仓查询失败！");
        }

        /// <summary>
        /// 所有合约信息查询
        /// </summary>
        public void ReqQryInstrument()
        {
            ThostFtdcQryInstrumentField instrumentField = new ThostFtdcQryInstrumentField();
            instrumentField.ExchangeID = String.Empty;
            instrumentField.ExchangeInstID = String.Empty;
            instrumentField.InstrumentID = String.Empty;
            instrumentField.ProductID = String.Empty;
            int r = TraderApi.ReqQryInstrument(instrumentField, ++iRequestID);
            if (r == 0)
            {
                Logger.WriteLog_text("--->>> 发送合约信息查询成功。");
                GlobalVar.InstrumentAlls.Clear();
            }
            else
                Logger.WriteLog_text("--->>> 发送合约信息查询失败！");
        }


        /// <summary>
        /// 下单
        /// 方法：ReqOrderInsert("150194", 0.999, 7800, "Buy");
        /// </summary>
        void ReqOrderInsert(string instrumentID, double price, int volume, string direction, int requestID)
        {
            ThostFtdcInputOrderField req = new ThostFtdcInputOrderField();	//交易请求消息体 req
            ///经纪公司代码
            req.BrokerID = AccountID.BrokerID;
            ///投资者代码
            req.InvestorID = req.UserID = AccountID.UserID;
            ///合约代码
            req.InstrumentID = instrumentID;															//合约代码
            ///报单引用
            req.OrderRef = ORDER_REF;
            ///用户代码
            //	TThostFtdcUserIDType	UserID;		//不需要
            ///报单价格条件: 限价
            req.OrderPriceType = CTP.EnumOrderPriceTypeType.LimitPrice;		// 选择:限价单（市价单）
            ///买卖方向: 
            if (direction == "Buy")
                req.Direction = EnumDirectionType.Buy;																		//买卖方向
            else if (direction == "Sell")
                req.Direction = EnumDirectionType.Sell;

            ///组合开平标志: 开平仓
            req.CombOffsetFlag_0 = CTP.EnumOffsetFlagType.Open;						///开平标志: 开仓

            req.RequestID = requestID;

            ///组合投机套保标志
            req.CombHedgeFlag_0 = CTP.EnumHedgeFlagType.Speculation;            ///投机
                                                                                ///价格
            req.LimitPrice = price;                                                                 ///请求价格
                                                                                                    ///数量: 1
            req.VolumeTotalOriginal = 1;                                                                    ///请求数量
                                                                                                            ///有效期类型: 当日有效
            req.TimeCondition = CTP.EnumTimeConditionType.GFD;
            ///GTD日期
            //	TThostFtdcDateType	GTDDate;
            ///成交量类型: 任何数量
            req.VolumeCondition = CTP.EnumVolumeConditionType.AV;
            ///最小成交量: 1
            req.MinVolume = 1;
            ///触发条件: 立即
            req.ContingentCondition = CTP.EnumContingentConditionType.Immediately;
            ///止损价
            //	TThostFtdcPriceType	StopPrice;
            ///强平原因: 非强平
            req.ForceCloseReason = CTP.EnumForceCloseReasonType.NotForceClose;
            ///自动挂起标志: 否
            req.IsAutoSuspend = 0;
            ///业务单元
            //	TThostFtdcBusinessUnitType	BusinessUnit;
            ///请求编号
            //	TThostFtdcRequestIDType	RequestID;
            ///用户强评标志: 否
            req.UserForceClose = 0;

            int nRes = TraderApi.ReqOrderInsert(req, requestID);
            string msg = string.Format("报单发送{0}", nRes == 0 ? "成功" : "失败");
            Debug.WriteLine(msg);
            //MessageBox.Show(msg);
            Console.WriteLine(msg);
        }

        /// <summary>
        /// 撤单、取消报单
        /// </summary>
        /// <param name="order"></param>
        void ReqOrderCancel(ThostFtdcOrderField pOrder)
        {

            if (ORDER_ACTION_SENT)
                return;

            ThostFtdcInputOrderActionField req = new ThostFtdcInputOrderActionField();
            ///经纪公司代码
            req.BrokerID = pOrder.BrokerID;
            ///投资者代码
            req.InvestorID = pOrder.InvestorID;
            ///报单操作引用
            //	TThostFtdcOrderActionRefType	OrderActionRef;
            ///报单引用
            req.OrderRef = pOrder.OrderRef;
            ///请求编号
            //	TThostFtdcRequestIDType	RequestID;
            ///前置编号
            req.FrontID = FRONT_ID;
            ///会话编号
            req.SessionID = SESSION_ID;
            ///交易所代码
            //	TThostFtdcExchangeIDType	ExchangeID;
            ///报单编号
            //	TThostFtdcOrderSysIDType	OrderSysID;
            ///操作标志
            req.ActionFlag = CTP.EnumActionFlagType.Delete;
            ///价格
            //	TThostFtdcPriceType	LimitPrice;
            ///数量变化
            //	TThostFtdcVolumeType	VolumeChange;
            ///用户代码
            //	TThostFtdcUserIDType	UserID;
            ///合约代码
            req.InstrumentID = pOrder.InstrumentID;

            //CancelOrder(field);
            int nReqID = ++iRequestID;
            req.RequestID = nReqID;
            int nRes = TraderApi.ReqOrderAction(req, ++iRequestID);
        }

        #endregion

        #region 回调函数
        /// <summary>
        /// 连接回调函数
        /// </summary>
        public void OnFrontConnected()
        {
            Logger.WriteLog_text("--->>> CTP 交易前置机连接成功！");
            Thread.Sleep(100);
            //提交用户登录
            ReqTraderLogin();
        }

        void OnFrontDisconnected(int nReason)
        {
            DebugPrintFunc(new StackTrace());
            Console.WriteLine("--->>> Reason = {0}", nReason);
        }

        void OnRspTraderLogout(ThostFtdcUserLogoutField pUserLogout, ThostFtdcRspInfoField pRspInfo, int nRequestID, bool bIsLast)
        {
            Logger.WriteLog_text(DateTime.Now + "--->>> CTP 用户已退出！");
        }

        /// <summary>
        /// 登录回调
        /// </summary>
        void OnRspTraderLogin(ThostFtdcRspUserLoginField pRspUserLogin, ThostFtdcRspInfoField pRspInfo, int nRequestID, bool bIsLast)
        {
            DebugPrintFunc(new StackTrace());
            if (bIsLast && !IsErrorRspInfo(pRspInfo))
            {
                // 保存会话参数
                FRONT_ID = pRspUserLogin.FrontID;
                SESSION_ID = pRspUserLogin.SessionID;
                int iNextOrderRef = 0;
                if (!string.IsNullOrEmpty(pRspUserLogin.MaxOrderRef))
                    iNextOrderRef = Convert.ToInt32(pRspUserLogin.MaxOrderRef);
                iNextOrderRef++;
                ORDER_REF = Convert.ToString(iNextOrderRef);

                ///获取当前交易日,说明登录成功了
                //String msg = "\n--->>> 获取当前交易日 = " + TraderApi.GetTradingDay();
                TradingDay = TraderApi.GetTradingDay();
                TradingDay = TradingDay.Insert(6, "-").Insert(4, "-");
                Logger.WriteLog_text("--->>> CTP 交易账号登录成功！");
                Logger.WriteLog_text("--->>> 获取当前交易日 = " + TradingDay);

                //请求投资确认
                Thread.Sleep(1500);
                //ReqSettlementInfoConfirm();
                ReqQryInstrument();
            }
            else
            {
                Logger.WriteLog_text("--->>> CTP 登录失败：账号或者密码错误！");
                Debug.WriteLine(pRspInfo.ErrorMsg);
            }
        }

        /// <summary>
        /// 所有合约信息查询回调
        /// </summary>
        void OnRspQryInstrument(ThostFtdcInstrumentField pInstrument, ThostFtdcRspInfoField pRspInfo, int nRequestID, bool bIsLast)
        {
            if (pInstrument != null)
            {
                if (!GlobalVar.InstrumentAlls.ContainsKey(pInstrument.InstrumentID))
                    GlobalVar.InstrumentAlls.Add(pInstrument.InstrumentID, pInstrument);
            }
            //合约查询完毕
            if (bIsLast)
            {
                //Console.WriteLine("CTP 所有合约信息查询完毕。 市场合约数： " + GlobalVar.InstrumentAlls.Count);
                Thread.Sleep(1500);
                ReqQryTradingAccount();
                
            }
        }

        /// <summary>
        /// 资金账号回调
        /// </summary>
        void OnRspQryTradingAccount(ThostFtdcTradingAccountField pTradingAccount, ThostFtdcRspInfoField pRspInfo, int nRequestID, bool bIsLast)
        {
            if (pTradingAccount != null)
            {
                TradingAccount = pTradingAccount;
                if (bIsLast)
                {
                    Console.WriteLine("CTP 资金信息查询完毕！");
                    Thread.Sleep(1500);
                    ReqQryInvestorPosition();
                }
            }
        }


        /// <summary>
        /// 持仓信息回调
        /// </summary>
        void OnRspQryInvestorPosition(ThostFtdcInvestorPositionField pInvestorPosition, ThostFtdcRspInfoField pRspInfo, int nRequestID, bool bIsLast)
        {
            if (pInvestorPosition != null)
            {
                if (!PositionList.Contains(pInvestorPosition))
                    PositionList.Add(pInvestorPosition);
            }
            if (bIsLast)
            {
                //StopEvent.Set();
                bTaskBlock = false;
                bTaskComplete = true;
            }
        }

        /// <summary>
        /// 报单回调
        /// </summary>
        /// <param name="pInputOrder"></param>
        /// <param name="pRspInfo"></param>
        /// <param name="nRequestID"></param>
        /// <param name="bIsLast"></param>
        void OnRspOrderInsert(ThostFtdcInputOrderField pInputOrder, ThostFtdcRspInfoField pRspInfo, int nRequestID, bool bIsLast)
        {
            Console.WriteLine("OnRspOrderInsert回调");
            if (pInputOrder != null)
            {
                if (bIsLast)
                {
                    Debug.WriteLine(pRspInfo.ErrorMsg);
                }
            }
        }

        /// <summary>
        /// 撤单回报
        /// </summary>
        void OnRspOrderAction(ThostFtdcInputOrderActionField pInputOrderAction, ThostFtdcRspInfoField pRspInfo, int nRequestID, bool bIsLast)
        {
            if (bIsLast && !IsErrorRspInfo(pRspInfo))
            {
                if (pRspInfo.ErrorMsg == "CTP:报单已全成交或已撤销，不能再撤")
                {

                }
            }
        }

        /// <summary>
        /// 委托交易情况回报（交易所回调信息）
        /// </summary>
        void OnRtnOrder(ThostFtdcOrderField pOrder)
        {
            if (pOrder == null) return;
            Debug.WriteLine("交易所报单回报 " + pOrder.OrderLocalID);
        }

        /// <summary>
        /// 成交回报
        /// </summary>
        void OnRtnTrade(ThostFtdcTradeField pTrade)
        {
            if (pTrade == null) return;
            Debug.WriteLine("成交回报 " + pTrade.OrderLocalID);
        }

        /// <summary>
        /// 发生错误
        /// </summary>
        void OnRspError(ThostFtdcRspInfoField pRspInfo, int nRequestID, bool bIsLast)
        {
            DebugPrintFunc(new StackTrace());
            IsErrorRspInfo(pRspInfo);
            //Console.WriteLine("OnRspError发生错误：" + pRspInfo.ErrorMsg);
            Logger.WriteLog_text("--->>> OnRspError发生错误：" + pRspInfo.ErrorMsg);
            MyEmail.DefualtSendMail("--->>> OnRspError发生错误：" + pRspInfo.ErrorMsg);
            Logger.WriteLog_text("--->>> 2秒后，重新执行 ReqQryInstrument()");
            Thread.Sleep(2000);
            ReqQryInstrument();
        }

        void OnHeartBeatWarning(int nTimeLapse)
        {
            DebugPrintFunc(new StackTrace());
            Debug.WriteLine("--->>> nTimerLapse = " + nTimeLapse);
        }

        #endregion

        /// <summary>
        /// 请求结算结果确认
        /// </summary>
        void ReqSettlementInfoConfirm()
        {
            ThostFtdcSettlementInfoConfirmField req = new ThostFtdcSettlementInfoConfirmField();
            req.BrokerID = User.BrokerID;
            req.InvestorID = User.UserID;
            int iResult = TraderApi.ReqSettlementInfoConfirm(req, ++iRequestID);
            Logger.WriteLog_text("--->>> 请求投资者结算结果确认: " + ((iResult == 0) ? "成功" : "失败"));
        }

        /// <summary>
        /// 回调结算结果确认
        /// </summary>
        void OnRspSettlementInfoConfirm(ThostFtdcSettlementInfoConfirmField pSettlementInfoConfirm, ThostFtdcRspInfoField pRspInfo, int nRequestID, bool bIsLast)
        {
            Console.WriteLine("--->>> OnRspSettlementInfoConfirm");
            DebugPrintFunc(new StackTrace());
            if (bIsLast && !IsErrorRspInfo(pRspInfo))
            {
                Logger.WriteLog_text("--->>> 投资者结算结果确认成功");
                TdIsInit = true;
                Thread.Sleep(1500);
                //查询所有合约信息
                ReqQryInstrument();
            }
        }

        /// <summary>
        /// 请求查询合约手续费率响应
        /// </summary>
        void ReqQryInstrumentCommissionRate(string Instrument_ID)
        {
            ThostFtdcQryInstrumentCommissionRateField req = new ThostFtdcQryInstrumentCommissionRateField();
            req.BrokerID = User.BrokerID;
            req.InvestorID = User.UserID;
            req.InstrumentID = Instrument_ID;
            int iResult = TraderApi.ReqQryInstrumentCommissionRate(req, ++iRequestID);
            Console.WriteLine("--->>> 请求查询合约手续费率响应: " + ((iResult == 0) ? "成功" : "失败"));
        }

        /// <summary>
        /// 回调：请求查询合约手续费率响应
        /// </summary>
        void OnRspQryInstrumentCommissionRate(ThostFtdcInstrumentCommissionRateField pInstrumentCommissionRate, ThostFtdcRspInfoField pRspInfo, int nRequestID, bool bIsLast)
        {
            if (bIsLast && !IsErrorRspInfo(pRspInfo))
            {
                //请求查询投资者持仓
                Console.WriteLine("--->>> 回调：请求查询合约手续费率响应");
                if (pInstrumentCommissionRate == null)
                    Console.WriteLine("--->>> pInstrumentCommissionRate == null");
            }
        }

        /// <summary>
        /// 请求查询合约保证金率响应
        /// </summary>
        void ReqQryInstrumentMarginRate(string Instrument_ID)
        {
            ThostFtdcQryInstrumentMarginRateField req = new ThostFtdcQryInstrumentMarginRateField();
            req.BrokerID = User.BrokerID;
            req.InvestorID = User.UserID;
            req.InstrumentID = Instrument_ID;
            req.HedgeFlag = EnumHedgeFlagType.Speculation;
            int iResult = TraderApi.ReqQryInstrumentMarginRate(req, ++iRequestID);
            Console.WriteLine("--->>> 请求查询合约保证金: " + ((iResult == 0) ? "成功" : "失败"));
        }
        /// <summary>
        /// 回调：请求查询合约保证金率响应
        /// </summary>
        void OnRspQryInstrumentMarginRate(ThostFtdcInstrumentMarginRateField pInstrumentMarginRate, ThostFtdcRspInfoField pRspInfo, int nRequestID, bool bIsLast)
        {
            if (bIsLast && !IsErrorRspInfo(pRspInfo))
            {
                //请求查询投资者持仓
                Console.WriteLine("--->>> 回调：请求查询合约保证金率响应");
                if (pInstrumentMarginRate == null)
                    Console.WriteLine("--->>> pInstrumentMarginRate == null");
            }
        }


        public static bool IsErrorRspInfo(ThostFtdcRspInfoField pRspInfo)
        {
            // 如果ErrorID != 0, 说明收到了错误的响应
            bool bResult = ((pRspInfo != null) && (pRspInfo.ErrorID != 0));
            if (bResult)
            {
                String msg = "\n--->>> ErrorID=" + pRspInfo.ErrorID + ", ErrorMsg=" + pRspInfo.ErrorMsg;

                Debug.WriteLine(msg);
            }
            return bResult;
        }
        void DebugPrintFunc(StackTrace stkTrace)
        {
            string s = stkTrace.GetFrame(0).ToString();
            s = s.Split(new char[] { ' ' })[0];
            Debug.WriteLine("\n\n--->>> " + DateTime.Now + "    ===========    " + s);

        }
    }
}
