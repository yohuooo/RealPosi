using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using MySql.Data.MySqlClient;
using MySql;

//using MySQL_DBC.Common;


namespace MySQL_DBC
{
    public class BaseDA
    {
        public BaseDA()
        {
        }
        /// <summary>
        /// 对数据库执行一个SqlCommand,返回受命令影响的行数Int
        /// </summary>
        public static int ExecuteNonQuery(string SQL)
        {
            DBConnectionManager oDbm = new DBConnectionManager();

            int nRet = DataAccessHelper.ExecuteNonQuery(oDbm.ConnectionString, SQL);
            oDbm.Dispose();
            return nRet;
        }

        /// <summary>
        /// 读操作获取的数据行数不为0，则返回True
        /// </summary>
        public static bool ExecuteCF(string SQL)
        {
            DBConnectionManager oDbm = new DBConnectionManager();

            MySqlDataReader oDr = DataAccessHelper.ExecuteReader(oDbm.ConnectionString, SQL);
            oDbm.Dispose();
            return oDr.HasRows;
        }


        /// <summary>
        /// 获取一个数据集
        /// </summary>
        public static DataSet ExecuteDataSet(string SQL)
        {
            DBConnectionManager oDbm = new DBConnectionManager();

            DataSet oDs = DataAccessHelper.ExecuteDataset(oDbm.ConnectionString, SQL);
            oDbm.Dispose();
            return oDs;
        }

        /// <summary>
        /// 获取多个数据集
        /// </summary>
        public static DataSet ExecuteDataSets(string[] sqls, string[] tabnames)
        {
            DataSet ds = new DataSet();
            MySqlDataAdapter da = new MySqlDataAdapter();
            for (int i = sqls.GetLowerBound(0); i <= sqls.GetUpperBound(0); i++)
            {
                da = BaseDA.ExecuteAdapter(sqls[i]);
                da.Fill(ds, tabnames[i]);
            }
            return ds;
        }

        ///// <summary>
        ///// 批量添加数据。 将DataTable 添加到 tableName 中。
        ///// </summary>
        //public static void ExecuteInsertDt(DataTable dt, string tableName)
        //{
        //    DBConnectionManager oDbm = new DBConnectionManager();

        //    int nRet = DataAccessHelper.ExecuteNonQuery(oDbm.ConnectionString, commandType, commandText, commandParameters);
        //    oDbm.Dispose();
        //    return nRet;
        //}

        /// <summary>
        /// 更新数据集
        /// </summary>
        public static DataSet ExecuteUpdateDs(DataSet changedDs, string sql, string tableName)
        {
            MySqlDataAdapter da = new MySqlDataAdapter();
            da = BaseDA.ExecuteAdapter(sql);
            da.Update(changedDs, tableName);
            changedDs.AcceptChanges();
            return changedDs;//返回更新了的数据库表
        }

        /// <summary>
        /// 执行select语句，返回只进的读句柄SqlDataReader
        /// </summary>
        public static MySqlDataReader ExecuteReader(string SQL)
        {
            DBConnectionManager oDbm = new DBConnectionManager();

            MySqlDataReader oDr = DataAccessHelper.ExecuteReader(oDbm.ConnectionString, SQL);
            oDbm.Dispose();
            return oDr;
        }


        /// <summary>
        /// 执行select语句，读取第一行记录的第一列值
        /// <param Name="sql">要执行的SQL语句</param>
        /// <returns>返回object类型数据</returns>
        /// </summary>
        public static object ExecuteScalar(string sql)
        {
            DBConnectionManager oDbm = new DBConnectionManager();
            object i = DataAccessHelper.ExecuteScalar(oDbm.ConnectionString, sql);
            return i;
        }


        /// <summary>
        /// 对数据库执行一个SqlCommand,返回受命令影响的行数Int
        /// </summary>
        public static int ExecuteNonQuery(CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
        {
            DBConnectionManager oDbm = new DBConnectionManager();

            int nRet = DataAccessHelper.ExecuteNonQuery(
                oDbm.ConnectionString,
                commandType,
                commandText,
                commandParameters);
            oDbm.Dispose();
            return nRet;
        }
        /// <summary>
        /// 执行读操作，返回只进的读句柄SqlDataReader
        /// </summary>
        public static MySqlDataReader ExecuteReader(CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
        {
            DBConnectionManager oDbm = new DBConnectionManager();

            MySqlDataReader oDr = DataAccessHelper.ExecuteReader(
                oDbm.ConnectionString,
                commandType,
                commandText,
                commandParameters);
            oDbm.Dispose();
            return oDr;
        }

        /// <summary>
        /// 获取一个数据集
        /// </summary>
        public static DataSet ExecuteDataSet(CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
        {
            DBConnectionManager oDbm = new DBConnectionManager();

            DataSet oDs = DataAccessHelper.ExecuteDataset(
                oDbm.ConnectionString,
                commandType,
                commandText,
                commandParameters);
            oDbm.Dispose();
            return oDs;
        }
        /// <summary>
        /// 返回一个SqlDataAdapter
        /// </summary>
        public static MySqlDataAdapter ExecuteAdapter(string SQL)
        {
            DBConnectionManager oDbm = new DBConnectionManager();

            MySqlDataAdapter oDa = DataAccessHelper.ExecuteAdapter(oDbm.ConnectionString, SQL);
            //oDa.Dispose();
            return oDa;

        }

    }
}
