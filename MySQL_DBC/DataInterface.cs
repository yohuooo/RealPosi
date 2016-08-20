using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Data;
using MySql.Data.MySqlClient;
using System.Collections;

namespace MySQL_DBC
{     
    /// <summary>
    /// DataInterface 存放可被动态调用的方法，中的动态方法可根据需要修改为适合项目使用的方法。
    /// 如需使用静态方法请直接使用 BaseDA。 
    /// </summary>
    public class DataInterface
    {
        private string lastSqlCommand;

        /// <summary>
        /// 构造函数
        /// </summary>
        public DataInterface()
        {
            //初始化 DBC 连接字符串
            DBConnectionManager.DBConnectionString = Const.SQLConnection;
        }

        /// <summary>
        /// 最后执行的SQL命令
        /// </summary>
        public string LastSqlCommand
        {
            get
            {
                return this.lastSqlCommand;
            }
        }

        /// <summary>
        /// 获取一个数据集
        /// </summary>
        public DataSet GetMultTableDs(string sql)
        {
            this.lastSqlCommand = sql;
            return BaseDA.ExecuteDataSet(sql);
        }

        /// <summary>
        /// 获取多个数据集
        /// </summary>
        public DataSet GetMultTablesDs(string[] sqls, string[] tabnames)
        {
            return BaseDA.ExecuteDataSets(sqls, tabnames);
        }

        /// <summary>
        /// 更新数据集
        /// </summary>
        public  DataSet UpdateDs(DataSet changedDs, string sql, string tableName)
        {
            this.lastSqlCommand = sql;
            return BaseDA.ExecuteUpdateDs(changedDs, sql, tableName);
        }

        /// <summary>
        /// 对数据库执行一个SqlCommand,返回受命令影响的行数Int
        /// </summary>
        public int ExecuteNonQuery(string sql)
        {
            this.lastSqlCommand = sql;
            return BaseDA.ExecuteNonQuery(sql);
        }

        /// <summary>
        /// 读操作获取的数据行数不为0，则返回True
        /// </summary>
        public bool ExecuteCF(string sql)
        {
            this.lastSqlCommand = sql;
            return BaseDA.ExecuteCF(sql);
        }

        /// <summary>
        /// 执行select语句，返回只进的读句柄SqlDataReader
        /// </summary>
        public MySqlDataReader ExecuteReader(string sql)
        {
            this.lastSqlCommand = sql;
            return BaseDA.ExecuteReader(sql);
        }

        /// <summary>
        /// 执行select语句，读取第一行记录的第一列值
        /// <param Name="sql">要执行的SQL语句</param>
        /// <returns>返回object类型数据</returns>
        /// </summary>
        public object ExecuteScalar(string sql)
        {
            this.lastSqlCommand = sql;
            return BaseDA.ExecuteScalar(sql);
        }

        /// <summary>
        /// 返回一个SqlDataAdapter
        /// </summary>
        public MySqlDataAdapter ExecuteAdapter(string sql)
        {
            this.lastSqlCommand = sql;
            return BaseDA.ExecuteAdapter(sql);
        }
    }
}
