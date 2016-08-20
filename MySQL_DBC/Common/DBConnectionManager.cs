using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Xml;
using MySql.Data.MySqlClient;
using System.Collections;
using System.Configuration;


namespace MySQL_DBC
{
    public class DBConnectionManager : IDisposable
    {

        #region Privates

        private String _innerTransName = "innerTransaction";
        //#if(Orcle)
        // private OracleConnection _conn = null;
        //#else
        private MySqlConnection _conn = null;
        //#endif
        //private OracleTransaction _trans = null;
        //private System.Data.SqlClient.SqlTransaction _trans = null;
        private MySql.Data.MySqlClient.MySqlTransaction _trans = null;
        private bool _disposed = false;
        //public static readonly string DBConnectionString = ConfigurationSettings.AppSettings["strConnection"];
        public static string DBConnectionString = Const.SQLConnection;
        //警告	3	“System.Configuration.ConfigurationSettings.AppSettings”已过时:“This method is obsolete, it has been replaced by System.Configuration!System.Configuration.ConfigurationManager.AppSettings”	C:\Users\clliu\Documents\Visual Studio 2010\Projects\MySQL_DBC\DB.Common\DBConnectionManager.cs	29	60	DB.Common
        //public static readonly string DBConnectionString = ConfigurationManager.AppSettings["strConnection"];

        #endregion

        #region Constructor

        /// <summary>
        /// Overloaded. Initializes a new instance of the DBConnectionManager class.
        /// The Constructor will use 
        /// the default connection which is read from config file.
        /// </summary>
        public DBConnectionManager()
            : this(DBConnectionString)
        {
            this._innerTransName = "PM" + DateTime.Now.Ticks.ToString();
        }

        /// <summary>
        /// Overloaded. Initializes a new instance of the DBConnectionManager class.
        /// </summary>
        /// <param Name="connectionString">The connection string used to open the Oracle Server database.</param>
        public DBConnectionManager(string connectionString)
        {
            _conn = new MySqlConnection(connectionString);
            this._innerTransName = "PM" + DateTime.Now.Ticks.ToString();
        }

        #endregion

        #region Properties

        /// <summary>
        /// Gets the string used to open a Oracle Server database.
        /// </summary>
        public String ConnectionString
        {
            get { return DBConnectionString; }
        }

        #endregion

        #region Open and Close

        /// <summary>
        /// Opens a database connection.
        /// </summary>
        private void Open()
        {
            if (_conn != null && _conn.State != ConnectionState.Open)
            {
                _conn.Open();
            }
        }

        /// <summary>
        /// Closes the connection to the database. 
        /// This is the preferred method of closing any open connection.
        /// </summary>
        public void Close()
        {
            if (_conn != null)
            {
                if (_conn.State == ConnectionState.Open)
                    _conn.Close();
            }
        }

        #endregion

        #region Dispose and Finalize

        /// <summary>
        /// Overridden. Releases the resources used by the DBConnectionManager.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Overridden. Releases the all resources used by the DBConnectionManager before the Component is reclaimed by garbage collection. 
        /// </summary>
        //protected void Finalize()
        //{
        //    Dispose(true);
        //}

        //dispose object
        private void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (_trans != null)
                {
                    _trans.Dispose();
                    _trans = null;
                }
                if (_conn != null)
                {
                    _conn.Close();
                    _conn.Dispose();
                    _conn = null;
                }
                _disposed = true;
            }
        }

        #endregion

        #region Transaction

        /// <summary>
        ///  Begins a database transaction.
        /// </summary>
        public /*OracleTransaction*/MySqlTransaction BeginTransaction()
        {
            Open();
            if (_trans == null)
                _trans = _conn.BeginTransaction(IsolationLevel.ReadUncommitted);//(IsolationLevel.ReadUncommitted, _innerTransName);
            return _trans;
        }

        /// <summary>
        ///  Commits the database transaction.
        /// </summary>
        public void CommitTransaction()
        {
            if ((_conn.State == ConnectionState.Open) && (_trans != null))
            {
                _trans.Commit();
            }
        }

        /// <summary>
        ///  Rolls back a transaction from a pending state.
        /// </summary>
        public void RollbackTransaction()
        {
            if ((_conn.State == ConnectionState.Open) && (_trans != null))
            {
                _trans.Rollback();//Rollback(_innerTransName);
            }
        }

        #endregion

        #region other Methods

        /// <summary>
        /// Gets a database connection object.
        /// </summary>
        /// <returns>a connection object.</returns>
        //		public OracleConnection GetConnection()
        //		{
        //			return _conn;
        //		}
        public MySql.Data.MySqlClient.MySqlConnection GetConnection()
        {
            return _conn;
        }

        /// <summary>
        ///   format the Oracle string.
        /// </summary>
        /// <param Name="OracleString">a string.</param>
        /// <returns>a string.</returns>
        public static string FormatOracleString(string OracleString)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            strBuilder.Append(OracleString).Replace("'", "''").Insert(0, '\'').Append('\'');
            return strBuilder.ToString();
        }
        public static string FormartSQLServerString(string SqlServerString)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            strBuilder.Append(SqlServerString).Replace("'", "''").Insert(0, '\'').Append('\'');
            return strBuilder.ToString();
        }
        #endregion

    }
}
