using System;
using System.Data;
using System.Xml;
using MySql.Data.MySqlClient;
using System.Collections;

namespace MySQL_DBC
{
    public sealed class DataAccessHelper
    {
        #region private utility methods & constructors

        //Since this class provides only static methods, make the default constructor private to prevent 
        //instances from being created with "new DataAccessHelper()".
        private DataAccessHelper() { }



        /// <summary>
        /// This method is used to attach array of SqlParameters to a SqlCommand.
        /// 
        /// This method will assign a Value of DbNull to any parameter with a direction of
        /// InputOutput and a Value of null.  
        /// 
        /// This behavior will prevent default values from being used, but
        /// this will be the less common case than an intended pure output parameter (derived as InputOutput)
        /// where the user provided no input Value.
        /// </summary>
        /// <param Name="command">The command to which the parameters will be added</param>
        /// <param Name="commandParameters">an array of SqlParameters tho be added to command</param>
        private static void AttachParameters(MySqlCommand/*SqlCommand*/ command, MySqlParameter[]/*SqlParameter[]*/ commandParameters)
        {
            foreach (MySqlParameter p in commandParameters)
            //foreach (SqlParameter p in commandParameters)  //modify by hqs 2010-06-26
            {
                //check for derived output Value with no Value assigned
                if ((p.Direction == ParameterDirection.InputOutput) && (p.Value == null))
                {
                    p.Value = DBNull.Value;
                }

                command.Parameters.Add(p);
            }
        }

        /// <summary>
        /// This method assigns an array of values to an array of SqlParameters.
        /// </summary>
        /// <param Name="commandParameters">array of SqlParameters to be assigned values</param>
        /// <param Name="parameterValues">array of objects holding the values to be assigned</param>
        private static void AssignParameterValues(MySqlParameter[]/*SqlParameter[]*/ commandParameters, object[] parameterValues)
        {
            if ((commandParameters == null) || (parameterValues == null))
            {
                //do nothing if we get no data
                return;
            }

            // we must have the same number of values as we pave parameters to put them in
            if (commandParameters.Length != parameterValues.Length)
            {
                throw new ArgumentException("Parameter count does not match Parameter Value count.");
            }

            //iterate through the SqlParameters, assigning the values from the corresponding position in the 
            //Value array
            int str = 0;
            string stra = "";
            for (int i = 0, j = commandParameters.Length; i < j; i++)
            {
                commandParameters[i].Value = parameterValues[i];

                stra = parameterValues[i].ToString();
                stra = commandParameters[i].Value.ToString();
                str = Convert.ToInt32(commandParameters[i].Value.ToString());

            }
        }

        /// <summary>
        /// This method opens (if necessary) and assigns a connection, transaction, command Type and parameters 
        /// to the provided command.
        /// </summary>
        /// <param Name="command">the SqlCommand to be prepared</param>
        /// <param Name="connection">a valid SqlConnection, on which to execute this command</param>
        /// <param Name="transaction">a valid SqlTransaction, or 'null'</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">an array of SqlParameters to be associated with the command or 'null' if no parameters are required</param>
        private static void PrepareCommand(MySqlCommand/*SqlCommand*/ command, MySqlConnection/*SqlConnection*/ connection,
                        MySqlTransaction/*SqlTransaction*/ transaction, CommandType commandType, string commandText, MySqlParameter[]/*SqlParameter[]*/ commandParameters)
        {
            //if the provided connection is not open, we will open it
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }

            //associate the connection with the command
            command.Connection = connection;

            //set the command text (stored procedure Name or Sql statement)
            command.CommandText = commandText;

            //if we were provided a transaction, assign it.
            if (transaction != null)
            {
                command.Transaction = transaction;
            }

            //set the command Type
            command.CommandType = commandType;

            //attach the command parameters if they are provided
            if (commandParameters != null)
            {
                AttachParameters(command, commandParameters);
            }

            return;
        }

        /// <summary>
        /// This method opens (if necessary) and assigns a connection, transaction, command Type and parameters 
        /// to the provided command.
        /// </summary>
        /// <param Name="command">the SqlCommand to be prepared</param>
        /// <param Name="connection">a valid SqlConnection, on which to execute this command</param>
        /// <param Name="transaction">a valid SqlTransaction, or 'null'</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">a collection of SqlParameters to be associated with the command or 'null' if no parameters are required</param>
        private static void PrepareCommand(MySqlCommand/*SqlCommand*/ command,
                                           MySqlConnection/*SqlConnection*/ connection,
                                           MySqlTransaction/*SqlTransaction*/ transaction, CommandType commandType, string commandText,
                                           MySqlParameterCollection/*SqlParameterCollection*/ commandParameters)
        {
            //if the provided connection is not open, we will open it
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }

            //associate the connection with the command
            command.Connection = connection;

            //set the command text (stored procedure Name or Sql statement)
            command.CommandText = commandText;

            //if we were provided a transaction, assign it.
            if (transaction != null)
            {
                command.Transaction = transaction;
            }

            //set the command Type
            command.CommandType = commandType;

            //attach the command parameters if they are provided
            if (commandParameters != null)
            {
                //detach the SqlParameters from the command object
                command.Parameters.Clear();

                object[] arr = new Object[commandParameters.Count];
                commandParameters.CopyTo(arr, 0);

                //detach the SqlParameters from commandParameters.
                commandParameters.Clear();
                //attach the command parameters to the current Command to be prepared.
                foreach (MySqlParameter para in arr)
                {
                    command.Parameters.Add(para);
                }
            }

            return;
        }


        #endregion private utility methods & constructors

        #region ExecuteNonQuery

        /// <summary>
        /// Execute a SqlCommand (that returns no resultset and takes no parameters) against the database specified in 
        /// the connection string. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int result = ExecuteNonQuery(connString, "select * from Orders");
        /// </remarks>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="SqlString">the stored procedure Name or T-Sql command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(string connectionString, string SqlString)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteNonQuery(connectionString, CommandType.Text, SqlString,
                                   (MySqlParameter[]/*SqlParameter[]*/)null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns no resultset and takes no parameters) against the database specified in 
        /// the connection string. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int result = ExecuteNonQuery(connString, CommandType.StoredProcedure, "PublishOrders");
        /// </remarks>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(string connectionString, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteNonQuery(connectionString, commandType, commandText,
                (MySqlParameter[]/*SqlParameter[]*/)null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns no resultset) against the database specified in the connection string 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int result = ExecuteNonQuery(connString, CommandType.StoredProcedure, "PublishOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(string connectionString, CommandType commandType, string commandText,
            params MySqlParameter[]/*SqlParameter[]*/ commandParameters)
        {
            //create & open a SqlConnection, and dispose of it after we are done.
            using (MySqlConnection cn = new MySqlConnection(connectionString))
            //using (SqlConnection cn = new SqlConnection(connectionString))
            {
                cn.Open();

                //call the overload that takes a connection in place of the connection string
                return ExecuteNonQuery(cn, commandType, commandText, commandParameters);
            }
        }

        /// <summary>
        /// Execute a SqlCommand (that returns no resultset) against the database specified in the connection string 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int result = ExecuteNonQuery(connString, CommandType.StoredProcedure, "PublishOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">a collection of SqlParamters used to execute the command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(string connectionString, CommandType commandType, string commandText,
                                          MySqlParameterCollection/*SqlParameterCollection*/ commandParameters)
        {
            //create & open a SqlConnection, and dispose of it after we are done.
            using (MySqlConnection cn = new MySqlConnection(connectionString))
            //using (SqlConnection cn = new SqlConnection(connectionString))
            {
                cn.Open();

                //call the overload that takes a connection in place of the connection string
                return ExecuteNonQuery(cn, commandType, commandText, commandParameters);
            }
        }

        public static int ExecuteNonQuery(string connectionString,
                                          MySqlCommand/*SqlCommand*/ command)
        {
            //create & open a SqlConnection, and dispose of it after we are done.
            MySqlConnection cn = null;
            //SqlConnection cn = null;
            try
            {
                //cn = new SqlConnection(connectionString);
                cn = new MySqlConnection(connectionString);

                cn.Open();

                //call the overload that takes a connection in place of the connection string
                command.Connection = cn;
                //				return ExecuteNonQuery(cn, commandType, commandText, commandParameters);
                int rtnValue = command.ExecuteNonQuery();

                cn.Close();
                cn.Dispose();

                return rtnValue;

            }
            catch
            {
                try
                {
                    if (cn != null && cn.State == ConnectionState.Open)
                    {
                        cn.Close();
                        cn.Dispose();
                    }
                }
                catch { }
                //ExceptionManager.Publish(ex);
            }
            return -1;
        }

        public static int ExecuteNonQuery(MySqlTransaction/*SqlTransaction*/ transaction, MySqlCommand/*SqlCommand*/ command)
        {

            command.Transaction = transaction;
            command.Connection = transaction.Connection;
            //				return ExecuteNonQuery(cn, commandType, commandText, commandParameters);
            int rtnValue = command.ExecuteNonQuery();

            return rtnValue;
        }

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns no resultset) against the database specified in 
        /// the connection string using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return Value parameter.
        /// 
        /// e.g.:  
        ///  int result = ExecuteNonQuery(connString, "PublishOrders", 24, 36);
        /// </remarks>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="spName">the Name of the stored prcedure</param>
        /// <param Name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(string connectionString, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                MySqlParameter[] commandParameters = DataAccessHelperParameterCache.GetSpParameterSet(connectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SqlParameters
                return ExecuteNonQuery(connectionString, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteNonQuery(connectionString, CommandType.StoredProcedure, spName);
            }
        }


        /// <summary>
        /// Execute a SqlCommand (that returns no resultset and takes no parameters) against the provided SqlConnection. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int result = ExecuteNonQuery(conn, "select * frm Orders");
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="SqlString">T-Sql command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(MySqlConnection connection, string SqlString)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteNonQuery(connection, CommandType.Text, SqlString, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns no resultset and takes no parameters) against the provided SqlConnection. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int result = ExecuteNonQuery(conn, CommandType.StoredProcedure, "PublishOrders");
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(MySqlConnection connection, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteNonQuery(connection, commandType, commandText, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns no resultset) against the specified SqlConnection 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int result = ExecuteNonQuery(conn, CommandType.StoredProcedure, "PublishOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(MySqlConnection connection, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            MySqlCommand cmd = new MySqlCommand();
            PrepareCommand(cmd, connection, (MySqlTransaction)null, commandType, commandText, commandParameters);

            //finally, execute the command.
            int retval = cmd.ExecuteNonQuery();

            // detach the SqlParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();
            return retval;
        }

        /// <summary>
        /// Execute a SqlCommand (that returns no resultset) against the specified SqlConnection 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int result = ExecuteNonQuery(conn, CommandType.StoredProcedure, "PublishOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">a collection of SqlParamters used to execute the command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(MySqlConnection connection, CommandType commandType, string commandText, MySqlParameterCollection commandParameters)
        {
            //create a command and prepare it for execution
            MySqlCommand cmd = new MySqlCommand();
            PrepareCommand(cmd, connection, (MySqlTransaction)null, commandType, commandText, commandParameters);

            //finally, execute the command.
            int retval = cmd.ExecuteNonQuery();

            // detach the SqlParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();
            return retval;
        }

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns no resultset) against the specified SqlConnection 
        /// using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return Value parameter.
        /// 
        /// e.g.:  
        ///  int result = ExecuteNonQuery(conn, "PublishOrders", 24, 36);
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="spName">the Name of the stored procedure</param>
        /// <param Name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(MySqlConnection connection, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                MySqlParameter[] commandParameters = DataAccessHelperParameterCache.GetSpParameterSet(connection.ConnectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SqlParameters
                return ExecuteNonQuery(connection, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteNonQuery(connection, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>
        /// Execute a SqlCommand (that returns no resultset and takes no parameters) against the provided SqlTransaction. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int result = ExecuteNonQuery(trans, CommandType.StoredProcedure, "PublishOrders");
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="SqlString">T-Sql command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(MySqlTransaction transaction, string SqlString)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteNonQuery(transaction, CommandType.Text, SqlString, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns no resultset and takes no parameters) against the provided SqlTransaction. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int result = ExecuteNonQuery(trans, CommandType.StoredProcedure, "PublishOrders");
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(MySqlTransaction transaction, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteNonQuery(transaction, commandType, commandText, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns no resultset) against the specified SqlTransaction
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int result = ExecuteNonQuery(trans, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(MySqlTransaction transaction, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            MySqlCommand cmd = new MySqlCommand();
            PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters);

            //finally, execute the command.
            int retval = cmd.ExecuteNonQuery();

            // detach the SqlParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();
            return retval;
        }

        /// <summary>
        /// Execute a SqlCommand (that returns no resultset) against the specified SqlTransaction
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int result = ExecuteNonQuery(trans, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">a collection of SqlParamters used to execute the command</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(MySqlTransaction transaction, CommandType commandType, string commandText, MySqlParameterCollection commandParameters)
        {
            //create a command and prepare it for execution
            MySqlCommand cmd = new MySqlCommand();
            PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters);

            //finally, execute the command.
            int retval = cmd.ExecuteNonQuery();

            // detach the SqlParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();
            return retval;
        }

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns no resultset) against the specified 
        /// SqlTransaction using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return Value parameter.
        /// 
        /// e.g.:  
        ///  int result = ExecuteNonQuery(conn, trans, "PublishOrders", 24, 36);
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="spName">the Name of the stored procedure</param>
        /// <param Name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>an int representing the number of rows affected by the command</returns>
        public static int ExecuteNonQuery(MySqlTransaction transaction, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                MySqlParameter[] commandParameters = DataAccessHelperParameterCache.GetSpParameterSet(transaction.Connection.ConnectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SqlParameters
                return ExecuteNonQuery(transaction, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteNonQuery(transaction, CommandType.StoredProcedure, spName);
            }
        }


        #endregion ExecuteNonQuersy

        #region ExecuteDataSet

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the database specified in 
        /// the connection string. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(connString, "select * from Orders");
        /// </remarks>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="SqlString">T-Sql command</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(string connectionString, string SqlString)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteDataset(connectionString, CommandType.Text, SqlString, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the database specified in 
        /// the connection string. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(connString, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(string connectionString, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteDataset(connectionString, commandType, commandText, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the database specified in the connection string 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(connString, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(string connectionString, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
        {
            //create & open a SqlConnection, and dispose of it after we are done.
            using (MySqlConnection cn = new MySqlConnection(connectionString))
            {
                cn.Open();

                //call the overload that takes a connection in place of the connection string
                return ExecuteDataset(cn, commandType, commandText, commandParameters);
            }
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the database specified in the connection string 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(connString, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">a collection of SqlParamters used to execute the command</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(string connectionString, CommandType commandType, string commandText, MySqlParameterCollection commandParameters)
        {
            //create & open a SqlConnection, and dispose of it after we are done.
            using (MySqlConnection cn = new MySqlConnection(connectionString))
            {
                cn.Open();

                //call the overload that takes a connection in place of the connection string
                return ExecuteDataset(cn, commandType, commandText, commandParameters);
            }
        }

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns a resultset) against the database specified in 
        /// the connection string using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return Value parameter.
        /// 
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(connString, "GetOrders", 24, 36);
        /// </remarks>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="spName">the Name of the stored procedure</param>
        /// <param Name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(string connectionString, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                MySqlParameter[] commandParameters = DataAccessHelperParameterCache.GetSpParameterSet(connectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SqlParameters
                return ExecuteDataset(connectionString, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteDataset(connectionString, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the provided SqlConnection. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(conn, "Select * from Orders");
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="SqlString">T-Sql command </param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(MySqlConnection connection, string SqlString)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteDataset(connection, CommandType.Text, SqlString, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the provided SqlConnection. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(conn, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(MySqlConnection connection, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteDataset(connection, commandType, commandText, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the specified SqlConnection 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(conn, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(MySqlConnection connection, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            MySqlCommand cmd = new MySqlCommand();
            PrepareCommand(cmd, connection, (MySqlTransaction)null, commandType, commandText, commandParameters);

            try
            {
                //create the DataAdapter & DataSet
                MySqlDataAdapter da = new MySqlDataAdapter(cmd);
                DataSet ds = new DataSet();

                //fill the DataSet using default values for DataTable names, etc.
                da.Fill(ds);

                // detach the SqlParameters from the command object, so they can be used again.			
                cmd.Parameters.Clear();

                //return the dataset
                return ds;
            }
            catch (Exception ee)
            {
                //return null;
                throw new Exception(ee.Message);
            }
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the specified SqlConnection 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(conn, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">a collection of SqlParamters used to execute the command</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(MySqlConnection connection, CommandType commandType, string commandText, MySqlParameterCollection commandParameters)
        {
            //create a command and prepare it for execution
            MySqlCommand cmd = new MySqlCommand();

            PrepareCommand(cmd, connection, (MySqlTransaction)null, commandType, commandText, commandParameters);

            //create the DataAdapter & DataSet
            MySqlDataAdapter da = new MySqlDataAdapter(cmd);
            DataSet ds = new DataSet();

            //fill the DataSet using default values for DataTable names, etc.
            da.Fill(ds);

            // detach the SqlParameters from the command object, so they can be used again.			
            cmd.Parameters.Clear();

            //return the dataset
            return ds;
        }

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns a resultset) against the specified SqlConnection 
        /// using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return Value parameter.
        /// 
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(conn, "GetOrders", 24, 36);
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="spName">the Name of the stored procedure</param>
        /// <param Name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(MySqlConnection connection, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                MySqlParameter[] commandParameters = DataAccessHelperParameterCache.GetSpParameterSet(connection.ConnectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SqlParameters
                return ExecuteDataset(connection, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteDataset(connection, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the provided SqlTransaction. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(trans, "select * from Orders");
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="SqlString">T-Sql command</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(MySqlTransaction transaction, string SqlString)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteDataset(transaction, CommandType.Text, SqlString, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the provided SqlTransaction. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(trans, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(MySqlTransaction transaction, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteDataset(transaction, commandType, commandText, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the specified SqlTransaction
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(trans, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(MySqlTransaction transaction, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            MySqlCommand cmd = new MySqlCommand();
            PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters);

            //create the DataAdapter & DataSet
            MySqlDataAdapter da = new MySqlDataAdapter(cmd);
            DataSet ds = new DataSet();

            //fill the DataSet using default values for DataTable names, etc.
            da.Fill(ds);

            // detach the SqlParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();

            //return the dataset
            return ds;
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the specified SqlTransaction
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(trans, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">a collection of SqlParamters used to execute the command</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(MySqlTransaction transaction, CommandType commandType, string commandText, MySqlParameterCollection commandParameters)
        {
            //create a command and prepare it for execution
            MySqlCommand cmd = new MySqlCommand();
            PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters);

            //create the DataAdapter & DataSet
            MySqlDataAdapter da = new MySqlDataAdapter(cmd);
            DataSet ds = new DataSet();

            //fill the DataSet using default values for DataTable names, etc.
            da.Fill(ds);

            // detach the SqlParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();

            //return the dataset
            return ds;
        }

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns a resultset) against the specified 
        /// SqlTransaction using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return Value parameter.
        /// 
        /// e.g.:  
        ///  DataSet ds = ExecuteDataset(trans, "GetOrders", 24, 36);
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="spName">the Name of the stored procedure</param>
        /// <param Name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static DataSet ExecuteDataset(MySqlTransaction transaction, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                MySqlParameter[] commandParameters = DataAccessHelperParameterCache.GetSpParameterSet(transaction.Connection.ConnectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SqlParameters
                return ExecuteDataset(transaction, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteDataset(transaction, CommandType.StoredProcedure, spName);
            }
        }

        #endregion ExecuteDataSet

        #region ExecuteReader

        /// <summary>
        /// this enum is used to indicate whether the connection was provided by the caller, or created by DataAccessHelper, so that
        /// we can set the appropriate CommandBehavior when calling ExecuteReader()
        /// </summary>
        private enum SqlConnectionOwnership
        {
            /// <summary>Connection is owned and managed by DataAccessHelper</summary>
            Internal,
            /// <summary>Connection is owned and managed by the caller</summary>
            External
        }

        /// <summary>
        /// Create and prepare a SqlCommand, and call ExecuteReader with the appropriate CommandBehavior.
        /// </summary>
        /// <remarks>
        /// If we created and opened the connection, we want the connection to be closed when the DataReader is closed.
        /// 
        /// If the caller provided the connection, we want to leave it to them to manage.
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection, on which to execute this command</param>
        /// <param Name="transaction">a valid SqlTransaction, or 'null'</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">an array of SqlParameters to be associated with the command or 'null' if no parameters are required</param>
        /// <param Name="connectionOwnership">indicates whether the connection parameter was provided by the caller, or created by DataAccessHelper</param>
        /// <returns>SqlDataReader containing the results of the command</returns>
        private static MySqlDataReader ExecuteReader(MySqlConnection connection, MySqlTransaction transaction, CommandType commandType, string commandText, MySqlParameter[] commandParameters, SqlConnectionOwnership connectionOwnership)
        {
            //create a command and prepare it for execution
            MySqlCommand cmd = new MySqlCommand();
            PrepareCommand(cmd, connection, transaction, commandType, commandText, commandParameters);

            //create a reader
            MySqlDataReader dr;

            // call ExecuteReader with the appropriate CommandBehavior
            if (connectionOwnership == SqlConnectionOwnership.External)
            {
                dr = cmd.ExecuteReader();
            }
            else
            {
                dr = cmd.ExecuteReader(CommandBehavior.CloseConnection);
            }

            // detach the SqlParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();

            return dr;
        }

        //		/// <summary>
        //		/// Create and prepare a SqlCommand, and call ExecuteReader with the appropriate CommandBehavior.
        //		/// </summary>
        //		/// <remarks>
        //		/// If we created and opened the connection, we want the connection to be closed when the DataReader is closed.
        //		/// 
        //		/// If the caller provided the connection, we want to leave it to them to manage.
        //		/// </remarks>
        //		/// <param Name="connection">a valid SqlConnection, on which to execute this command</param>
        //		/// <param Name="transaction">a valid SqlTransaction, or 'null'</param>
        //		/// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        //		/// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        //		/// <param Name="commandParameters">a collection of SqlParameters to be associated with the command or 'null' if no parameters are required</param>
        //		/// <param Name="connectionOwnership">indicates whether the connection parameter was provided by the caller, or created by DataAccessHelper</param>
        //		/// <returns>SqlDataReader containing the results of the command</returns>
        //		private static SqlDataReader ExecuteReader(SqlConnection connection, SqlTransaction transaction, CommandType commandType, string commandText, SqlParameterCollection commandParameters, SqlConnectionOwnership connectionOwnership)
        //		{	
        //			//create a command and prepare it for execution
        //			SqlCommand cmd = new SqlCommand();
        //			PrepareCommand(cmd, connection, transaction, commandType, commandText, commandParameters);
        //			
        //			//create a reader
        //			SqlDataReader dr;
        //
        //			// call ExecuteReader with the appropriate CommandBehavior
        //			if (connectionOwnership == SqlConnectionOwnership.External)
        //			{
        //				dr = cmd.ExecuteReader();
        //			}
        //			else
        //			{
        //				dr = cmd.ExecuteReader(CommandBehavior.CloseConnection);
        //			}
        //			
        //			// detach the SqlParameters from the command object, so they can be used again.
        //			cmd.Parameters.Clear();
        //			
        //			return dr;
        //		}

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the database specified in 
        /// the connection string. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(connString, "select * frm Orders");
        /// </remarks>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="SqlString">the stored procedure Name or T-Sql command</param>
        /// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        public static MySqlDataReader ExecuteReader(string connectionString, string SqlString)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteReader(connectionString, CommandType.Text, SqlString, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the database specified in 
        /// the connection string. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(connString, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        public static MySqlDataReader ExecuteReader(string connectionString, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteReader(connectionString, commandType, commandText, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the database specified in the connection string 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(connString, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        public static MySqlDataReader ExecuteReader(string connectionString, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
        {
            //create & open a SqlConnection
            MySqlConnection cn = new MySqlConnection(connectionString);

            try
            {
                cn.Open();
                //call the private overload that takes an internally owned connection in place of the connection string
                return ExecuteReader(cn, null, commandType, commandText, commandParameters, SqlConnectionOwnership.Internal);
            }
            catch
            {
                //if we fail to return the SqlDatReader, we need to close the connection ourselves
                cn.Close();
                throw;
            }
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the database specified in the connection string 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(connString, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">a collection of SqlParamters used to execute the command</param>
        /// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        public static MySqlDataReader ExecuteReader(string connectionString, CommandType commandType, string commandText, MySqlParameterCollection commandParameters)
        {
            //create & open a SqlConnection
            MySqlConnection cn = new MySqlConnection(connectionString);
            cn.Open();

            try
            {
                //call the private overload that takes an internally owned connection in place of the connection string
                return ExecuteReader(cn, null, commandType, commandText, commandParameters, SqlConnectionOwnership.Internal);
            }
            catch
            {
                //if we fail to return the SqlDatReader, we need to close the connection ourselves
                cn.Close();
                throw;
            }
        }

        public static MySqlDataReader ExecuteReader(string connectionString, MySqlCommand command)
        {
            //create & open a SqlConnection
            MySqlConnection cn = new MySqlConnection(connectionString);
            cn.Open();

            try
            {
                //call the private overload that takes an internally owned connection in place of the connection string
                command.Connection = cn;
                return command.ExecuteReader();
            }
            catch
            {
                //if we fail to return the SqlDatReader, we need to close the connection ourselves
                cn.Close();
                cn.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns a resultset) against the database specified in 
        /// the connection string using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return Value parameter.
        /// 
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(connString, "GetOrders", 24, 36);
        /// </remarks>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="spName">the Name of the stored procedure</param>
        /// <param Name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        public static MySqlDataReader ExecuteReader(string connectionString, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                MySqlParameter[] commandParameters = DataAccessHelperParameterCache.GetSpParameterSet(connectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SqlParameters
                return ExecuteReader(connectionString, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteReader(connectionString, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param Name="connectionString"></param>
        /// <param Name="spName"></param>
        /// <param Name="parameterValues"></param>
        /// <returns></returns>
        public static MySqlDataReader ExecuteReader(string connectionString, string spName, params MySqlParameter[] parameterValues)
        {
            //call the overload that takes an array of SqlParameters
            return ExecuteReader(connectionString, CommandType.StoredProcedure, spName, parameterValues);

        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the provided SqlConnection. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(conn, "select * from Orders");
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="SqlString">T-Sql command</param>
        /// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        public static MySqlDataReader ExecuteReader(MySqlConnection connection, string SqlString)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteReader(connection, CommandType.Text, SqlString, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the provided SqlConnection. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(conn, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        public static MySqlDataReader ExecuteReader(MySqlConnection connection, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteReader(connection, commandType, commandText, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the specified SqlConnection 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(conn, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        public static MySqlDataReader ExecuteReader(MySqlConnection connection, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
        {
            //pass through the call to the private overload using a null transaction Value and an externally owned connection
            return ExecuteReader(connection, (MySqlTransaction)null, commandType, commandText, commandParameters, SqlConnectionOwnership.External);
        }

        //		/// <summary>
        //		/// Execute a SqlCommand (that returns a resultset) against the specified SqlConnection 
        //		/// using the provided parameters.
        //		/// </summary>
        //		/// <remarks>
        //		/// e.g.:  
        //		///  SqlDataReader dr = ExecuteReader(conn, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        //		/// </remarks>
        //		/// <param Name="connection">a valid SqlConnection</param>
        //		/// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        //		/// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        //		/// <param Name="commandParameters">a collection of SqlParamters used to execute the command</param>
        //		/// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        //		public static SqlDataReader ExecuteReader(SqlConnection connection, CommandType commandType, string commandText, SqlParameterCollection commandParameters)
        //		{
        //			//pass through the call to the private overload using a null transaction Value and an externally owned connection
        //			return ExecuteReader(connection, (SqlTransaction)null, commandType, commandText, commandParameters, SqlConnectionOwnership.External);
        //		}

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns a resultset) against the specified SqlConnection 
        /// using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return Value parameter.
        /// 
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(conn, "GetOrders", 24, 36);
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="spName">the Name of the stored procedure</param>
        /// <param Name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        public static MySqlDataReader ExecuteReader(MySqlConnection connection, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                MySqlParameter[] commandParameters = DataAccessHelperParameterCache.GetSpParameterSet(connection.ConnectionString, spName);

                AssignParameterValues(commandParameters, parameterValues);

                return ExecuteReader(connection, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteReader(connection, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the provided SqlTransaction. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(trans, "select * from Oreders");
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="SqlString">T-Sql command</param>
        /// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        public static MySqlDataReader ExecuteReader(MySqlTransaction transaction, string SqlString)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteReader(transaction, CommandType.Text, SqlString, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the provided SqlTransaction. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(trans, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        public static MySqlDataReader ExecuteReader(MySqlTransaction transaction, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteReader(transaction, commandType, commandText, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the specified SqlTransaction
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///   SqlDataReader dr = ExecuteReader(trans, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        public static MySqlDataReader ExecuteReader(MySqlTransaction transaction, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
        {
            //pass through to private overload, indicating that the connection is owned by the caller
            return ExecuteReader(transaction.Connection, transaction, commandType, commandText, commandParameters, SqlConnectionOwnership.External);
        }

        //		/// <summary>
        //		/// Execute a SqlCommand (that returns a resultset) against the specified SqlTransaction
        //		/// using the provided parameters.
        //		/// </summary>
        //		/// <remarks>
        //		/// e.g.:  
        //		///   SqlDataReader dr = ExecuteReader(trans, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        //		/// </remarks>
        //		/// <param Name="transaction">a valid SqlTransaction</param>
        //		/// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        //		/// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        //		/// <param Name="commandParameters">a collection of SqlParamters used to execute the command</param>
        //		/// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        //		public static SqlDataReader ExecuteReader(SqlTransaction transaction, CommandType commandType, string commandText, SqlParameterCollection commandParameters)
        //		{
        //			//pass through to private overload, indicating that the connection is owned by the caller
        //			return ExecuteReader(transaction.Connection, transaction, commandType, commandText, commandParameters, SqlConnectionOwnership.External);
        //		}

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns a resultset) against the specified
        /// SqlTransaction using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return Value parameter.
        /// 
        /// e.g.:  
        ///  SqlDataReader dr = ExecuteReader(trans, "GetOrders", 24, 36);
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="spName">the Name of the stored procedure</param>
        /// <param Name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>a SqlDataReader containing the resultset generated by the command</returns>
        public static MySqlDataReader ExecuteReader(MySqlTransaction transaction, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                MySqlParameter[] commandParameters = DataAccessHelperParameterCache.GetSpParameterSet(transaction.Connection.ConnectionString, spName);

                AssignParameterValues(commandParameters, parameterValues);

                return ExecuteReader(transaction, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteReader(transaction, CommandType.StoredProcedure, spName);
            }
        }

        #endregion ExecuteReader

        #region ExecuteScalar

        /// <summary>
        /// Execute a SqlCommand (that returns a 1x1 resultset and takes no parameters) against the database specified in 
        /// the connection string. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(connString,"select count(*) from orders");
        /// </remarks>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="SqlString">T-Sql command</param>
        /// <returns>an object containing the Value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(string connectionString, string SqlString)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteScalar(connectionString, CommandType.Text, SqlString, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a 1x1 resultset and takes no parameters) against the database specified in 
        /// the connection string. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(connString, CommandType.StoredProcedure, "GetOrderCount");
        /// </remarks>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <returns>an object containing the Value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(string connectionString, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteScalar(connectionString, commandType, commandText, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a 1x1 resultset) against the database specified in the connection string 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(connString, CommandType.StoredProcedure, "GetOrderCount", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>an object containing the Value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(string connectionString, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
        {
            //create & open a SqlConnection, and dispose of it after we are done.
            using (MySqlConnection cn = new MySqlConnection(connectionString))
            {
                cn.Open();

                //call the overload that takes a connection in place of the connection string
                return ExecuteScalar(cn, commandType, commandText, commandParameters);
            }
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a 1x1 resultset) against the database specified in the connection string 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(connString, CommandType.StoredProcedure, "GetOrderCount", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">a collection of SqlParamters used to execute the command</param>
        /// <returns>an object containing the Value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(string connectionString, CommandType commandType, string commandText, MySqlParameterCollection commandParameters)
        {
            //create & open a SqlConnection, and dispose of it after we are done.
            using (MySqlConnection cn = new MySqlConnection(connectionString))
            {
                cn.Open();

                //call the overload that takes a connection in place of the connection string
                return ExecuteScalar(cn, commandType, commandText, commandParameters);
            }
        }

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns a 1x1 resultset) against the database specified in 
        /// the connection string using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return Value parameter.
        /// 
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(connString, "GetOrderCount", 24, 36);
        /// </remarks>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="spName">the Name of the stored procedure</param>
        /// <param Name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>an object containing the Value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(string connectionString, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                MySqlParameter[] commandParameters = DataAccessHelperParameterCache.GetSpParameterSet(connectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SqlParameters
                return ExecuteScalar(connectionString, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteScalar(connectionString, CommandType.StoredProcedure, spName);
            }
        }

        public static object ExecuteScalar(string connectionString, string spName, params MySqlParameter[] parameterValues)
        {
            return ExecuteScalar(connectionString, CommandType.StoredProcedure, spName, parameterValues);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a 1x1 resultset and takes no parameters) against the provided SqlConnection. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(conn, "select count(*) from orders");
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="SqlString">the stored procedure Name or T-Sql command</param>
        /// <returns>an object containing the Value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(MySqlConnection connection, string SqlString)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteScalar(connection, CommandType.Text, SqlString, (MySqlParameter[])null);
        }


        /// <summary>
        /// Execute a SqlCommand (that returns a 1x1 resultset and takes no parameters) against the provided SqlConnection. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(conn, CommandType.StoredProcedure, "GetOrderCount");
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <returns>an object containing the Value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(MySqlConnection connection, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteScalar(connection, commandType, commandText, (MySqlParameter[])null);
        }

        public static object ExecuteScalar(MySqlTransaction transaction, MySqlCommand command)
        {
            //pass through the call providing null for the set of SqlParameters
            command.Transaction = transaction;
            command.Connection = transaction.Connection;
            return command.ExecuteScalar();
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a 1x1 resultset) against the specified SqlConnection 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(conn, CommandType.StoredProcedure, "GetOrderCount", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>an object containing the Value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(MySqlConnection connection, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            MySqlCommand cmd = new MySqlCommand();
            PrepareCommand(cmd, connection, (MySqlTransaction)null, commandType, commandText, commandParameters);

            //execute the command & return the results
            object retval = cmd.ExecuteScalar();

            // detach the SqlParameters from the command object, so they can be used again
            cmd.Parameters.Clear();
            return retval;

        }

        /// <summary>
        /// Execute a SqlCommand (that returns a 1x1 resultset) against the specified SqlConnection 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(conn, CommandType.StoredProcedure, "GetOrderCount", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">a collection of SqlParamters used to execute the command</param>
        /// <returns>an object containing the Value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(MySqlConnection connection, CommandType commandType, string commandText, MySqlParameterCollection commandParameters)
        {
            //create a command and prepare it for execution
            MySqlCommand cmd = new MySqlCommand();
            PrepareCommand(cmd, connection, (MySqlTransaction)null, commandType, commandText, commandParameters);

            //execute the command & return the results
            object retval = cmd.ExecuteScalar();

            // detach the SqlParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();
            return retval;

        }

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns a 1x1 resultset) against the specified SqlConnection 
        /// using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return Value parameter.
        /// 
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(conn, "GetOrderCount", 24, 36);
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="spName">the Name of the stored procedure</param>
        /// <param Name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>an object containing the Value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(MySqlConnection connection, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                MySqlParameter[] commandParameters = DataAccessHelperParameterCache.GetSpParameterSet(connection.ConnectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SqlParameters
                return ExecuteScalar(connection, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteScalar(connection, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a 1x1 resultset and takes no parameters) against the provided SqlTransaction. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(trans, "select count(*) from orders");
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="SqlString">T-Sql command</param>
        /// <returns>an object containing the Value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(MySqlTransaction transaction, string SqlString)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteScalar(transaction, CommandType.Text, SqlString, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a 1x1 resultset and takes no parameters) against the provided SqlTransaction. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(trans, CommandType.StoredProcedure, "GetOrderCount");
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <returns>an object containing the Value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(MySqlTransaction transaction, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteScalar(transaction, commandType, commandText, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a 1x1 resultset) against the specified SqlTransaction
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(trans, CommandType.StoredProcedure, "GetOrderCount", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>an object containing the Value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(MySqlTransaction transaction, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            MySqlCommand cmd = new MySqlCommand();
            PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters);

            //execute the command & return the results
            object retval = cmd.ExecuteScalar();

            // detach the SqlParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();
            return retval;
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a 1x1 resultset) against the specified SqlTransaction
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(trans, CommandType.StoredProcedure, "GetOrderCount", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">a collection of SqlParamters used to execute the command</param>
        /// <returns>an object containing the Value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(MySqlTransaction transaction, CommandType commandType, string commandText, MySqlParameterCollection commandParameters)
        {
            //create a command and prepare it for execution
            MySqlCommand cmd = new MySqlCommand();
            PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters);

            //execute the command & return the results
            object retval = cmd.ExecuteScalar();

            // detach the SqlParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();
            return retval;
        }

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns a 1x1 resultset) against the specified
        /// SqlTransaction using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return Value parameter.
        /// 
        /// e.g.:  
        ///  int orderCount = (int)ExecuteScalar(trans, "GetOrderCount", 24, 36);
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="spName">the Name of the stored procedure</param>
        /// <param Name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>an object containing the Value in the 1x1 resultset generated by the command</returns>
        public static object ExecuteScalar(MySqlTransaction transaction, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                MySqlParameter[] commandParameters = DataAccessHelperParameterCache.GetSpParameterSet(transaction.Connection.ConnectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SqlParameters
                return ExecuteScalar(transaction, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteScalar(transaction, CommandType.StoredProcedure, spName);
            }
        }

        #endregion ExecuteScalar

        #region ExecuteXmlReader

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the provided SqlConnection. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  XmlReader r = ExecuteXmlReader(conn, "select * from orders");
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="SqlString">T-Sql command using "FOR XML AUTO"</param>
        /// <returns>an XmlReader containing the resultset generated by the command</returns>
        public static XmlReader ExecuteXmlReader(MySqlConnection connection, string SqlString)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteXmlReader(connection, CommandType.Text, SqlString, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the provided SqlConnection. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  XmlReader r = ExecuteXmlReader(conn, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command using "FOR XML AUTO"</param>
        /// <returns>an XmlReader containing the resultset generated by the command</returns>
        public static XmlReader ExecuteXmlReader(MySqlConnection connection, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteXmlReader(connection, commandType, commandText, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the specified SqlConnection 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  XmlReader r = ExecuteXmlReader(conn, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command using "FOR XML AUTO"</param>
        /// <param Name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>an XmlReader containing the resultset generated by the command</returns>
        public static XmlReader ExecuteXmlReader(MySqlConnection connection, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            MySqlCommand cmd = new MySqlCommand();
            PrepareCommand(cmd, connection, (MySqlTransaction)null, commandType, commandText, commandParameters);

            //create the DataAdapter & DataSet
            XmlReader retval = null;//  cmd.ExecuteXmlReader();


            // detach the SqlParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();
            return retval;

        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the specified SqlConnection 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  XmlReader r = ExecuteXmlReader(conn, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command using "FOR XML AUTO"</param>
        /// <param Name="commandParameters">a colleciton of SqlParamters used to execute the command</param>
        /// <returns>an XmlReader containing the resultset generated by the command</returns>
        public static XmlReader ExecuteXmlReader(MySqlConnection connection, CommandType commandType, string commandText, MySqlParameterCollection commandParameters)
        {
            //create a command and prepare it for execution
            MySqlCommand cmd = new MySqlCommand();
            PrepareCommand(cmd, connection, (MySqlTransaction)null, commandType, commandText, commandParameters);

            //create the DataAdapter & DataSet
            XmlReader retval = null;//cmd.ExecuteXmlReader();

            // detach the SqlParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();
            return retval;

        }

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns a resultset) against the specified SqlConnection 
        /// using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return Value parameter.
        /// 
        /// e.g.:  
        ///  XmlReader r = ExecuteXmlReader(conn, "GetOrders", 24, 36);
        /// </remarks>
        /// <param Name="connection">a valid SqlConnection</param>
        /// <param Name="spName">the Name of the stored procedure using "FOR XML AUTO"</param>
        /// <param Name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>an XmlReader containing the resultset generated by the command</returns>
        public static XmlReader ExecuteXmlReader(MySqlConnection connection, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                MySqlParameter[] commandParameters = DataAccessHelperParameterCache.GetSpParameterSet(connection.ConnectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SqlParameters
                return ExecuteXmlReader(connection, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteXmlReader(connection, CommandType.StoredProcedure, spName);
            }
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the provided SqlTransaction. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  XmlReader r = ExecuteXmlReader(trans,"select * from orders");
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="SqlString">T-Sql command using "FOR XML AUTO"</param>
        /// <returns>an XmlReader containing the resultset generated by the command</returns>
        public static XmlReader ExecuteXmlReader(MySqlTransaction transaction, string SqlString)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteXmlReader(transaction, CommandType.Text, SqlString, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset and takes no parameters) against the provided SqlTransaction. 
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  XmlReader r = ExecuteXmlReader(trans, CommandType.StoredProcedure, "GetOrders");
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command using "FOR XML AUTO"</param>
        /// <returns>an XmlReader containing the resultset generated by the command</returns>
        public static XmlReader ExecuteXmlReader(MySqlTransaction transaction, CommandType commandType, string commandText)
        {
            //pass through the call providing null for the set of SqlParameters
            return ExecuteXmlReader(transaction, commandType, commandText, (MySqlParameter[])null);
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the specified SqlTransaction
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  XmlReader r = ExecuteXmlReader(trans, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command using "FOR XML AUTO"</param>
        /// <param Name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>an XmlReader containing the resultset generated by the command</returns>
        public static XmlReader ExecuteXmlReader(MySqlTransaction transaction, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
        {
            //create a command and prepare it for execution
            MySqlCommand cmd = new MySqlCommand();
            PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters);

            //create the DataAdapter & DataSet
            XmlReader retval = null;//cmd.ExecuteXmlReader();

            // detach the SqlParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();
            return retval;
        }

        /// <summary>
        /// Execute a SqlCommand (that returns a resultset) against the specified SqlTransaction
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  XmlReader r = ExecuteXmlReader(trans, CommandType.StoredProcedure, "GetOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command using "FOR XML AUTO"</param>
        /// <param Name="commandParameters">a collection of SqlParamters used to execute the command</param>
        /// <returns>an XmlReader containing the resultset generated by the command</returns>
        public static XmlReader ExecuteXmlReader(MySqlTransaction transaction, CommandType commandType, string commandText, MySqlParameterCollection commandParameters)
        {
            //create a command and prepare it for execution
            MySqlCommand cmd = new MySqlCommand();
            PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters);

            //create the DataAdapter & DataSet
            XmlReader retval = null;//cmd.ExecuteXmlReader();

            // detach the SqlParameters from the command object, so they can be used again.
            cmd.Parameters.Clear();
            return retval;
        }

        /// <summary>
        /// Execute a stored procedure via a SqlCommand (that returns a resultset) against the specified 
        /// SqlTransaction using the provided parameter values.  This method will query the database to discover the parameters for the 
        /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
        /// </summary>
        /// <remarks>
        /// This method provides no access to output parameters or the stored procedure's return Value parameter.
        /// 
        /// e.g.:  
        ///  XmlReader r = ExecuteXmlReader(trans, "GetOrders", 24, 36);
        /// </remarks>
        /// <param Name="transaction">a valid SqlTransaction</param>
        /// <param Name="spName">the Name of the stored procedure</param>
        /// <param Name="parameterValues">an array of objects to be assigned as the input values of the stored procedure</param>
        /// <returns>a dataset containing the resultset generated by the command</returns>
        public static XmlReader ExecuteXmlReader(MySqlTransaction transaction, string spName, params object[] parameterValues)
        {
            //if we receive parameter values, we need to figure out where they go
            if ((parameterValues != null) && (parameterValues.Length > 0))
            {
                //pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                MySqlParameter[] commandParameters = DataAccessHelperParameterCache.GetSpParameterSet(transaction.Connection.ConnectionString, spName);

                //assign the provided values to these parameters based on parameter order
                AssignParameterValues(commandParameters, parameterValues);

                //call the overload that takes an array of SqlParameters
                return ExecuteXmlReader(transaction, CommandType.StoredProcedure, spName, commandParameters);
            }
            //otherwise we can just call the SP without params
            else
            {
                return ExecuteXmlReader(transaction, CommandType.StoredProcedure, spName);
            }
        }


        #endregion ExecuteXmlReader

        #region ExecteAdapter

        public static MySqlDataAdapter ExecuteAdapter(string connectionString, string SqlString)
        {
            MySqlDataAdapter Da = new MySqlDataAdapter();
            MySqlConnection cn = new MySqlConnection(connectionString);
            cn.Open();
            Da.SelectCommand = new MySqlCommand(SqlString, cn);

            return Da;
        }

        #endregion
    }

    /// <summary>
    /// DataAccessHelperParameterCache provides functions to leverage a static cache of procedure parameters, and the
    /// ability to discover parameters for stored procedures at run-time.
    /// </summary>
    public sealed class DataAccessHelperParameterCache
    {
        #region private methods, variables, and constructors

        //Since this class provides only static methods, make the default constructor private to prevent 
        //instances from being created with "new DataAccessHelperParameterCache()".
        private DataAccessHelperParameterCache() { }

        private static Hashtable paramCache = Hashtable.Synchronized(new Hashtable());

        /// <summary>
        /// resolve at run time the appropriate set of SqlParameters for a stored procedure
        /// </summary>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="spName">the Name of the stored procedure</param>
        /// <param Name="includeReturnValueParameter">whether or not to include their return Value parameter</param>
        /// <returns></returns>
        private static MySqlParameter[] DiscoverSpParameterSet(string connectionString, string spName, bool includeReturnValueParameter)
        {
            using (MySqlConnection cn = new MySqlConnection(connectionString))
            using (MySqlCommand cmd = new MySqlCommand(spName, cn))
            {
                cn.Open();
                cmd.CommandType = CommandType.StoredProcedure;

                MySqlCommandBuilder.DeriveParameters(cmd);

                if (!includeReturnValueParameter)
                {
                    cmd.Parameters.RemoveAt(0);
                }

                MySqlParameter[] discoveredParameters = new MySqlParameter[cmd.Parameters.Count]; ;

                cmd.Parameters.CopyTo(discoveredParameters, 0);

                return discoveredParameters;
            }
        }

        //deep copy of cached SqlParameter array
        private static MySqlParameter[] CloneParameters(MySqlParameter[] originalParameters)
        {
            MySqlParameter[] clonedParameters = new MySqlParameter[originalParameters.Length];

            for (int i = 0, j = originalParameters.Length; i < j; i++)
            {
                clonedParameters[i] = (MySqlParameter)((ICloneable)originalParameters[i]).Clone();
            }

            return clonedParameters;
        }

        #endregion private methods, variables, and constructors

        #region caching functions

        /// <summary>
        /// add parameter array to the cache
        /// </summary>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <param Name="commandParameters">an array of SqlParamters to be cached</param>
        public static void CacheParameterSet(string connectionString, string commandText, params MySqlParameter[] commandParameters)
        {
            string hashKey = connectionString + ":" + commandText;

            paramCache[hashKey] = commandParameters;
        }

        /// <summary>
        /// retrieve a parameter array from the cache
        /// </summary>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="commandText">the stored procedure Name or T-Sql command</param>
        /// <returns>an array of SqlParamters</returns>
        public static MySqlParameter[] GetCachedParameterSet(string connectionString, string commandText)
        {
            string hashKey = connectionString + ":" + commandText;

            MySqlParameter[] cachedParameters = (MySqlParameter[])paramCache[hashKey];

            if (cachedParameters == null)
            {
                return null;
            }
            else
            {
                return CloneParameters(cachedParameters);
            }
        }

        #endregion caching functions

        #region Parameter Discovery Functions

        /// <summary>
        /// Retrieves the set of SqlParameters appropriate for the stored procedure
        /// </summary>
        /// <remarks>
        /// This method will query the database for this information, and then store it in a cache for future requests.
        /// </remarks>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="spName">the Name of the stored procedure</param>
        /// <returns>an array of SqlParameters</returns>
        public static MySqlParameter[] GetSpParameterSet(string connectionString, string spName)
        {
            return GetSpParameterSet(connectionString, spName, false);
        }

        /// <summary>
        /// Retrieves the set of SqlParameters appropriate for the stored procedure
        /// </summary>
        /// <remarks>
        /// This method will query the database for this information, and then store it in a cache for future requests.
        /// </remarks>
        /// <param Name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param Name="spName">the Name of the stored procedure</param>
        /// <param Name="includeReturnValueParameter">a bool Value indicating whether the return Value parameter should be included in the results</param>
        /// <returns>an array of SqlParameters</returns>
        public static MySqlParameter[] GetSpParameterSet(string connectionString, string spName, bool includeReturnValueParameter)
        {
            string hashKey = connectionString + ":" + spName + (includeReturnValueParameter ? ":include ReturnValue Parameter" : "");

            MySqlParameter[] cachedParameters;

            cachedParameters = (MySqlParameter[])paramCache[hashKey];

            if (cachedParameters == null)
            {
                cachedParameters = (MySqlParameter[])(paramCache[hashKey] = DiscoverSpParameterSet(connectionString, spName, includeReturnValueParameter));
            }

            return CloneParameters(cachedParameters);
        }

        public static MySqlParameter GetParameter(string name, MySqlDbType dt, ParameterDirection pd, object val)
        {


            MySqlParameter idp = new MySqlParameter();
            idp.ParameterName = name;
            idp.Direction = pd;
            idp.MySqlDbType = dt;
            if (val != null)
            { idp.Value = val; }

            return idp;
        }

        public static MySqlParameter GetParameter(string name, MySqlDbType dt, int size, ParameterDirection pd, object val)
        {
            MySqlParameter idp = new MySqlParameter();
            idp.ParameterName = name;
            idp.Direction = pd;
            idp.Size = size;
            idp.MySqlDbType = dt;
            if (val != null)
            { idp.Value = val; }

            return idp;
        }


        #endregion Parameter Discovery Functions

    }
}
