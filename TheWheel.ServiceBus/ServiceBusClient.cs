﻿using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace TheWheel.ServiceBus
{
    /// <summary>
    /// Base class to inherit from when making a bus client
    /// </summary>
    public abstract class ServiceBusClient<TMessage> : IDisposable
        where TMessage : MessageBase
    {
        private IDbConnection connection;

        public ServiceBusClient()
        {
            connection = new SqlConnection();
        }

        protected virtual string MessageType
        {
            get { return typeof(TMessage).FullName; }
        }
        protected virtual string Contract
        {
            get { return GetType().FullName; }
        }
        protected virtual string Queue
        {
            get { return GetType().FullName + "Queue"; }
        }
        protected virtual string Service
        {
            get { return GetType().FullName; }
        }
        public virtual TimeSpan Timeout
        {
            get { return System.Threading.Timeout.InfiniteTimeSpan; }
        }

        public void EnsureBrokerReady()
        {
            EnsureConnectionIsOpen();
            var cmd = connection.CreateCommand();
            // Message type
            cmd.CommandText = "SELECT Count(1) FROM sys.service_message_types WHERE name='" + MessageType + "'";
            if (Convert.ToInt32(cmd.ExecuteScalar()) == 0)
            {
                cmd = connection.CreateCommand();
                cmd.CommandText = "CREATE MESSAGE TYPE [" + MessageType + "] VALIDATION = NONE";
                cmd.ExecuteNonQuery();
            }
            // contracts
            cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT Count(1) FROM sys.service_contracts WHERE name='" + Contract + "'";
            if (Convert.ToInt32(cmd.ExecuteScalar()) == 0)
            {
                cmd = connection.CreateCommand();
                cmd.CommandText = "CREATE CONTRACT [" + Contract + "] ([" + MessageType + "] SENT BY ANY)";
                cmd.ExecuteNonQuery();
            }
            // queues
            cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT Count(1) FROM sys.service_queues WHERE name='" + Queue + "'";
            if (Convert.ToInt32(cmd.ExecuteScalar()) == 0)
            {
                cmd = connection.CreateCommand();
                cmd.CommandText = "CREATE QUEUE [" + Queue + "]";
                cmd.ExecuteNonQuery();
            }
            // services
            cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT Count(1) FROM sys.services WHERE name='" + Service + "'";
            if (Convert.ToInt32(cmd.ExecuteScalar()) == 0)
            {
                cmd = connection.CreateCommand();
                cmd.CommandText = "CREATE SERVICE [" + Service + "] ON QUEUE [" + Queue + "] ([" + Contract + "])";
                cmd.ExecuteNonQuery();
            }
        }

        private void EnsureConnectionIsOpen()
        {
            lock (connection)
            {
                if (connection.State != System.Data.ConnectionState.Open)
                    connection.Open();
            }
        }



        public TMessage GetMessage()
        {
            EnsureBrokerReady();
            EnsureConnectionIsOpen();
            var cmd = connection.CreateCommand();
            cmd.CommandText = "WAITFOR (RECEIVE TOP (1) CONVERT(NVARCHAR(MAX), message_body), conversation_handle, message_type_name FROM [" + Queue + "])";
            var timeout = Timeout;
            if (timeout != System.Threading.Timeout.InfiniteTimeSpan)
                timeout = TimeSpan.FromSeconds(Math.Min(cmd.CommandTimeout - 5, Timeout.TotalSeconds));
            else
                timeout = TimeSpan.FromSeconds(cmd.CommandTimeout - 5);

            cmd.CommandText += ", TIMEOUT " + timeout.TotalMilliseconds.ToString("F0");
            TMessage m = default(TMessage);
            using (var reader = cmd.ExecuteReader())
            {
                if (reader.Read())
                {
                    var type = reader.GetString(2);
                    var handle = reader.GetGuid(1);
                    if (type == MessageType)
                    {
                        m = JsonConvert.DeserializeObject<TMessage>(reader.GetString(0));
                        m.ConversationHandle = handle;
                        //m.ConversationGroup = reader.GetGuid(3);
                        Init(m);
                    }
                    //else if (type == "http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog")
                    //{
                    //    cmd = connection.CreateCommand();
                    //    cmd.CommandText = "END CONVERSATION " + handle;
                    //    cmd.ExecuteNonQuery();
                    //}
                    //else if (type == "http://schemas.microsoft.com/SQL/ServiceBroker/Error")
                    //{
                    //    cmd = connection.CreateCommand();
                    //    cmd.CommandText = "END CONVERSATION " + handle;
                    //    cmd.ExecuteNonQuery();
                    //}
                }
            }
            return m;
        }

        protected abstract void Init(TMessage m);

        public void WaitMessage()
        {
            Task.Run<TMessage>(new Func<TMessage>(GetMessage)).ContinueWith<object>(Process);
        }

        #region IDisposable Members

        public void Dispose()
        {
            try
            {
                if (connection != null && connection.State != System.Data.ConnectionState.Closed)
                    connection.Close();
            }
            catch
            {
                throw;
            }
        }

        #endregion

        #region IServiceBus Members

        public virtual void Start(Guid tenantId)
        {
            WaitMessage();
        }



        protected async Task Process(Task<TMessage> message)
        {
            try
            {
                using (var m = await message)
                {
                    WaitMessage();
                    if (m != null)
                    {
                        try
                        {
                            await Handle(m);
                            if (!m.IsOneWay)
                                m.Reply();
                        }
                        catch (Exception ex)
                        {
                            using (var error = new ErrorMessage(m.ConversationHandle.Value, m, ex))
                            {
                                error.Send();
                            }
                        }
                    }
                }
            }
            catch(Exception)
            {
                // Error occurs when reading message
                WaitMessage();
                throw;
            }
        }

        protected abstract Task Handle(TMessage tMessage);

        #endregion
    }
}
