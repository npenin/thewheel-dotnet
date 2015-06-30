using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace TheWheel.ServiceBus
{
    public abstract class MessageBase : IDisposable
    {
        internal Guid? ConversationHandle { get; set; }
        internal Guid? ConversationGroup { get; set; }
        protected internal abstract bool IsOneWay { get; }
        public string Culture { get; set; }

        protected IDbConnection connection;

        protected MessageBase()
        {
            connection = new SqlConnection();
        }

        public MessageBase(MessageBase other)
            : this()
        {
            ConversationHandle = other.ConversationHandle;
            connection.ConnectionString = other.connection.ConnectionString;
        }

        protected virtual string MessageType
        {
            get { return GetType().FullName; }
        }
        protected virtual string Queue
        {
            get { return MessageType; }
        }
        protected virtual string Service
        {
            get { return GetType().FullName + "Service"; }
        }

        public void EnsureBrokerIsReady()
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
                cmd.CommandText = "CREATE SERVICE [" + Service + "] ON QUEUE [" + Queue + "]";
                cmd.ExecuteNonQuery();
            }
        }

        protected void EnsureConnectionIsOpen()
        {
            if (connection.State != System.Data.ConnectionState.Open)
                connection.Open();
        }

        public Task SendAsync()
        {
            return Task.Run(new Action(Send));
        }

        public void Send()
        {
            EnsureBrokerIsReady();
            if (ConversationHandle.HasValue)
            {
                Reply();
                return;
            }

            var cmd = connection.CreateCommand();
            cmd.CommandText = "SendBrokerMessage";
            cmd.CommandType = CommandType.StoredProcedure;

            var fromService = cmd.CreateParameter();
            fromService.ParameterName = "FromService";
            fromService.Value = Service;
            fromService.DbType = DbType.String;
            cmd.Parameters.Add(fromService);

            var messageType = cmd.CreateParameter();
            messageType.ParameterName = "MessageType";
            messageType.Value = MessageType;
            messageType.DbType = DbType.String;
            cmd.Parameters.Add(messageType);

            var message = cmd.CreateParameter();
            message.ParameterName = "MessageBody";
            message.DbType = DbType.String;
            message.Value = JsonConvert.SerializeObject(this);
            cmd.Parameters.Add(message);

            var isOneWay = cmd.CreateParameter();
            isOneWay.ParameterName = "IsOneWay";
            isOneWay.DbType = DbType.Boolean;
            isOneWay.Value = IsOneWay;
            cmd.Parameters.Add(isOneWay);

            if (IsOneWay)
            {
                var count = Convert.ToInt32(cmd.ExecuteScalar());
            }
            else
            {
                cmd.CommandTimeout = 90;
                List<ErrorMessage> errors = new List<ErrorMessage>();
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        try
                        {
                            Merge(reader);
                        }
                        catch (ErrorMessage.Exception ex)
                        {
                            errors.AddRange(ex.Errors);
                        }
                    }
                }
                if (errors.Any())
                    throw new ErrorMessage.Exception(errors.ToArray());
            }

        }

        public Task ReplyAsync()
        {
            return Task.Run(new Action(Reply));
        }

        public virtual void Reply()
        {
            EnsureConnectionIsOpen();

            var cmd = connection.CreateCommand();
            cmd.CommandText = "SEND ON CONVERSATION @handle MESSAGE TYPE [" + MessageType + "] (@message)";
            var message = cmd.CreateParameter();
            message.ParameterName = "message";
            message.Value = JsonConvert.SerializeObject(this);
            cmd.Parameters.Add(message);
            var handle = cmd.CreateParameter();
            handle.ParameterName = "handle";
            handle.Value = ConversationHandle;
            cmd.Parameters.Add(handle);
            cmd.ExecuteNonQuery();

            cmd = connection.CreateCommand();
            cmd.CommandText = "END CONVERSATION @handle";
            handle = cmd.CreateParameter();
            handle.ParameterName = "handle";
            handle.Value = ConversationHandle;
            cmd.Parameters.Add(handle);
            cmd.ExecuteNonQuery();
        }

        private void Merge(IDataReader reader)
        {
            var messageType = reader.GetString(reader.GetOrdinal("message_type_name"));
            if (messageType == MessageType)
            {
                var other = JsonConvert.DeserializeObject(reader.GetString(reader.GetOrdinal("message_body")), GetType());
                Merge((MessageBase)other);
            }
            else if (messageType == "http://schemas.microsoft.com/SQL/ServiceBroker/Error")
            {

                var doc = new XmlDocument();
                doc.LoadXml(reader.GetString(reader.GetOrdinal("message_body")).Substring(1));
                var xmlns = new XmlNamespaceManager(doc.NameTable);
                xmlns.AddNamespace("er", doc.DocumentElement.NamespaceURI);
                var other = new ErrorMessage()
                {
                    Message = doc.SelectSingleNode("/er:Error/er:Description/text()", xmlns).Value
                };
                throw new ErrorMessage.Exception(other);
            }
        }

        protected abstract void Merge(MessageBase message);

        #region IDisposable Members

        public void Dispose()
        {
            if (connection != null)
            {
                if (connection.State != ConnectionState.Closed)
                    connection.Close();
                connection.Dispose();
            }
        }

        #endregion


        public Guid CorrelationId { get; set; }

    }
}
