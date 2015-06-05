using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TheWheel.ServiceBus
{
    public class ErrorMessage : MessageBase
    {
        [Serializable]
        public class Exception : System.Exception
        {
            public Exception(params ErrorMessage[] errors)
            {
                Errors = errors;
            }

            public ErrorMessage[] Errors { get; set; }

            public override string Message
            {
                get
                {
                    if (Errors != null && Errors.Length > 0)
                        return Errors[0].Message;
                    return base.Message;
                }
            }
        }

        public static string Type
        {
            get
            {
                return new ErrorMessage().MessageType;
            }
        }

        public ErrorMessage(Guid conversationHandle, MessageBase other, System.Exception ex)
            : base(other)
        {
            Message = ex.ToString();
        }

        internal ErrorMessage()
            : base()
        {

        }

        protected internal override bool IsOneWay
        {
            get { return true; }
        }

        protected override void Merge(MessageBase message)
        {
            throw new NotImplementedException();
        }

        public string Message { get; set; }

        public sealed override void Reply()
        {
            EnsureConnectionIsOpen();

            var cmd = connection.CreateCommand();
            cmd.CommandText = "END CONVERSATION @handle WITH ERROR= 500 DESCRIPTION=@error";
            var handle = cmd.CreateParameter();
            handle.ParameterName = "handle";
            handle.Value = ConversationHandle;
            cmd.Parameters.Add(handle);

            var error = cmd.CreateParameter();
            error.ParameterName = "error";
            error.Value = Message;
            cmd.Parameters.Add(error);

            cmd.ExecuteNonQuery();
        }
    }
}
