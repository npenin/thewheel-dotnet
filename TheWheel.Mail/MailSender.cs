using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;

namespace TheWheel.Mail
{
    public class MailSender
    {
        private readonly string connectionString;

        public MailSender(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public string ProfileName { get; set; }

        public void Send(MailMessage mail)
        {
            if (mail.Attachments.Count > 0)
                throw new NotSupportedException("Attachments are not supported (yet?)");
            SqlConnection connection = new SqlConnection(connectionString);
            var cmd = connection.CreateCommand();
            cmd.CommandText = "sp_send_dbmail";
            cmd.CommandType = System.Data.CommandType.StoredProcedure;
            var param = cmd.CreateParameter();
            param.ParameterName = "profile_name";
            param.Value = ProfileName;

            param = cmd.CreateParameter();
            param.ParameterName = "recipients";

            param.Value = AddressList(mail.To);

            param = cmd.CreateParameter();
            param.ParameterName = "copy_recipients";
            param.Value = AddressList(mail.CC);

            param = cmd.CreateParameter();
            param.ParameterName = "blind_copy_recipients";
            param.Value = AddressList(mail.Bcc);

            param = cmd.CreateParameter();
            param.ParameterName = "from_address";
            param.Value = mail.From.ToString();

            param = cmd.CreateParameter();
            param.ParameterName = "reply_to";
            if (mail.ReplyToList.Count > 1)
                throw new NotSupportedException("SQL Server only accepts 1 address as a reply to");
            param.Value = mail.ReplyToList.FirstOrDefault().ToString() ?? "";

            param = cmd.CreateParameter();
            param.ParameterName = "subject";
            param.Value = mail.Subject;
            param = cmd.CreateParameter();
            param.ParameterName = "body";
            param.Value = mail.Body;
            param = cmd.CreateParameter();
            param.ParameterName = "body_format";
            param.Value = mail.IsBodyHtml ? "HTML" : "TEXT";

            param = cmd.CreateParameter();
            param.ParameterName = "importance";
            param.Value = mail.Priority.ToString();
            if (mail.Headers["Sensitivity"] != null)
            {
                param = cmd.CreateParameter();
                param.ParameterName = "sensitivity";
                param.Value = mail;
            }

            param = cmd.CreateParameter();
            param.ParameterName = "mailitem_id";
            param.Direction = System.Data.ParameterDirection.Output;
        }

        private string AddressList(MailAddressCollection addresses)
        {
            StringBuilder sb = new StringBuilder();
            bool isFirst = true;
            foreach (var recipient in addresses)
            {
                if (isFirst)
                    isFirst = false;
                else
                    sb.Append(';');
                sb.Append(recipient);
            }
            return sb.ToString();
        }
    }

}
