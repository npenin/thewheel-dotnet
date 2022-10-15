using System.Threading;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;
using System.Net;
using System.Data;

namespace TheWheel.ETL.Provider.Mail
{

    public class AttachmentReference : FieldReference
    {
        public readonly FieldReference FileName;
        public readonly FieldReference Bytes;
        public readonly FieldReference ContentType;

        public AttachmentReference(string name) : base(name)
        {
        }

        public AttachmentReference(int index) : base(index)
        {
        }

        public AttachmentReference() : base(null) { }
    }

    public class MailReceiverOptions
    {
        public readonly FieldReference Subject;
        public readonly FieldReference From;
        public readonly MimeKit.InternetAddressList StaticFrom;
        public readonly FieldReference ReplyTo;
        public readonly FieldReference To;
        public readonly MimeKit.InternetAddressList StaticTo;
        public readonly FieldReference CCs;
        public readonly MimeKit.InternetAddressList StaticCCs;
        public readonly FieldReference Bccs;
        public readonly MimeKit.InternetAddressList StaticBccs;
        public readonly FieldReference TextBody;
        public readonly string StaticTextBody;
        public readonly FieldReference HtmlBody;
        public readonly string StaticHtmlBody;
        public readonly MimeKit.BodyBuilder StaticBodyBuilder;
        public readonly AttachmentReference[] Attachments;

        public readonly string connectionUri;

        public readonly ICredentials credentials;
    }

    public class MailReceiver : IDataReceiver<MailReceiverOptions>, ITransportable<SmtpClientTransport>
    {
        public SmtpClientTransport Transport { get; private set; }

        public async Task ReceiveAsync(IDataProvider provider, MailReceiverOptions query, CancellationToken token)
        {
            if (query.credentials != null)
                await Transport.InitializeAsync(query.connectionUri, token, new System.Collections.Generic.KeyValuePair<string, object>("Credentials", query.credentials));
            else
                await Transport.InitializeAsync(query.connectionUri, token);

            using (var client = await Transport.GetStreamAsync(token))
            using (var reader = await provider.ExecuteReaderAsync(token))
            {
                while (reader.Read())
                {
                    var message = new MimeKit.MimeMessage();
                    message.From.AddRange(MimeKit.InternetAddressList.Parse(query.From.GetString(reader)));
                    if (query.ReplyTo != null && query.ReplyTo.IsDefined)
                        message.ReplyTo.AddRange(MimeKit.InternetAddressList.Parse(query.ReplyTo.GetString(reader)));
                    message.To.AddRange(MimeKit.InternetAddressList.Parse(query.To.GetString(reader)));
                    if (query.CCs != null && query.CCs.IsDefined)
                        message.Cc.AddRange(MimeKit.InternetAddressList.Parse(query.CCs.GetString(reader)));
                    if (query.Bccs != null && query.Bccs.IsDefined)
                        message.Bcc.AddRange(MimeKit.InternetAddressList.Parse(query.Bccs.GetString(reader)));

                    MimeKit.BodyBuilder bodyBuilder;
                    if (query.StaticBodyBuilder != null)
                        bodyBuilder = query.StaticBodyBuilder;
                    else
                        bodyBuilder = new MimeKit.BodyBuilder();

                    if (query.TextBody != null && query.TextBody.IsDefined)
                        bodyBuilder.TextBody = query.TextBody.GetString(reader);
                    if (query.HtmlBody != null && query.HtmlBody.IsDefined)
                        bodyBuilder.HtmlBody = query.HtmlBody.GetString(reader);

                    if (query.Attachments != null)
                        foreach (var attachment in query.Attachments)
                        {
                            var attachmentReader = reader;
                            if (attachment.IsDefined)
                                attachmentReader = attachment.GetData(reader);

                            var buffer = new byte[attachment.GetBytes(reader, 0, null, 0, 0)];

                            for (long offset = 0, readLength = -1; offset < buffer.LongLength; offset += readLength)
                                readLength = attachment.Bytes.GetBytes(reader, offset, buffer, (int)offset, (int)(buffer.LongLength - offset));

                            var ms = new System.IO.MemoryStream(buffer);

                            await bodyBuilder.Attachments.AddAsync(attachment.FileName.GetString(attachmentReader), ms, MimeKit.ContentType.Parse(attachment.ContentType.GetString(reader)), token);
                        }

                    message.Body = bodyBuilder.ToMessageBody();

                    await client.SendAsync(message, token);
                }
            }
        }
    }

}