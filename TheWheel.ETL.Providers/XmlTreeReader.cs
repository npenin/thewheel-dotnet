using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;
using Newtonsoft.Json;
using TheWheel.ETL.Contracts;
using TheWheel.Domain;

namespace TheWheel.ETL.Providers
{
    public class XmlTreeReader : TreeReader
    {
        public XmlTreeReader()
        : base("xml:///", "TheWheel.ETL.Providers.Xml")
        {

        }

        public static async Task<DataProvider<XmlTreeReader, TreeOptions, ITransport<Stream>>> From<TTransport>(string connectionString, params KeyValuePair<string, object>[] parameters)
            where TTransport : ITransport<Stream>, new()
        {
            var provider = new DataProvider<XmlTreeReader, TreeOptions, ITransport<Stream>>();
            await provider.InitializeAsync(new TTransport());
            await provider.Transport.InitializeAsync(connectionString, parameters);
            return provider;
        }
        public override bool IsClosed => reader.ReadState != ReadState.Closed;

        public override bool EndOfStream => reader.EOF;

        private XmlReader reader;

        protected override void ConfigureInternal(bool reConfiguring)
        {
            reader = XmlReader.Create(this.BaseStream, new XmlReaderSettings
            {
                DtdProcessing = DtdProcessing.Ignore,
                CloseInput = true,
                IgnoreComments = true,
                IgnoreProcessingInstructions = true,
            });
        }

        protected override bool DocRead(ref int lastPosition, string subItemPath, Bag<string, object> subItem)
        {
            while (InternalDocRead())
            {
                switch (reader.NodeType)
                {
                    case XmlNodeType.Attribute:
                        QuickPath(subItemPath, subItem, "@" + reader.LocalName, reader.Value);
                        break;
                    case XmlNodeType.CDATA:
                    case XmlNodeType.Text:
                        QuickPath(subItemPath, subItem, "text()", reader.Value);

                        break;
                    case XmlNodeType.Comment:
                        break;
                    case XmlNodeType.Document:
                        break;
                    case XmlNodeType.DocumentFragment:
                        break;
                    case XmlNodeType.DocumentType:
                        break;
                    case XmlNodeType.Element:
                        OpenSegment(reader.LocalName + '/', ref lastPosition, subItem, ref subItemPath);

                        if (reader.IsEmptyElement)
                        {
                            if (CloseSegment(reader.LocalName + '/', ref subItem, ref lastPosition, subItemPath))
                                return true;
                        }
                        break;
                    case XmlNodeType.EndElement:

                        if (CloseSegment(reader.LocalName + '/', ref subItem, ref lastPosition, subItemPath))
                            return true;
                        break;
                    case XmlNodeType.EndEntity:
                        break;
                    case XmlNodeType.Entity:
                        break;
                    case XmlNodeType.EntityReference:
                        break;
                    case XmlNodeType.None:
                        break;
                    case XmlNodeType.Notation:
                        break;
                    case XmlNodeType.ProcessingInstruction:
                        break;
                    case XmlNodeType.SignificantWhitespace:
                        break;
                    case XmlNodeType.Whitespace:
                        break;
                    case XmlNodeType.XmlDeclaration:
                        break;
                    default:
                        break;
                }
            }
            return false;
        }

        private bool InternalDocRead()
        {
            switch (reader.NodeType)
            {
                case XmlNodeType.Attribute:
                    return reader.MoveToNextAttribute() || reader.Read();
                case XmlNodeType.Element:
                    return reader.MoveToFirstAttribute() || reader.Read();
                default:
                    return reader.Read();
            }
        }
    }
}


