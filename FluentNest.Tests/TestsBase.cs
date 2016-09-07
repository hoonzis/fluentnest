using System;
using System.CodeDom;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Elasticsearch.Net;
using FluentNest.Tests.Model;
using Nest;

namespace Tests
{
    public class TestsBase
    {
        protected ElasticClient Client;
        protected IndexName CarIndex = Infer.Index<Car>();

        public TestsBase(Func<ConnectionSettings, IElasticsearchSerializer> serializerFactory = null, Func<ConnectionSettings, ConnectionSettings> additionalSettings = null)
        {
            var node = new Uri("http://localhost:9200");
            var connectionPool = new SingleNodeConnectionPool(node);

            var settings = new ConnectionSettings(connectionPool, serializerFactory).DefaultIndex("my-application" + Guid.NewGuid());
            if (additionalSettings != null)
            {
                settings = additionalSettings(settings);
            }

            Client = new ElasticClient(settings);
        }

        public string Serialize<T>(T entity)
        {
            using (var ms = new MemoryStream())
            {
                Client.Serializer.Serialize(entity, ms);
                return Encoding.UTF8.GetString(ms.ToArray());
            }
        }
    }
}
