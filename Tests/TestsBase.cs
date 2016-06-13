using System;
using System.CodeDom;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Elasticsearch.Net;
using Nest;
using TestModel;

namespace Tests
{
    public class TestsBase
    {
        protected ElasticClient client;
        protected IndexName CarIndex = Infer.Index<Car>();

        public TestsBase(Func<ConnectionSettings, IElasticsearchSerializer> serializerFactory = null)
        {
            var node = new Uri("http://localhost:9200");
            var connectionPool = new SingleNodeConnectionPool(node);

            var settings = new ConnectionSettings(connectionPool, serializerFactory).DefaultIndex("my-application");

            client = new ElasticClient(settings);
        }

        public void EntityToConsole<T>(T entity)
        {
            using (var ms = new MemoryStream())
            {
                client.Serializer.Serialize(entity, ms);
                Console.WriteLine(Encoding.UTF8.GetString(ms.ToArray()));
            }
        }
    }
}
