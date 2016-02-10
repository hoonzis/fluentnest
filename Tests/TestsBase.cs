using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Elasticsearch.Net;
using Nest;
using static Nest.Infer;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using TestModel;

namespace Tests
{
    public class TestsBase
    {
        protected ElasticClient client;

        public TestsBase(Func<ConnectionSettings, IElasticsearchSerializer> serializerFactory = null)
        {
            var node = new Uri("http://localhost:9200");
            var connectionPool = new SingleNodeConnectionPool(node);

            var settings = new ConnectionSettings(connectionPool, serializerFactory).DefaultIndex("my-application");

            client = new ElasticClient(settings);
        }

        public void AddSimpleTestData()
        {
            client.DeleteIndex(Index<Car>());
            client.CreateIndex(Index<Car>(), x => x.Mappings(
                m => m.Map<Car>(t => t
            .Properties(prop => prop.String(str => str.Name(s => s.EngineType).Index(FieldIndexOption.NotAnalyzed))))));

            for (int i = 0; i < 10; i++)
            {
                var car = new Car
                {
                    Timestamp = new DateTime(2010, i + 1, 1),
                    Name = "Car" + i,
                    Price = 10,
                    Sold = i % 2 == 0 ? true : false,
                    CarType = "Type" + i % 3,
                    Length = i,
                    EngineType = i % 2 == 0 ? EngineType.Diesel : EngineType.Standard,
                    Weight = 5,
                    ConditionalRanking = i%2 ==0 ? null : (int?)i
                };

                using (var ms = new MemoryStream())
                {
                    client.Serializer.Serialize(car, ms);
                    Console.WriteLine(Encoding.UTF8.GetString(ms.ToArray()));
                }
                client.Index(car);
            }
            client.Flush(Index<Car>());
        }
    }
}
