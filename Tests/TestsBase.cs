using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nest;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using TestModel;

namespace Tests
{
    public class TestsBase
    {
        protected ElasticClient client;

        public TestsBase(params Func<ConnectionSettings, ConnectionSettings>[] additionalSettings)
        {
            var node = new Uri("http://localhost:9200");

            var settings = new ConnectionSettings(
                node,
                defaultIndex: "my-application"
                );

            settings = additionalSettings.Aggregate(settings, (current, newSetting) => newSetting(current));

            client = new ElasticClient(settings);
        }

        public void AddSimpleTestData()
        {
            client.DeleteIndex(x => x.Index<Car>());
            client.CreateIndex(c => c.Index<Car>().AddMapping<Car>(x => x
            .Properties(prop => prop.String(str => str.Name(s => s.EngineType).Index(FieldIndexOption.NotAnalyzed)))));

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

                var json = Encoding.UTF8.GetString(client.Serializer.Serialize(car));
                Console.WriteLine(json);
                client.Index(car);
            }
            client.Flush(x => x.Index<Car>());
        }
    }
}
