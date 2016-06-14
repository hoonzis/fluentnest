using System;
using System.Linq;
using System.Text;
using FluentNest.Tests.Model;
using Nest;

namespace FluentNest.Tests
{
    public class TestsBase
    {
        protected ElasticClient Client;

        public TestsBase(params Func<ConnectionSettings, ConnectionSettings>[] additionalSettings)
        {
            var node = new Uri("http://localhost:9200");

            var settings = new ConnectionSettings(
                node,
                defaultIndex: "my-application"
                );

            settings = additionalSettings.Aggregate(settings, (current, newSetting) => newSetting(current));

            Client = new ElasticClient(settings);
        }

        public void AddSimpleTestData()
        {
            Client.DeleteIndex(x => x.Index<Car>());
            Client.CreateIndex(c => c.Index<Car>().AddMapping<Car>(x => x
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
                    ConditionalRanking = i%2 ==0 ? null : (int?)i,
                    Description = "Desc" + i,
                };

                var json = Encoding.UTF8.GetString(Client.Serializer.Serialize(car));
                Console.WriteLine(json);
                Client.Index(car);
            }
            Client.Flush(x => x.Index<Car>());
        }
    }
}
