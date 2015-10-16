using System;
using Nest;
using TestModel;

namespace Tests
{
    public class TestsBase
    {
        protected ElasticClient client;

        public TestsBase()
        {
            var node = new Uri("http://localhost:9600");

            var settings = new ConnectionSettings(
                node,
                defaultIndex: "my-application"
            );

            client = new ElasticClient(settings);
        }
        public void AddSimpleTestData()
        {
            client.DeleteIndex(x => x.Index<Car>());
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
                    Weight = 5
                };
                client.Index(car);
            }
            client.Flush(x => x.Index<Car>());
        }
    }
}
