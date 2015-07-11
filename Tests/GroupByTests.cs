using System;
using FluentNest;
using Nest;
using NFluent;
using TestModel;
using Xunit;

namespace Tests
{
    public class GroupByTests
    {
        private ElasticClient client;


        public GroupByTests()
        {
            var node = new Uri("http://localhost:9200");

            var settings = new ConnectionSettings(
                node,
                defaultIndex: "my-application"
            );

            client = new ElasticClient(settings);
        }

        private void AddSimpleTestData()
        {
            client.DeleteIndex(x => x.Index<Car>());
            for (int i = 0; i < 10; i++)
            {
                var car = new Car
                {
                    Timestamp = new DateTime(2010,i+1,1),
                    Name = "Car" + i,
                    Price = 10,
                    Sold = i % 2 == 0 ? true : false,
                    CarType = "Type" + i%3,
                    EngineType = "Engine" + i%2
                };
                client.Index(car);
            }
            client.Flush(x => x.Index<Car>());
        }

        [Fact]
        public void NestedGroupBy()
        {
            AddSimpleTestData();
            var sumOnPrice = Sums.SumOnField<Car>(s => s.Price);

            var result =
                client.Search<Car>(
                    search =>
                        search.Aggregations(x => sumOnPrice.GroupBy(s => s.EngineType).GroupBy(b => b.CarType)));

            
            var carTypes = result.Aggs.GetGroupBy<Car>(x => x.CarType);
            Check.That(carTypes).HasSize(3);
            foreach (var carType in carTypes)
            {
                var engineTypes = carType.GetGroupBy<Car>(x => x.EngineType);
                Check.That(engineTypes).HasSize(2);
            }
        }

        [Fact]
        public void GetDictionaryFromGroupBy()
        {
            AddSimpleTestData();
            var sumOnPrice = Sums.SumOnField<Car>(s => s.Price);

            var result =
                client.Search<Car>(search => search.Aggregations(x => sumOnPrice.GroupBy(s => s.EngineType)));


            var carTypes = result.Aggs.GetDictioanry<Car>(x => x.EngineType);
            Check.That(carTypes).HasSize(2);
            Check.That(carTypes.Keys).ContainsExactly("engine0", "engine1");
        }
    }
}
