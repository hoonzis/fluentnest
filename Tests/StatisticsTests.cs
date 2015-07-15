using System;
using FluentNest;
using Nest;
using NFluent;
using TestModel;
using Xunit;

namespace Tests
{
    public class StatisticsTests
    {
        private ElasticClient client;

        public StatisticsTests()
        {
            var node = new Uri("http://localhost:9600");

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
                    Name = "Car" + i,
                    Price = 10,
                    Sold = i % 2 == 0 ? true : false,
                    Length = i,
                    CarType = "Car" + i%2
                };
                client.Index(car);
            }
            client.Flush(x => x.Index<Car>());
        }

        [Fact]
        public void TestSimpleSum()
        {

            AddSimpleTestData();
            var standardSum = Sums.SumOnField<Car>(x => x.Price);
            var result =
                client.Search<Car>(
                    search =>
                        search.Take(10).Aggregations(x => standardSum));

            var sum = result.Aggs.GetSum<Car>(x => x.Price).Value;
            Check.That(sum).Equals(100d);
        }

        [Fact]
        public void TestConditionalSum()
        {
            AddSimpleTestData();
            var sumCond = Sums.ConditionalSumOnField<Car>(x => x.Price, x => x.Sold == true);

            var result =
                client.Search<Car>(
                    search =>
                        search.Take(10)
                        .Aggregations(x => sumCond));

            var sum = result.Aggs.GetConditionalSum<Car>(x => x.Price, x => x.Sold);
            Check.That(sum).Equals(50d);
        }

        [Fact]
        public void MultipleAggregationsInSingleAggregation()
        {
            AddSimpleTestData();
            var notionalSumAgg = Sums.SumOnField<Car>(x => x.Price);

            var result = client.Search<Car>(s => s
                .Take(100)
                .Aggregations(a => notionalSumAgg.AndAvgBy(x => x.Length).AndCountBy(x=>x.CarType)));

            var priceSum = result.Aggs.GetSum<Car>(x => x.Price);
            var avgLength = result.Aggs.GetAvg<Car>(x => x.Length);
            var count = result.Aggs.GetCount<Car>(x => x.CarType);

            Check.That(priceSum).Equals(100d);
            Check.That(avgLength).Equals(4.5d);
            Check.That(count).Equals(10d);
        }
    }
}
