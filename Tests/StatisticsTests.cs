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
                    CarType = "Car" + i%2,
                    EngineType = "Engine" + i%2
                };
                client.Index(car);
            }
            client.Flush(x => x.Index<Car>());
        }

        [Fact]
        public void TestSimpleSum()
        {

            AddSimpleTestData();
            var standardSum = Statistics.SumBy<Car>(x => x.Price);
            var result =
                client.Search<Car>(
                    search =>
                        search.Take(10).Aggregations(x => standardSum));

            var sum = result.Aggs.GetSum<Car, Decimal>(x => x.Price);
            Check.That(sum).Equals(100m);
        }

        [Fact]
        public void TestConditionalSum()
        {
            AddSimpleTestData();
            var sumCond = Statistics.CondSumBy<Car>(x => x.Price, x => x.Sold == true);

            var result =
                client.Search<Car>(
                    search =>
                        search.Take(10)
                        .Aggregations(x => sumCond));

            var sum = result.Aggs.GetCondSum<Car>(x => x.Price, x => x.Sold);
            //getting the cond sum without specifying the condition
            var sumTwo = result.Aggs.GetCondSum<Car>(x => x.Price);
            Check.That(sum).Equals(50d);
            Check.That(sumTwo).Equals(50d);
        }

        [Fact]
        public void MultipleAggregationsInSingleAggregation()
        {
            AddSimpleTestData();
            var engineTypeSum = Statistics.CondCountBy<Car>(x => x.Name, c => c.EngineType == "Engine1");

            var notionalSumAgg = engineTypeSum.AndSumBy(x => x.Price)
                .AndAvgBy(x => x.Length)
                .AndCountBy(x => x.CarType)
                .AndCardinalityBy(x => x.EngineType);


            var result = client.Search<Car>(s => s
                .Take(100)
                .Aggregations(x => notionalSumAgg));
                    

            var priceSum = result.Aggs.GetSum<Car,Decimal>(x => x.Price);
            var avgLength = result.Aggs.GetAvg<Car>(x => x.Length);
            var count = result.Aggs.GetCount<Car>(x => x.CarType);
            var typeOneCount = result.Aggs.GetCondCount<Car>(x => x.Name, x => x.EngineType);
            var engineCardinality = result.Aggs.GetCardinality<Car>(x => x.EngineType);

            //we can get back cond count without specifying the condition - in that case it will return the first one
            var typeOneCountAgain = result.Aggs.GetCondCount<Car>(x => x.Name);
            
            Check.That(priceSum).Equals(100m);
            Check.That(avgLength).Equals(4.5d);
            Check.That(count).Equals(10);
            Check.That(typeOneCount).Equals(5);
            Check.That(typeOneCountAgain).Equals(5);
            Check.That(engineCardinality).Equals(2);
        }

        [Fact]
        public void MultipleAggregationsInSingleAggregation_ReversingOrder()
        {
            AddSimpleTestData();
            var agg = Statistics.SumBy<Car>(x => x.Price)
                .AndAvgBy(x => x.Length)
                .AndCountBy(x => x.CarType)
                .AndCondCountBy(x => x.Name, c => c.EngineType == "Engine1")
                .AndCondSumBy(x => x.Price, c => c.CarType == "Car1");


            var result = client.Search<Car>(s => s
                .Take(100)
                .Aggregations(x => agg));


            var priceSum = result.Aggs.GetSum<Car, Decimal>(x => x.Price);
            var avgLength = result.Aggs.GetAvg<Car>(x => x.Length);
            var count = result.Aggs.GetCount<Car>(x => x.CarType);
            var typeOneCount = result.Aggs.GetCondCount<Car>(x => x.Name, x => x.EngineType);
            var car1PriceSum = result.Aggs.GetCondSum<Car>(x => x.Price, x => x.CarType);

            Check.That(priceSum).Equals(100m);
            Check.That(avgLength).Equals(4.5d);
            Check.That(count).Equals(10);
            Check.That(typeOneCount).Equals(5);
            Check.That(car1PriceSum).Equals(50d);
        }
    }
}
