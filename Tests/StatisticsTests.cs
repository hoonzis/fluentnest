using System;
using FluentNest;
using NFluent;
using TestModel;
using Xunit;

namespace Tests
{
    public class StatisticsTests :TestsBase
    {
        [Fact]
        public void SumTest()
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
        public void CountTest()
        {

            AddSimpleTestData();
            var count = Statistics.CountBy<Car>(x => x.Price);
            var result =
                client.Search<Car>(
                    search =>
                        search.Take(10).Aggregations(x => count));

            var val = result.Aggs.GetCount<Car>(x => x.Price);
            Check.That(val).Equals(10);
        }

        [Fact]
        public void CardinalityTest()
        {

            AddSimpleTestData();
            var cardAgg = Statistics.CardinalityBy<Car>(x => x.EngineType);
            var result =
                client.Search<Car>(
                    search =>
                        search.Take(10).Aggregations(x => cardAgg));

            var card = result.Aggs.GetCardinality<Car>(x => x.EngineType);
            Check.That(card).Equals(2);
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
            var engineTypeSum = Statistics.CondCountBy<Car>(x => x.Name, c => c.EngineType == EngineType.Diesel);

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
                .AndCondCountBy(x => x.Name, c => c.EngineType == EngineType.Diesel)
                .AndCondSumBy(x => x.Price, c => c.CarType == "type1");


            var result = client.Search<Car>(s => s
                .Take(100)
                .Aggregations(x => agg));
            
            var priceSum = result.Aggs.GetSum<Car, Decimal>(x => x.Price);
            var avgLength = result.Aggs.GetAvg<Car>(x => x.Length);
            var count = result.Aggs.GetCount<Car>(x => x.CarType);
            var typeOneCount = result.Aggs.GetCondCount<Car>(x => x.Name, x => x.EngineType);
            var car1PriceSum = result.Aggs.GetCondSum<Car>(x => x.Price, x => x.CarType);

            var aggsContainer = result.Aggs.AsContainer<Car>();
            var priceSum2 = aggsContainer.GetSum(x => x.Price);
            var avgLength2 = aggsContainer.GetAvg(x => x.Length);
            var count2 = aggsContainer.GetCount(x => x.CarType);
            var typeOneCount2 = aggsContainer.GetCondCount(x => x.Name, x => x.EngineType);
            var car1PriceSum2 = aggsContainer.GetCondSum(x => x.Price, x => x.CarType);


            Check.That(priceSum).Equals(100m);
            Check.That(avgLength).Equals(4.5d);
            Check.That(count).Equals(10);
            Check.That(typeOneCount).Equals(5);
            Check.That(car1PriceSum).Equals(30d);

            Check.That(priceSum2).Equals(100m);
            Check.That(avgLength2).Equals(4.5d);
            Check.That(count2).Equals(10);
            Check.That(typeOneCount2).Equals(5);
            Check.That(car1PriceSum2).Equals(30d);
        }

        [Fact]
        public void SumOfNullableDecimal()
        {
            AddSimpleTestData();
            var standardSum = Statistics.SumBy<Car>(x => x.Weight);
            var result =
                client.Search<Car>(
                    search =>
                        search.Take(10).Aggregations(x => standardSum));

            var sum = result.Aggs.GetSum<Car,decimal?>(x => x.Weight);

            var container = result.Aggs.AsContainer<Car>();

            var sum2 = container.GetSum(x => x.Weight);

            Check.That(sum).Equals(50m);
            Check.That(sum2).Equals(50m);
        }
    }
}
