using System;
using System.Linq;
using FluentNest;
using Nest;
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
            var standardSum = new AggregationDescriptor<Car>().SumBy(x => x.Price);
            var result =
                client.Search<Car>(
                    search =>
                        search.Take(10).Aggregations(x => standardSum));

            var sum = result.Aggs.GetSum<Car, Decimal>(x => x.Price);
            Check.That(sum).Equals(100m);
        }


        [Fact]
        public void ConditionalStats_Without_FluentNest()
        {
            AddSimpleTestData();
            var result = client.Search<Car>(search => search
                .Aggregations(agg => agg
                    .Filter("filterOne", f => f.Filter(innerFilter => innerFilter.Term(fd => fd.EngineType, EngineType.Diesel))
                    .Aggregations(innerAgg => innerAgg.Sum("sumAgg", innerField => 
                        innerField.Field(field => field.Price)))
                    )
                    .Filter("filterTwo", f => f.Filter(innerFilter => innerFilter.Term(fd => fd.CarType, "type1"))
                    .Aggregations(innerAgg => innerAgg.Sum("sumAgg", innerField =>
                        innerField.Field(field => field.Price)))
                    )
                )
            );

            var sumAgg = result.Aggs.Filter("filterOne");
            Check.That(sumAgg).IsNotNull();
            var sumValue = sumAgg.Sum("sumAgg");
            Check.That(sumValue.Value).Equals(50d);

            var sumAgg2 = result.Aggs.Filter("filterTwo");
            Check.That(sumAgg2).IsNotNull();
            var sumValue2 = sumAgg2.Sum("sumAgg");
            Check.That(sumValue2.Value).Equals(30d);
        }

        [Fact]
        public void CountTest()
        {
            AddSimpleTestData();
            var count = new AggregationDescriptor<Car>().CountBy(x => x.Price);
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
            var cardAgg = new AggregationDescriptor<Car>().CardinalityBy(x => x.EngineType);
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
            AggregationDescriptor<Car> sumCond = new AggregationDescriptor<Car>().SumBy(x => x.Price, x => x.Sold == true);

            var result =
                client.Search<Car>(
                    search =>
                        search.Take(10)
                        .Aggregations(x => sumCond));

            var sum = result.Aggs.GetSum<Car,decimal>(x => x.Price, x => x.Sold == true);
            Check.That(sum).Equals(50m);
        }

        [Fact]
        public void MultipleAggregationsInSingleAggregation()
        {
            AddSimpleTestData();
            var engineTypeSum = new AggregationDescriptor<Car>().CountBy(x => x.Name, c => c.EngineType == EngineType.Diesel);

            var notionalSumAgg = engineTypeSum.SumBy(x => x.Price)
                .AverageBy(x => x.Length)
                .CountBy(x => x.CarType)
                .CardinalityBy(x => x.EngineType);

            var result = client.Search<Car>(s => s
                .Take(100)
                .Aggregations(x => notionalSumAgg));

            var priceSum = result.Aggs.GetSum<Car,decimal>(x => x.Price);
            var avgLength = result.Aggs.GetAverage<Car,double>(x => x.Length);
            var count = result.Aggs.GetCount<Car>(x => x.CarType);
            var typeOneCount = result.Aggs.GetCount<Car>(x => x.Name, x => x.EngineType == EngineType.Diesel);
            var engineCardinality = result.Aggs.GetCardinality<Car>(x => x.EngineType);
            
            Check.That(priceSum).Equals(100m);
            Check.That(avgLength).Equals(4.5d);
            Check.That(count).Equals(10);
            Check.That(typeOneCount).Equals(5);
            Check.That(engineCardinality).Equals(2);
        }

        [Fact]
        public void MultipleAggregationsInSingleAggregation_ReversingOrder()
        {
            AddSimpleTestData();
            var agg = new AggregationDescriptor<Car>().SumBy(x => x.Price)
                .AverageBy(x => x.Length)
                .CountBy(x => x.CarType)
                .CountBy(x => x.Name, c => c.EngineType == EngineType.Diesel)
                .SumBy(x => x.Price, c => c.CarType == "type1");

            var result = client.Search<Car>(s => s
                .Take(100)
                .Aggregations(x => agg));
            
            var priceSum = result.Aggs.GetSum<Car, decimal>(x => x.Price);
            var avgLength = result.Aggs.GetAverage<Car,double>(x => x.Length);
            var count = result.Aggs.GetCount<Car>(x => x.CarType);
            var typeOneCount = result.Aggs.GetCount<Car>(x => x.Name, x => x.EngineType == EngineType.Diesel);
            var car1PriceSum = result.Aggs.GetSum<Car,decimal>(x => x.Price, x => x.CarType == "type1");

            var aggsContainer = result.Aggs.AsContainer<Car>();
            var priceSum2 = aggsContainer.GetSum(x => x.Price);
            var avgLength2 = aggsContainer.GetAverage(x => x.Length);
            var count2 = aggsContainer.GetCount(x => x.CarType);
            var typeOneCount2 = aggsContainer.GetCount(x => x.Name, x => x.EngineType == EngineType.Diesel);
            var car1PriceSum2 = aggsContainer.GetSum(x => x.Price, x => x.CarType == "type1");


            Check.That(priceSum).Equals(100m);
            Check.That(avgLength).Equals(4.5d);
            Check.That(count).Equals(10);
            Check.That(typeOneCount).Equals(5);
            Check.That(car1PriceSum).Equals(30m);

            Check.That(priceSum2).Equals(100m);
            Check.That(avgLength2).Equals(4.5d);
            Check.That(count2).Equals(10);
            Check.That(typeOneCount2).Equals(5);
            Check.That(car1PriceSum2).Equals(30m);
        }

        [Fact]
        public void SumOfNullableDecimal()
        {
            AddSimpleTestData();
            var standardSum = new AggregationDescriptor<Car>().SumBy(x => x.Weight);
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

        [Fact]
        public void Condition_Equals_Not_Null_Test()
        {
            AddSimpleTestData();
            var standardSum = new AggregationDescriptor<Car>().SumBy(x => x.Weight,x=>x.ConditionalRanking.HasValue);

            var result =
                client.Search<Car>(
                    search =>
                        search.Take(10).Aggregations(x => standardSum));

            var sum = result.Aggs.GetSum<Car,decimal?>(x => x.Weight, c => c.ConditionalRanking.HasValue);

            var container = result.Aggs.AsContainer<Car>();

            var sum2 = container.GetSum(x => x.Weight,c=>c.ConditionalRanking.HasValue);

            Check.That(sum).Equals(25m);
            Check.That(sum2).Equals(25m);
        }

        [Fact]
        public void Two_Conditional_Sums_Similar_Condition_One_More_Restrained()
        {
            AddSimpleTestData();
            var aggs = new AggregationDescriptor<Car>()
                .SumBy(x => x.Weight, x => x.ConditionalRanking.HasValue)
                .SumBy(x => x.Weight, x => x.ConditionalRanking.HasValue && x.CarType == "Type1");

            var result =
                client.Search<Car>(
                    search =>
                        search.Take(10).Aggregations(x => aggs));

            var sum = result.Aggs.GetSum<Car,decimal?>(x => x.Weight, c => c.ConditionalRanking.HasValue);
            var sum2 = result.Aggs.GetSum<Car, decimal?>(x => x.Weight, c => c.ConditionalRanking.HasValue && c.CarType == "Type1");

            Check.That(sum).Equals(25m);
            Check.That(sum2).Equals(0m);
        }

        [Fact]
        public void Percentiles_Test()
        {
            AddSimpleTestData();
            
            var result =
                client.Search<Car>(
                    search =>
                        search.Take(10).Aggregations(agg => agg.PercentilesBy(x=>x.Price)));

            var percentiles = result.Aggs.GetPercentile<Car>(x => x.Price);
            Check.That(percentiles).HasSize(7);
            Check.That(percentiles.Single(x => x.Percentile == 50.0).Value).Equals(10d);
        }

        [Fact]
        public void Max_Test()
        {
            AddSimpleTestData();

            var result =
                client.Search<Car>(
                    search =>
                        search.Take(10).Aggregations(agg => agg.MaxBy(x=>x.Length)));

            var container = result.Aggs.AsContainer<Car>();
            var max = container.GetMax(x => x.Length);
            Check.That(max).Equals(9d);
        }

        [Fact]
        public void MinTest()
        {
            AddSimpleTestData();

            var result =
                client.Search<Car>(
                    search =>
                        search.Take(10).Aggregations(agg => agg.MinBy(x => x.Length)));

            var container = result.Aggs.AsContainer<Car>();
            var min = container.GetMin(x => x.Length);
            Check.That(min).Equals(0d);
        }
    }
}
