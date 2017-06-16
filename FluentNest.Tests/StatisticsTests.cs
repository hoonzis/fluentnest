using System;
using System.Linq;
using FluentNest.Tests.Model;
using Nest;
using NFluent;
using Tests;
using Xunit;

namespace FluentNest.Tests
{
    public class StatisticsTests : TestsBase
    {
        public string AddSimpleTestData()
        {
            var indexName = "index_" + Guid.NewGuid();
            Client.CreateIndex(indexName, x => x.Mappings(
                m => m.Map<Car>(t => t
            .Properties(prop => prop.Keyword(str => str.Name(s => s.EngineType)))
            .Properties(prop => prop.Text(str => str.Name(s => s.CarType).Fielddata()))
            .Properties(prop => prop.Text(str => str.Name(s => s.Name).Fielddata()))
            )));

            for (int i = 0; i < 10; i++)
            {
                var car = new Car
                {
                    Id = Guid.NewGuid(),
                    Timestamp = new DateTime(2010, i + 1, 1),
                    Name = "Car" + i,
                    Price = 10,
                    Sold = i % 2 == 0 ? true : false,
                    CarType = "Type" + i % 3,
                    Length = i,
                    EngineType = i % 2 == 0 ? EngineType.Diesel : EngineType.Standard,
                    Weight = 5,
                    ConditionalRanking = i % 2 == 0 ? null : (int?)i,
                    Description = "Desc" + i,
                    LastControlCheck = i % 3 == 0 ? new DateTime(2012, i + 1, 1) : (DateTime?)null
                };

                Client.Index(car, ind => ind.Index(indexName));
            }
            Client.Flush(indexName);
            return indexName;
        }
        
        [Fact]
        public void ConditionalStats_Without_FluentNest()
        {
            var index = AddSimpleTestData();
            var result = Client.Search<Car>(search => search.Index(index)
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
            Client.DeleteIndex(index);
        }

        [Fact]
        public void CountAndCardinalityTest()
        {
            var index = AddSimpleTestData();
            var result = Client.Search<Car>(sc => sc.Index(index).Aggregations(agg => agg
                .CountBy(x=>x.Price)
                .CardinalityBy(x => x.EngineType)
            ));
            var val = result.Aggs.GetCount<Car>(x => x.Price);
            Check.That(val).Equals(10);
            var card = result.Aggs.GetCardinality<Car>(x => x.EngineType);
            Check.That(card).Equals(2);
            Client.DeleteIndex(index);
        }

        [Fact]
        public void CardinalityFilterTest()
        {
            var index = AddSimpleTestData();
            var result = Client.Search<Car>(sc => sc.Index(index).Aggregations(agg => agg
                .CardinalityBy(x => x.EngineType, x => x.EngineType == EngineType.Standard)
            ));

            var container = result.Aggs.AsContainer<Car>();
            var card = result.Aggs.GetCardinality<Car>(x => x.EngineType, x => x.EngineType == EngineType.Standard);
            Check.That(card).Equals(1);
            Check.That(container.GetCardinality(x => x.EngineType, x => x.EngineType == EngineType.Standard)).Equals(1);
        }

        [Fact]
        public void TestConditionalSum()
        {
            var index = AddSimpleTestData();

            var result = Client.Search<Car>(sc => sc.Index(index).Aggregations(agg => agg
                .SumBy(x => x.Price, x => x.Sold == true)
            ));

            var sum = result.Aggs.GetSum<Car,decimal>(x => x.Price, x => x.Sold == true);
            Check.That(sum).Equals(50m);
            Client.DeleteIndex(index);
        }

        [Fact]
        public void MultipleAggregationsInSingleAggregation()
        {
            var index = AddSimpleTestData();
            
            var result = Client.Search<Car>(s => s.Index(index).Aggregations(agg => agg
                .CountBy(x => x.Name, c => c.EngineType == EngineType.Diesel)
                .SumBy(x => x.Price)
                .AverageBy(x => x.Length)
                .CountBy(x => x.CarType)
                .CardinalityBy(x => x.EngineType))
            );

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
            Client.DeleteIndex(index);
        }

        [Fact]
        public void MultipleAggregationsInSingleAggregation_ReversingOrder()
        {
            var index = AddSimpleTestData();
            
            var result = Client.Search<Car>(s => s.Index(index).Aggregations(agg => agg
                .SumBy(x => x.Price)
                .AverageBy(x => x.Length)
                .CountBy(x => x.CarType)
                .CountBy(x => x.Name, c => c.EngineType == EngineType.Diesel)
                .SumBy(x => x.Price, c => c.CarType == "type1")
            ));
            
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
            Client.DeleteIndex(index);
        }

        [Fact]
        public void SumOfNullableDecimal()
        {
            var index = AddSimpleTestData();
            var result = Client.Search<Car>(sc => sc.Index(index).Aggregations(agg => agg.SumBy(x => x.Weight)));
            var sum = result.Aggs.GetSum<Car,decimal?>(x => x.Weight);

            var container = result.Aggs.AsContainer<Car>();

            var sum2 = container.GetSum(x => x.Weight);

            Check.That(sum).Equals(50m);
            Check.That(sum2).Equals(50m);
            Client.DeleteIndex(index);
        }

        [Fact]
        public void Condition_Equals_Not_Null_Test()
        {
            var index = AddSimpleTestData();
            
            var result = Client.Search<Car>(sc => sc.Index(index).Aggregations(agg => agg
                .SumBy(x => x.Weight, x => x.ConditionalRanking.HasValue)
            ));

            var sum = result.Aggs.GetSum<Car,decimal?>(x => x.Weight, c => c.ConditionalRanking.HasValue);

            var container = result.Aggs.AsContainer<Car>();

            var sum2 = container.GetSum(x => x.Weight,c=>c.ConditionalRanking.HasValue);

            Check.That(sum).Equals(25m);
            Check.That(sum2).Equals(25m);
            Client.DeleteIndex(index);
        }

        [Fact]
        public void Two_Conditional_Sums_Similar_Condition_One_More_Restrained()
        {
            var index = AddSimpleTestData();
            
            var result = Client.Search<Car>(sc => sc.Index(index).Aggregations(agg => agg
                .SumBy(x => x.Weight, x => x.ConditionalRanking.HasValue)
                .SumBy(x => x.Weight, x => x.ConditionalRanking.HasValue && x.CarType == "Type1")
            ));

            var sum = result.Aggs.GetSum<Car,decimal?>(x => x.Weight, c => c.ConditionalRanking.HasValue);
            var sum2 = result.Aggs.GetSum<Car, decimal?>(x => x.Weight, c => c.ConditionalRanking.HasValue && c.CarType == "Type1");

            Check.That(sum).Equals(25m);
            Check.That(sum2).Equals(0m);
            Client.DeleteIndex(index);
        }

        [Fact]
        public void Percentiles_Test()
        {
            var index = AddSimpleTestData();
            
            var result = Client.Search<Car>(sc => sc.Index(index).Aggregations(agg => agg
                .PercentilesBy(x=>x.Price)
            ));

            var percentiles = result.Aggs.GetPercentile<Car>(x => x.Price);
            var container = result.Aggs.AsContainer<Car>();

            Check.That(percentiles).HasSize(7);
            Check.That(container.GetPercentile(x => x.Price)).HasSize(7);

            Check.That(percentiles.Single(x => x.Percentile == 50.0).Value).Equals(10d);
            Client.DeleteIndex(index);
        }

        [Fact]
        public void MinMaxTest_DoubleField()
        {
            var index = AddSimpleTestData();

            var result = Client.Search<Car>(sc => sc.Index(index).Aggregations(agg => agg
                .MinBy(x => x.Length)
                .MaxBy(x=> x.Length))
            );

            var container = result.Aggs.AsContainer<Car>();
            var min = container.GetMin(x => x.Length);
            var max = container.GetMax(x => x.Length);

            Check.That(min).Equals(0d);
            Check.That(max).Equals(9d);
            Client.DeleteIndex(index);
        }

        [Fact]
        public void MinMaxTest_DateTimeField()
        {
            var index = AddSimpleTestData();

            var result = Client.Search<Car>(sc => sc.Index(index).Aggregations(agg => agg
                .MinBy(x => x.Timestamp)
                .MaxBy(x => x.Timestamp))
            );

            var container = result.Aggs.AsContainer<Car>();
            var min = container.GetMin(x => x.Timestamp);
            var max = container.GetMax(x => x.Timestamp);

            Check.That(min).Equals(new DateTime(2010, 1, 1));
            Check.That(max).Equals(new DateTime(2010, 10, 1));
            Client.DeleteIndex(index);
        }

        [Fact]
        public void MinMaxTest_NullableDateTimeField()
        {
            var index = AddSimpleTestData();

            var result = Client.Search<Car>(sc => sc.Index(index).Aggregations(agg => agg
                .MinBy(x => x.LastControlCheck)
                .MaxBy(x => x.LastControlCheck))
            );

            var container = result.Aggs.AsContainer<Car>();
            var min = container.GetMin(x => x.LastControlCheck);
            var max = container.GetMax(x => x.LastControlCheck);

            Check.That(min).Equals(new DateTime(2010, 1, 3));
            Check.That(max).Equals(new DateTime(2010, 1, 9));
            Client.DeleteIndex(index);
        }

        [Fact]
        public void Agg_On_NUllable_Field_With_No_Result()
        {
            //all price limit values are null - the result should be null
            var index = AddSimpleTestData();

            var result =
                Client.Search<Car>(
                    search => search.Index(index).Take(10).Aggregations(agg => agg
                            .MinBy(x => x.PriceLimit)
                            .MaxBy(x=>x.PriceLimit)
                            .PercentilesBy(x=> x.PriceLimit)));

            var container = result.Aggs.AsContainer<Car>();
            var min = container.GetMin(x => x.PriceLimit);
            Check.That(min).IsNull();

            var max = container.GetMax(x => x.PriceLimit);
            Check.That(max).IsNull();
        }

        [Fact]
        public void Min_Max_Stats_By_Test()
        {
            //all price limit values are null - the result should be null
            var index = AddSimpleTestData();

            var result =
                Client.Search<Car>(
                    search => search.Index(index)
                        .Take(10).Aggregations(agg => agg
                            .MinBy(x => x.Length)
                            .MaxBy(x => x.Length)
                            .StatsBy(x => x.Length)));

            var container = result.Aggs.AsContainer<Car>();
            var min = container.GetMin(x => x.Length);
            var max = container.GetMax(x => x.Length);
            var stats = container.GetStats(x => x.Length);
            Check.That(stats.Min).Equals(0d);
            Check.That(stats.Max).Equals(9d);
            Check.That(min).Equals(0d);
            Check.That(max).Equals(9d);
            Client.DeleteIndex(index);
        }
        
        [Fact]
        public void FirstByTests()
        {
            //very stupid test, getting the single value of engine type when engine type is diesel
            var index = AddSimpleTestData();

            var result = Client.Search<Car>(sc => sc.Index(index).Aggregations(agg => agg
                .SumBy(x => x.Weight, x => x.ConditionalRanking.HasValue)
                .FirstBy(x => x.EngineType, c => c.EngineType == EngineType.Diesel)
                .FirstBy(x => x.CarType, c => c.Sold == true)
                .FirstBy(x => x.Length)
            ));

            var sum = result.Aggs.GetSum<Car, decimal?>(x => x.Weight, c => c.ConditionalRanking.HasValue);
            var engineType = result.Aggs.GetFirstBy<Car,EngineType>(x => x.EngineType, c => c.EngineType == EngineType.Diesel);

            //car type of first sold car
            var carType = result.Aggs.GetFirstBy<Car, string>(x => x.CarType, c => c.Sold == true);

            var firstLength = result.Aggs.GetFirstBy<Car, double>(x => x.Length);

            Check.That(sum).Equals(25m);
            Check.That(engineType).Equals(EngineType.Diesel);
            Check.That(carType).Equals("type0");
            Check.That(firstLength).Equals(0d);

            var container = result.Aggs.AsContainer<Car>();
            var lengthFromContainer = container.GetFirstBy(x => x.Length);
            Check.That(lengthFromContainer).Equals(0d);
            Client.DeleteIndex(index);
        }

        [Fact]
        public void Non_Available_Conditional_Stat()
        {
            var result = Client.Search<Car>(search => search.Take(10).Aggregations(agg => agg));
            var exception = Record.Exception(() => result.Aggs.GetCount<Car>(x => x.CarType));
            Assert.NotNull(exception);
            Assert.IsType<InvalidOperationException>(exception);
            Check.That(exception.Message).Contains("No aggregations");
        }
    }
}
