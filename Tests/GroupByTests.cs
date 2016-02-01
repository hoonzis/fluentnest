using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentNest;
using Nest;
using NFluent;
using TestModel;
using Xunit;

namespace Tests
{   
    public class GroupByTests : TestsBase
    {
        [Fact]
        public void NestedGroupBy()
        {
            AddSimpleTestData();
            var agg = new AggregationContainerDescriptor<Car>()
                .SumBy(s => s.Price)
                .GroupBy(s => s.EngineType)
                .GroupBy(b => b.CarType);

            var result = client.Search<Car>(search => search.Aggregations(x => agg));


            var carTypes = result.Aggs.GetGroupBy<Car>(x => x.CarType).ToList();
            Check.That(carTypes).HasSize(3);
            foreach (var carType in carTypes)
            {
                var engineTypes = carType.GetGroupBy<Car,CarType>(x => x.EngineType, k => new CarType
                {
                    Type = k.Key,
                    Price = k.GetSum<Car,Decimal>(x=>x.Price)
                });
                Check.That(engineTypes).HasSize(2);
                Check.That(engineTypes.First().Price).Equals(20m);
            }
        }

        [Fact]
        public void GetDictionaryFromGroupBy()
        {
            AddSimpleTestData();
            var sumOnPrice = new AggregationContainerDescriptor<Car>().SumBy(s => s.Price);

            var result =
                client.Search<Car>(search => search.Aggregations(x => sumOnPrice.GroupBy(s => s.EngineType)));

            var carTypes = result.Aggs.GetDictioanry<Car,EngineType>(x => x.EngineType);
            Check.That(carTypes).HasSize(2);
            Check.That(carTypes.Keys).ContainsExactly(EngineType.Diesel, EngineType.Standard);
        }

        [Fact]
        public void GroupByStringKeys()
        {
            AddSimpleTestData();
            var agg = new AggregationContainerDescriptor<Car>()
                .SumBy(s => s.Price)
                .GroupBy("engineType");

            var result = client.Search<Car>(search => search.Aggregations(x => agg));

            var engineTypes = result.Aggs.GetGroupBy("engineType");
            Check.That(engineTypes).HasSize(2);
        }

        [Fact]
        public void DynamicGroupByListOfKeys()
        {
            AddSimpleTestData();
            var agg = new AggregationContainerDescriptor<Car>()
                .SumBy(s => s.Price)
                .GroupBy(new List<string> {"engineType", "carType"});

            var result = client.Search<Car>(search => search.Aggregations(x => agg));

            var engineTypes = result.Aggs.GetGroupBy("engineType");
            Check.That(engineTypes).HasSize(2);

            foreach (var engineType in engineTypes)
            {
                var carTypes = engineType.GetGroupBy("carType");
                Check.That(carTypes).HasSize(3);
            }
        }

        //Sum of car grouped by engines and carTypes. Just to be compared with the better syntax
        [Fact]
        public void StandardTwoLevelGroupByWithSum()
        {
            AddSimpleTestData();
            var result = client.Search<Car>(s => s
                .Aggregations(fstAgg => fstAgg
                    .Terms("firstLevel", f => f
                        .Field(z => z.CarType)
                        .Aggregations(sndLevel => sndLevel
                            .Terms("secondLevel", f2 => f2.Field(f3 => f3.EngineType)
                                .Aggregations(sums => sums
                                    .Sum("priceSum", son => son
                                    .Field(f4 => f4.Price))
                                )
                            )
                        )
                    )
                )
            );

            var carTypes = result.Aggs.Terms("firstLevel");

            foreach (var carType in carTypes.Items)
            {
                var engineTypes = carType.Terms("secondLevel");
                foreach (var engineType in engineTypes.Items)
                {
                    var priceSum = (decimal)engineType.Sum("priceSum").Value;
                    Check.That(priceSum).IsPositive();
                }               
            }
        }

        [Fact]
        public void Distinct_Test()
        {
            AddSimpleTestData();
            var agg = new AggregationContainerDescriptor<Car>()
                .DistinctBy(x => x.CarType)
                .DistinctBy(x => x.EngineType);

            var result = client.Search<Car>(search => search.Aggregations(x => agg));
            
            var engineTypes = result.Aggs.GetDistinct<Car, EngineType>(x => x.EngineType).ToList();

            var container = result.Aggs.AsContainer<Car>();
            var distinctCarTypes = container.GetDistinct(x => x.CarType).ToList();

            Check.That(distinctCarTypes).IsNotNull();
            Check.That(distinctCarTypes).HasSize(3);
            Check.That(distinctCarTypes).ContainsExactly("type0", "type1", "type2");

            Check.That(engineTypes).IsNotNull();
            Check.That(engineTypes).HasSize(2);
            Check.That(engineTypes).ContainsExactly(EngineType.Diesel, EngineType.Standard);
        }

        [Fact]
        public void Simple_Filtered_Distinct_Test()
        {
            AddSimpleTestData();
            var agg = new AggregationContainerDescriptor<Car>()
                .DistinctBy(x => x.CarType)
                .DistinctBy(x => x.EngineType);

            var filter = NestHelperMethods.CreateFilter<Car>(x => x.CarType == "type0");
            var result = client.Search<Car>(search => search.FilteredOn(filter).Aggregations(x => agg));

            var distinctCarTypes = result.Aggs.GetDistinct<Car, String>(x => x.CarType).ToList();
            var engineTypes = result.Aggs.GetDistinct<Car, EngineType>(x => x.EngineType).ToList();

            Check.That(distinctCarTypes).IsNotNull();
            Check.That(distinctCarTypes).HasSize(1);
            Check.That(distinctCarTypes).ContainsExactly("type0");

            Check.That(engineTypes).IsNotNull();
            Check.That(engineTypes).HasSize(2);
            Check.That(engineTypes).ContainsExactly(EngineType.Diesel, EngineType.Standard);
        }

        [Fact]
        public void Distinct_Time_And_Term_Filter_Test()
        {
            AddSimpleTestData();
            var agg = new AggregationContainerDescriptor<Car>()
                .DistinctBy(x => x.CarType)
                .DistinctBy(x => x.EngineType);

            var filter = NestHelperMethods.CreateFilter<Car>(x => x.Timestamp > new DateTime(2010,2,1) && x.Timestamp < new DateTime(2010, 8, 1));
            filter = filter.AndFilteredOn<Car>(x => x.CarType == "type0");

            var sc = new SearchDescriptor<Car>()
                .FilteredOn(filter).Aggregations(x => agg);

            var result = client.Search<Car>(sc);

            var distinctCarTypes = result.Aggs.GetDistinct<Car, String>(x => x.CarType).ToList();
            var engineTypes = result.Aggs.GetDistinct<Car, EngineType>(x => x.EngineType).ToList();

            Check.That(distinctCarTypes).IsNotNull();
            Check.That(distinctCarTypes).HasSize(1);
            Check.That(distinctCarTypes).ContainsExactly("type0");

            Check.That(engineTypes).IsNotNull();
            Check.That(engineTypes).HasSize(2);
            Check.That(engineTypes).ContainsExactly(EngineType.Diesel, EngineType.Standard);
        }
    }
}
