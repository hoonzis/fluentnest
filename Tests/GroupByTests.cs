using System;
using System.Collections.Generic;
using System.Linq;
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
            var agg = Statistics
                .SumBy<Car>(s => s.Price)
                .GroupBy(s => s.EngineType)
                .GroupBy(b => b.CarType);

            var result =client.Search<Car>(search =>search.Aggregations(x => agg));


            var carTypes = result.Aggs.GetGroupBy<Car>(x => x.CarType).ToList();
            Check.That(carTypes).HasSize(3);
            foreach (var carType in carTypes)
            {
                var engineTypes = carType.GetGroupBy<Car,CarType>(x => x.EngineType, k => new CarType
                {
                    Type = k.Key,
                    Price = k.GetSum<Car,Decimal>(x=>x.Price) ?? 0m
                });
                Check.That(engineTypes).HasSize(2);
                Check.That(engineTypes.First().Price).Equals(20m);
            }
        }

        [Fact]
        public void GetDictionaryFromGroupBy()
        {
            AddSimpleTestData();
            var sumOnPrice = Statistics.SumBy<Car>(s => s.Price);

            var result =
                client.Search<Car>(search => search.Aggregations(x => sumOnPrice.GroupBy(s => s.EngineType)));


            var carTypes = result.Aggs.GetDictioanry<Car>(x => x.EngineType);
            Check.That(carTypes).HasSize(2);
            Check.That(carTypes.Keys).ContainsExactly("engine0", "engine1");
        }

        [Fact]
        public void GroupByStringKeys()
        {
            AddSimpleTestData();
            var agg = Statistics
                .SumBy<Car>(s => s.Price)
                .GroupBy("engineType");

            var result = client.Search<Car>(search => search.Aggregations(x => agg));

            var engineTypes = result.Aggs.GetGroupBy("engineType");
            Check.That(engineTypes).HasSize(2);
        }

        [Fact]
        public void DynammicGroupByListOfKeys()
        {
            AddSimpleTestData();
            var agg = Statistics
                .SumBy<Car>(s => s.Price)
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
    }
}
