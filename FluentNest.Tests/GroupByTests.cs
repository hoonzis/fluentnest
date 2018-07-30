using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using FluentNest.Tests.Model;
using Nest;
using NFluent;
using Tests;
using Xunit;
using User = FluentNest.Tests.Model.User;

namespace FluentNest.Tests
{
    public class GroupByTests : TestsBase
    {
        public string AddSimpleTestData()
        {
            var indexName = "index_" + Guid.NewGuid();
            Client.CreateIndex(indexName, x => x.Mappings(
                m => m.Map<Car>(t => t
            .Properties(prop => prop.Keyword(str => str.Name(s => s.EngineType)))
            .Properties(prop => prop.Text(str => str.Name(s => s.CarType).Fielddata()))
            )));

            for (int i = 0; i < 10; i++)
            {
                var car = new Car
                {
                    Id = Guid.NewGuid(),
                    Timestamp = new DateTime(2010, i + 1, 1),
                    Name = "Car" + i,
                    Price = 10,
                    Sold = i % 2 == 0,
                    CarType = "Type" + i % 3,
                    Length = i,
                    EngineType = i % 2 == 0 ? EngineType.Diesel : EngineType.Standard,
                    Weight = 5,
                    ConditionalRanking = i % 2 == 0 ? null : (int?)i,
                    Description = "Desc" + i,
                };
                Client.Index(car, ind => ind.Index(indexName));
            }
            Client.Flush(indexName);
            return indexName;
        }

        [Fact]
        public void NestedGroupBy()
        {
            var index = AddSimpleTestData();

            // The standard NEST way without FluentNest
            var result = Client.Search<Car>(s => s.Index(index)
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

            var carTypesAgg = result.Aggs.Terms("firstLevel");

            foreach (var carType in carTypesAgg.Buckets)
            {
                var engineTypes = carType.Terms("secondLevel");
                foreach (var engineType in engineTypes.Buckets)
                {
                    var priceSum = (decimal)engineType.Sum("priceSum").Value;
                    Check.That(priceSum).IsPositive();
                }
            }

            // Now with FluentNest
            result =Client.Search<Car>(search => search.Index(index).Aggregations(agg => agg
                .SumBy(s => s.Price)
                .GroupBy(s => s.EngineType)
                .GroupBy(b => b.CarType)
            ));

            var aggsContainer = result.Aggs.AsContainer<Car>();
            var carTypes = aggsContainer.GetGroupBy(x => x.CarType).ToList();
            Check.That(carTypes).HasSize(3);
            foreach (var carType in carTypes)
            {
                var container = carType.AsContainer<Car>();
                var engineTypes = container.GetGroupBy(x => x.EngineType, k => new CarType
                {
                    Type = k.Key,
                    Price = k.GetSum<Car, decimal>(x => x.Price)
                }).ToList();

                Check.That(engineTypes).HasSize(2);
                Check.That(engineTypes.First().Price).Equals(20m);
            }
            Client.DeleteIndex(index);
        }

        [Fact]
        public void GetDictionaryFromGroupBy()
        {
            var index = AddSimpleTestData();

            var result = Client.Search<Car>(sc => sc.Index(index).Aggregations(agg => agg
                .SumBy(s => s.Price)
                .GroupBy(s => s.EngineType))
            );

            var aggsContainer = result.Aggs.AsContainer<Car>();

            var carTypesList = result.Aggs.GetGroupBy<Car>(x => x.EngineType);
            var carTypesDictionary = aggsContainer.GetDictionary(x => x.EngineType);

            Check.That(carTypesDictionary).HasSize(2);
            Check.That(carTypesList).HasSize(2);
            Check.That(carTypesDictionary.Keys).ContainsExactly(EngineType.Diesel, EngineType.Standard);
            Client.DeleteIndex(index);
        }

        [Fact]
        public void GetDictionaryWithDecimalKeysFromGroupBy()
        {
            var index = AddSimpleTestData();

            var result =
                Client.Search<Car>(search => search.Index(index).Aggregations(x => x.GroupBy(s => s.Price)));

            var aggsContainer = result.Aggs.AsContainer<Car>();
            var carTypes = aggsContainer.GetDictionary(x => x.Price);
            Check.That(carTypes).HasSize(1);
            Check.That(carTypes.Keys).ContainsExactly(10m);
            Client.DeleteIndex(index);
        }

        [Fact]
        public void GroupByStringKeys()
        {
            var index = AddSimpleTestData();

            var result = Client.Search<Car>(search => search.Index(index).Aggregations(agg => agg
                .SumBy(s => s.Price)
                .GroupBy("engineType")
            ));

            var engineTypes = result.Aggs.GetGroupBy("engineType");
            Check.That(engineTypes).HasSize(2);
            Client.DeleteIndex(index);
        }

        [Fact]
        public void DynamicGroupByListOfKeys()
        {
            var index = AddSimpleTestData();

            var result = Client.Search<Car>(search => search.Index(index).Aggregations(agg => agg
                .SumBy(s => s.Price)
                .GroupBy(new List<string> { "engineType", "carType" })
            ));

            var engineTypes = result.Aggs.GetGroupBy("engineType").ToList();
            Check.That(engineTypes).HasSize(2);

            foreach (var engineType in engineTypes)
            {
                var carTypes = engineType.GetGroupBy("carType");
                Check.That(carTypes).HasSize(3);
            }

            Client.DeleteIndex(index);
        }

        [Fact]
        public void Distinct_Test()
        {
            var index = AddSimpleTestData();

            var result = Client.Search<Car>(search => search.Index(index).Aggregations(agg => agg
                .DistinctBy(x => x.CarType)
                .DistinctBy(x => x.EngineType)
            ));

            var engineTypes = result.Aggs.GetDistinct<Car, EngineType>(x => x.EngineType).ToList();

            var container = result.Aggs.AsContainer<Car>();
            var distinctCarTypes = container.GetDistinct(x => x.CarType).ToList();

            Check.That(distinctCarTypes).IsNotNull();
            Check.That(distinctCarTypes).HasSize(3);
            Check.That(distinctCarTypes).ContainsExactly("type0", "type1", "type2");

            Check.That(engineTypes).IsNotNull();
            Check.That(engineTypes).HasSize(2);
            Check.That(engineTypes).ContainsExactly(EngineType.Diesel, EngineType.Standard);
            Client.DeleteIndex(index);
        }

        [Fact]
        public void Distinct_NamedField_Test()
        {
            var index = AddSimpleTestData();

            var result = Client.Search<Car>(search => search.Index(index).Aggregations(agg => agg
                .DistinctBy(x => x.GetFieldNamed<string>("carType"))
                .DistinctBy(x => x.EngineType)
            ));

            var engineTypes = result.Aggs.GetDistinct<Car, EngineType>(x => x.EngineType).ToList();

            var container = result.Aggs.AsContainer<Car>();
            var distinctCarTypes = container.GetDistinct(x => x.GetFieldNamed<string>("carType")).ToList();

            Check.That(distinctCarTypes).IsNotNull();
            Check.That(distinctCarTypes).HasSize(3);
            Check.That(distinctCarTypes).ContainsExactly("type0", "type1", "type2");

            Check.That(engineTypes).IsNotNull();
            Check.That(engineTypes).HasSize(2);
            Check.That(engineTypes).ContainsExactly(EngineType.Diesel, EngineType.Standard);
            Client.DeleteIndex(index);
        }

        [Fact]
        public void Simple_Filtered_Distinct_Test()
        {
            var index = AddSimpleTestData();

            var result = Client.Search<Car>(search => search.Index(index)
                .FilterOn(f=> f.CarType == "type0")
                .Aggregations(agg => agg
                    .DistinctBy(x => x.CarType)
                    .DistinctBy(x => x.EngineType)
                )
            );

            var distinctCarTypes = result.Aggs.GetDistinct<Car, string>(x => x.CarType).ToList();
            var engineTypes = result.Aggs.GetDistinct<Car, EngineType>(x => x.EngineType).ToList();

            Check.That(distinctCarTypes).IsNotNull();
            Check.That(distinctCarTypes).HasSize(1);
            Check.That(distinctCarTypes).ContainsExactly("type0");

            Check.That(engineTypes).IsNotNull();
            Check.That(engineTypes).HasSize(2);
            Check.That(engineTypes).ContainsExactly(EngineType.Diesel, EngineType.Standard);
            Client.DeleteIndex(index);
        }

        [Fact]
        public void Distinct_Time_And_Term_Filter_Test()
        {
            var index = AddSimpleTestData();

            var filter = Filters.CreateFilter<Car>(x => x.Timestamp > new DateTime(2010,2,1) && x.Timestamp < new DateTime(2010, 8, 1))
                .AndFilteredOn<Car>(x => x.CarType == "type0");

            var result = Client.Search<Car>(sc => sc.Index(index).FilterOn(filter).Aggregations(agg => agg
                .DistinctBy(x => x.CarType)
                .DistinctBy(x => x.EngineType)
            ));

            var distinctCarTypes = result.Aggs.GetDistinct<Car, string>(x => x.CarType).ToList();
            var engineTypes = result.Aggs.GetDistinct<Car, EngineType>(x => x.EngineType).ToList();

            Check.That(distinctCarTypes).IsNotNull();
            Check.That(distinctCarTypes).HasSize(1);
            Check.That(distinctCarTypes).ContainsExactly("type0");

            Check.That(engineTypes).IsNotNull();
            Check.That(engineTypes).HasSize(2);
            Check.That(engineTypes).ContainsExactly(EngineType.Diesel, EngineType.Standard);
            Client.DeleteIndex(index);
        }

        [Fact]
        public void Terms_Aggregation_Big_Size()
        {
            var index = CreateUsersIndex(200, 20);
            var result = Client.Search<User>(sc => sc.Index(index).Aggregations(agg => agg.DistinctBy(x=>x.Nationality)));
            var nationalities = result.Aggs.GetDistinct<User, string>(x => x.Nationality).ToList();

            Check.That(nationalities).IsNotNull();
            Check.That(nationalities).HasSize(20);
        }

        [Fact]
        public void GroupBy_With_TopHits_Specifying_Properties_To_Fetch()
        {
            var index = AddSimpleTestData();

            var result = Client.Search<Car>(search => search.Index(index).Aggregations(agg => agg
                //get name and weight for each retrieved document
                .TopHits(3, x => x.Name, x => x.Weight)
                .GroupBy(b => b.CarType)
            ));


            var carTypes = result.Aggs.GetGroupBy<Car>(x => x.CarType).ToList();
            Check.That(carTypes).HasSize(3);
            foreach (var carType in carTypes)
            {
                var hits = carType.GetTopHits<Car>().ToList();
                Check.That(hits).HasSize(3);
                // we have asked only for name and weights
                Check.That(hits[0].Name).IsNotNull();
                Check.That(hits[0].Weight).IsNotNull();
                // description must be null
                Check.That(hits[0].Description).IsNull();
            }
            Client.DeleteIndex(index);
        }

        [Fact]
        public void GroupBy_With_TopHits_Specifying_NamedProperties_To_Fetch()
        {
            var index = AddSimpleTestData();


            var searchDescriptor = new SearchDescriptor<Car>().Index(index).Aggregations(agg => agg
                //get name and weight for each retrieved document
                .TopHits(3, x => x.GetFieldNamed<string>("name"), x => x.Weight)
                .GroupBy(b => b.CarType)
            );
            var c = Serialize(searchDescriptor);

            var result = Client.Search<Car>(searchDescriptor);


            var carTypes = result.Aggs.GetGroupBy<Car>(x => x.CarType).ToList();
            Check.That(carTypes).HasSize(3);
            foreach (var carType in carTypes)
            {
                var hits = carType.GetTopHits<Car>().ToList();
                Check.That(hits).HasSize(3);
                // we have asked only for name and weights
                Check.That(hits[0].Name).IsNotNull();
                Check.That(hits[0].Weight).IsNotNull();
                // description must be null
                Check.That(hits[0].Description).IsNull();
            }
            Client.DeleteIndex(index);
        }

        [Fact]
        public void GroupBy_With_TopHits_NoProperties_GetsWholeSource()
        {
            var index = AddSimpleTestData();

            var result = Client.Search<Car>(search => search.Index(index).Aggregations(x => x
                .TopHits(3)
                .GroupBy(b => b.CarType))
            );

            var carTypes = result.Aggs.GetGroupBy<Car>(x => x.CarType).ToList();
            Check.That(carTypes).HasSize(3);
            foreach (var carType in carTypes)
            {
                var hits = carType.GetTopHits<Car>().ToList();
                Check.That(hits).HasSize(3);
                Check.That(hits[0].Name).IsNotNull();
                Check.That(hits[0].Weight).IsNotNull();
                Check.That(hits[0].Description).IsNotNull();
            }
            Client.DeleteIndex(index);
        }

        [Fact]
        public void TopHits_In_Double_GroupBy()
        {
            var indexName = CreateUsersIndex(250, 2);

            var result = Client.Search<User>(search => search.Index(indexName).Aggregations(agg => agg
                .TopHits(40, x => x.Name)
                .GroupBy(b => b.Active)
                .GroupBy(b => b.Nationality))
            );

            var userByNationality = result.Aggs.GetGroupBy<User>(x => x.Nationality).ToList();
            Check.That(userByNationality).HasSize(2);
            foreach (var nationality in userByNationality)
            {
                var byActive = nationality.GetGroupBy<User>(x => x.Active).ToList();

                var activeUsers = byActive[0];
                var inactiveUser = byActive[1];


                var hits = activeUsers.GetTopHits<User>().ToList();
                Check.That(hits).HasSize(40);
                Check.That(hits[0].Name).IsNotNull();

                hits = inactiveUser.GetTopHits<User>().ToList();

                Check.That(hits).HasSize(40);
                Check.That(hits[0].Name).IsNotNull();
            }
        }

        private string CreateUsersIndex(int size, int nationalitiesCount)
        {
            var indexName = "index_" + Guid.NewGuid();
            Client.CreateIndex(indexName, x => x.Mappings(
                 m => m.Map<User>(t => t
             .Properties(prop => prop.Text(str => str.Name(s => s.Nationality).Fielddata()))
             .Properties(prop => prop.Text(str => str.Name(s => s.Name).Fielddata()))
             .Properties(prop => prop.Text(str => str.Name(s => s.Email).Fielddata()))
             )));
            var users = new List<User>();
            for (int i = 0; i < size; i++)
            {
                var user = new User
                {
                    Id = Guid.NewGuid(),
                    Name = "User" + i,
                    Nationality = "Nationality" + i % nationalitiesCount,
                    Active = i%3 == 0,
                    Age =  (i + 1) % 10
                };

                users.Add(user);
            }
            Client.Bulk(x => x.CreateMany(users).Index(indexName));
            Client.Flush(indexName);
            return indexName;
        }

        [Fact]
        public void TopHits_Sorted_SettingSize()
        {
            var index = CreateUsersIndex(100, 10);
            var result = Client.Search<User>(search => search.Index(index).Aggregations(agg => agg
                // get 40 first users, sort by name. for each user retrieve name and email
                .SortedTopHits(40, x=>x.Name, SortType.Ascending, x => x.Name, y=>y.Email)
                .SortedTopHits(40, x=>x.Name, SortType.Descending, x=>x.Name, y=>y.Email)
                .SumBy(x=>x.Age)
                .GroupBy(b => b.Nationality))
            );

            var userByNationality = result.Aggs.GetGroupBy<User>(x => x.Nationality).ToList();
            Check.That(userByNationality).HasSize(10);
            var firstNotionality = userByNationality.Single(x => x.Key == "nationality0");

            var ascendingHits = firstNotionality.GetSortedTopHits<User>(x => x.Name, SortType.Ascending).ToList();
            Check.That(ascendingHits).HasSize(10);
            Check.That(ascendingHits[0].Name).IsNotNull();

            Check.That(firstNotionality.GetSum<User, int>(x => x.Age)).Equals(10);
            Check.That(ascendingHits[0].Name).Equals("User0");
            Check.That(ascendingHits[1].Name).Equals("User10");
            Check.That(ascendingHits[2].Name).Equals("User20");
            Check.That(ascendingHits[3].Name).Equals("User30");

            var descendingHits = firstNotionality.GetSortedTopHits<User>(x => x.Name, SortType.Descending).ToList();
            Check.That(descendingHits).HasSize(10);
            Check.That(descendingHits[0].Name).IsNotNull();

            Check.That(descendingHits[0].Name).Equals("User90");
            Check.That(descendingHits[1].Name).Equals("User80");
            Check.That(descendingHits[2].Name).Equals("User70");
            Check.That(descendingHits[3].Name).Equals("User60");
        }

        [Fact]
        public void TopHits_Sorted_NamedField()
        {
            var index = CreateUsersIndex(100, 10);
            var result = Client.Search<User>(search => search.Index(index).Aggregations(agg => agg
                // get 40 first users, sort by name. for each user retrieve name and email
                .SortedTopHits(40, x => x.GetFieldNamed<string>("name"), SortType.Ascending, x => x.Name, y => y.Email)
                .SortedTopHits(40, x => x.GetFieldNamed<string>("name"), SortType.Descending, x => x.Name, y => y.Email)
                .SumBy(x => x.Age)
                .GroupBy(b => b.Nationality))
            );

            var userByNationality = result.Aggs.GetGroupBy<User>(x => x.Nationality).ToList();
            Check.That(userByNationality).HasSize(10);
            var firstNotionality = userByNationality.Single(x => x.Key == "nationality0");

            var ascendingHits = firstNotionality.GetSortedTopHits<User>(x => x.GetFieldNamed<string>("name"), SortType.Ascending).ToList();
            Check.That(ascendingHits).HasSize(10);
            Check.That(ascendingHits[0].Name).IsNotNull();

            Check.That(firstNotionality.GetSum<User, int>(x => x.Age)).Equals(10);
            Check.That(ascendingHits[0].Name).Equals("User0");
            Check.That(ascendingHits[1].Name).Equals("User10");
            Check.That(ascendingHits[2].Name).Equals("User20");
            Check.That(ascendingHits[3].Name).Equals("User30");

            var descendingHits = firstNotionality.GetSortedTopHits<User>(x => x.GetFieldNamed<string>("name"), SortType.Descending).ToList();
            Check.That(descendingHits).HasSize(10);
            Check.That(descendingHits[0].Name).IsNotNull();

            Check.That(descendingHits[0].Name).Equals("User90");
            Check.That(descendingHits[1].Name).Equals("User80");
            Check.That(descendingHits[2].Name).Equals("User70");
            Check.That(descendingHits[3].Name).Equals("User60");
        }

        [Fact]
        public void GettingNonExistingGroup_Test()
        {
            var index = AddSimpleTestData();

            var result = Client.Search<Car>(search => search.Index(index).Aggregations(agg => agg
                .GroupBy(b => b.Emissions)
            ));

            var exception = Record.Exception(() => result.Aggs.GetGroupBy<Car>(x => x.CarType));

            Assert.NotNull(exception);
            Assert.IsType<InvalidOperationException>(exception);
            Check.That(exception.Message).Contains("Available aggregations: GroupByEmissions");
        }

        [Fact]
        public void NoAggregations_On_The_Result_Test()
        {
            // no data - no aggregations on the result
            var result = Client.Search<Car>(search => search.Aggregations(agg => agg
                .GroupBy(b => b.Emissions)
            ));

            var exception = Record.Exception(() => result.Aggs.GetGroupBy<Car>(x => x.CarType));

            Assert.NotNull(exception);
            Assert.IsType<InvalidOperationException>(exception);
            Check.That(exception.Message).Contains("No aggregations");
        }

    }
}
