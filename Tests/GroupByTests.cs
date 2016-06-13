using System;
using System.Collections.Generic;
using System.Linq;
using FluentNest;
using NFluent;
using TestModel;
using Xunit;

namespace Tests
{   
    public class GroupByTests :TestsBase
    {
        [Fact]
        public void NestedGroupBy()
        {
            AddSimpleTestData();
            
            var result =client.Search<Car>(search =>search.Aggregations(agg => agg
                .SumBy(s => s.Price)
                .GroupBy(s => s.EngineType)
                .GroupBy(b => b.CarType)
            ));


            var carTypes = result.Aggs.GetGroupBy<Car>(x => x.CarType).ToList();
            Check.That(carTypes).HasSize(3);
            foreach (var carType in carTypes)
            {
                var engineTypes = carType.GetGroupBy<Car, CarType>(x => x.EngineType, k => new CarType
                {
                    Type = k.Key,
                    Price = k.GetSum<Car, decimal>(x => x.Price)
                }).ToList();

                Check.That(engineTypes).HasSize(2);
                Check.That(engineTypes.First().Price).Equals(20m);
            }
        }

        [Fact]
        public void GetDictionaryFromGroupBy()
        {
            AddSimpleTestData();

            var result = client.Search<Car>(sc => sc.Aggregations(agg => agg
                .SumBy(s => s.Price)
                .GroupBy(s => s.EngineType))
            );

            var carTypesList = result.Aggs.GetGroupBy<Car>(x => x.EngineType);
            var carTypesDictionary = result.Aggs.GetDictionary<Car,EngineType>(x => x.EngineType);
            
            Check.That(carTypesDictionary).HasSize(2);
            Check.That(carTypesList).HasSize(2);
            Check.That(carTypesDictionary.Keys).ContainsExactly(EngineType.Diesel, EngineType.Standard);
        }

        [Fact]
        public void GetDictionaryWithDecimalKeysFromGroupBy()
        {
            AddSimpleTestData();

            var result =
                client.Search<Car>(search => search.Aggregations(x => x.GroupBy(s => s.Price)));

            var carTypes = result.Aggs.GetDictionary<Car, decimal>(x => x.Price);
            Check.That(carTypes).HasSize(1);
            Check.That(carTypes.Keys).ContainsExactly(10m);
        }

        [Fact]
        public void GroupByStringKeys()
        {
            AddSimpleTestData();
            
            var result = client.Search<Car>(search => search.Aggregations(agg => agg
                .SumBy(s => s.Price)
                .GroupBy("engineType")
            ));

            var engineTypes = result.Aggs.GetGroupBy("engineType");
            Check.That(engineTypes).HasSize(2);
        }

        [Fact]
        public void DynamicGroupByListOfKeys()
        {
            AddSimpleTestData();
            
            var result = client.Search<Car>(search => search.Aggregations(agg => agg
                .SumBy(s => s.Price)
                .GroupBy(new List<string> { "engineType", "carType" })
            ));

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
            
            var result = client.Search<Car>(search => search.Aggregations(agg => agg
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
        }

        [Fact]
        public void Simple_Filtered_Distinct_Test()
        {
            AddSimpleTestData();
            
            var result = client.Search<Car>(search => search
                .FilteredOn(f=> f.CarType == "type0")
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
        }

        [Fact]
        public void Distinct_Time_And_Term_Filter_Test()
        {
            AddSimpleTestData();
            
            var filter = FluentNest.Filters.CreateFilter<Car>(x => x.Timestamp > new DateTime(2010,2,1) && x.Timestamp < new DateTime(2010, 8, 1))
                .AndFilteredOn<Car>(x => x.CarType == "type0");

            var result = client.Search<Car>(sc => sc.FilteredOn(filter).Aggregations(agg => agg
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
        }

        [Fact]
        public void Terms_Aggregation_Big_Size()
        {
            client.DeleteIndex(x => x.Index<User>());
            client.CreateIndex(c => c.Index<User>());
            for (int i = 0; i < 1000; i++)
            {
                var user = new User
                {
                    Name = "User" + i,
                    Nationality = "Nationality" + i%50
                };
                
                client.Index(user);
            }
            client.Flush(x=>x.Index<User>());
    
            var result = client.Search<User>(sc => sc.Aggregations(agg => agg.DistinctBy(x=>x.Nationality)));

            var nationalities = result.Aggs.GetDistinct<User, string>(x => x.Nationality).ToList();

            Check.That(nationalities).IsNotNull();
            Check.That(nationalities).HasSize(50);
        }

        [Fact]
        public void GroupBy_With_TopHits_Specifying_Properties()
        {
            AddSimpleTestData();
            
            var result = client.Search<Car>(search => search.Aggregations(agg => agg
                .TopHits(3, x => x.Name)
                .GroupBy(b => b.CarType)
            ));


            var carTypes = result.Aggs.GetGroupBy<Car>(x => x.CarType).ToList();
            Check.That(carTypes).HasSize(3);
            foreach (var carType in carTypes)
            {
                var hits = carType.GetTopHits<Car>().ToList();
                Check.That(hits).HasSize(3);
                Check.That(hits[0].Name).IsNotNull();
                Check.That(hits[0].Weight).IsNull();
            }
        }

        [Fact]
        public void GroupBy_With_TopHits_Specifying_More_Properties()
        {
            AddSimpleTestData();

            var result = client.Search<Car>(search => search.Aggregations(agg => agg
                //get name and weight for each retrived document
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
        }

        [Fact]
        public void GroupBy_With_TopHits_NoProperties_GetsWholeSource()
        {
            AddSimpleTestData();
            
            var result = client.Search<Car>(search => search.Aggregations(x => x
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
        }
        
        [Fact]
        public void TopHits_In_Double_GroupBy()
        {
            client.DeleteIndex(x => x.Index<User>());
            client.CreateIndex(c => c.Index<User>());
            for (int i = 0; i < 1000; i++)
            {
                var user = new User
                {
                    Name = "User" + i,
                    Nationality = "Nationality" + i % 2,
                    Active = i%3 == 0
                };

                client.Index(user);
            }
            client.Flush(x => x.Index<User>());

            var result = client.Search<User>(search => search.Aggregations(agg => agg
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

        [Fact]
        public void TopHits_Sorted_SettingSize()
        {
            client.DeleteIndex(x => x.Index<User>());
            client.CreateIndex(c => c.Index<User>());
            for (int i = 0; i < 100; i++)
            {
                var user = new User
                {
                    Name = "User" + i,
                    Nationality = "Nationality" + i % 10,
                    Age = (i+1) % 10
                };

                client.Index(user);
            }
            client.Flush(x => x.Index<User>());

            var result = client.Search<User>(search => search.Aggregations(agg => agg
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
    }
}
