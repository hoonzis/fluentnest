using System;
using System.Collections.Generic;
using System.Text;
using FluentNest.Tests.Model;
using Nest;
using NFluent;
using Tests.Model;
using Xunit;

namespace FluentNest.Tests
{
    public class FilterTests : TestsBase
    {
        public FilterTests()
        {
            Console.WriteLine("This tests is using the default serializer for enums, they should be serialized as integers");
        }

        private new void AddSimpleTestData()
        {
            Client.DeleteIndex(x => x.Index<Car>());
            Client.CreateIndex(c => c.Index<Car>().AddMapping<Car>(x => x
            .Properties(prop => prop.String(str => str.Name(s => s.EngineType).Index(FieldIndexOption.NotAnalyzed)))));
            Client.DeleteIndex(x => x.Index<User>());
            Client.CreateIndex(c => c.Index<User>().AddMapping<User>(x => x
            .Properties(prop => prop.String(str => str.Name(s => s.Email).Index(FieldIndexOption.NotAnalyzed)))));
            
            for (int i = 0; i < 10; i++)
            {
                var car = new Car
                {
                    Timestamp = new DateTime(2010,(i%12)+1,1),
                    Name = "Car" + i,
                    Price = 10,
                    Sold = i % 2 == 0,
                    CarType = "Type" + i%2,
                    Emissions = i+1,
                    IntField = i
                };
                Client.Index(car);
            }

            for (int i = 0; i < 10; i++)
            {
                var user = new User
                {
                    Email = "Email@email"+i%2+".com",
                    Name = "name"+i%3,
                    Age = i+1,
                    Enabled = i%2 == 0,
                    Active = i % 2 == 0,
                    CreationTime = DateTime.Now
                };
                Client.Index<User>(user);
            }
            Client.Flush(x => x.Index<User>());
            Client.Flush(x => x.Index<Car>());
        }


        [Fact]
        public void DateComparisonAndTerm()
        {
            AddSimpleTestData();

            var startDate = new DateTime(2010, 1, 1);
            var endDate = new DateTime(2010, 3, 1);
            var result = Client.Search<Car>(s => s.FilterOn(x => x.Timestamp >= startDate && x.Timestamp <= endDate && x.CarType == "type0"));


            Check.That(result.Documents).HasSize(2);
        }

        [Fact]
        public void DateEqualityTest()
        {
            AddSimpleTestData();

            var startDate = new DateTime(2010, 1, 1);
            var result = Client.Search<Car>(s => s.FilterOn(x => x.Timestamp == startDate));
            
            Check.That(result.Documents).HasSize(1);
        }

        [Fact]
        public void TestSimpleComparisonFilter()
        {
            AddSimpleTestData();
            var startDate = new DateTime(2010, 1, 1);
            var endDate = new DateTime(2010, 3, 1);
            var result = Client.Search<Car>(s => s
                .FilterOn(x => x.Timestamp > startDate && x.Timestamp < endDate));
            Check.That(result.Documents).HasSize(1);
        }

        [Fact]
        public void TestEqualityFilter()
        {
            AddSimpleTestData();

            var carType = "Type0".ToLower();
            //Standard Nest way of getting the docuements. Values are lowered by ES
            var result = Client.Search<Car>(s => s.Filter(x => x.Term(f => f.CarType, carType)));
            Check.That(result.Documents).HasSize(5);
            
            //Best way
            result = Client.Search<Car>(s => s.FilterOn(x => x.CarType == carType));
            Check.That(result.Documents).HasSize(5);

        }

        [Fact]
        public void TestRangeFilters_And_And()
        {
            AddSimpleTestData();

            var startDate = new DateTime(2010, 1, 1);
            var endDate = new DateTime(2010, 5, 1);
         
            var result = Client.Search<Car>(s => s.Query(
                    q=>q.Filtered(fil=>fil.Filter(
                        x => x.And(
                            left=>left.Range(f=>f.OnField(fd=>fd.Timestamp).Greater(startDate)),
                            right=>right.Range(f=>f.OnField(fd=>fd.Timestamp).Lower(endDate))
                        )
                    )
                )
            ));
            Check.That(result.Documents).HasSize(3);

            //Much better
            result = Client.Search<Car>(s => s.FilteredOn(f => f.Timestamp > startDate && f.Timestamp < endDate));
            Check.That(result.Documents).HasSize(3);
        }

        [Fact]
        public void FilterOnSpecialCharacter()
        {
            AddSimpleTestData();

            var allUsers = Client.Search<User>(x=>x.MatchAll());
            Check.That(allUsers.Documents).HasSize(10);

            //these two searches should provide the same result
            var result =
                Client.Search<User>(
                    s => s.Query(q => q.Filtered(f => f.Filter(fil => fil.Term(term => term.Email, "Email@email1.com")))));
            Check.That(result.Documents).HasSize(5);

            result = Client.Search<User>(s => s.FilteredOn(f => f.Email == "Email@email1.com"));
            Check.That(result.Documents).HasSize(5);
        }

        [Fact]
        public void TestConsecutiveFilters()
        {
            AddSimpleTestData();

            var filter = Filters
                .CreateFilter<User>(x => x.Name == "name1" && x.Age >= 5)
                .AndFilteredOn<User>(x => x.Email == "Email@email1.com");

            var users = Client.Search<User>(s => s.Filter(filter));
            Check.That(users.Documents).HasSize(1);            
        }

        [Fact]
        public void TestBooleanFilter()
        {
            AddSimpleTestData();

            var filter = Filters
                .CreateFilter<User>(x => x.Enabled == true);

            var allUsers = Client.Search<User>(s => s.Filter(filter));
            Check.That(allUsers.Documents).HasSize(5);
        }

        [Fact]
        public void MultipleFiltersAndSomeAggregations()
        {
            AddSimpleTestData();
            
            var filter = Filters
                .CreateFilter<User>(x => x.Name == "name1" && x.Age >= 5)
                .AndFilteredOn<User>(x => x.Email == "Email@email1.com");
            
            var result = Client.Search<User>(sc => sc
                .FilteredOn(filter)
                .Aggregations(agg => agg
                .SumBy(x=>x.Age)
            ));

            var sumValue = result.Aggs.GetSum<User, int>(x => x.Age);

            var aggsContainer = result.Aggs.AsContainer<User>();
            var sum2 = aggsContainer.GetSum(x => x.Age);
            Check.That(sumValue).Equals(8);
            Check.That(sum2).Equals(8);
        }

        [Fact]
        public void Or_Filter_Test()
        {
            AddSimpleTestData();
            var users = Client.Search<User>(s => s.FilterOn(x=> x.Name == "name1" || x.Age >= 5));
            Check.That(users.Documents).HasSize(7);
        }

        [Fact]
        public void Noq_equal_Filter_Test()
        {
            AddSimpleTestData();

            var filter = Filters
                .CreateFilter<User>(x => x.Name != "name1" && x.Name != "name2");

            var allUsers = Client.Search<User>(s => s.Filter(filter));
            Check.That(allUsers.Documents).HasSize(4);
        }

        [Fact]
        public void Bool_filter_test()
        {
            AddSimpleTestData();
            var allUsers = Client.Search<User>(s => s.FilterOn(f=>f.Active));
            Check.That(allUsers.Documents).HasSize(5);
        }

        [Fact]
        public void Time_equality_filter()
        {
            AddSimpleTestData();
            
            var allUsers = Client.Search<Car>(s=>s.FilterOn(x=>x.Timestamp == new DateTime(2010,1,1)));
            Check.That(allUsers.Documents).HasSize(1);
        }

        [Fact]
        public void Decimal_Two_Side_Range_Test()
        {
            AddSimpleTestData();
            var allUsers = Client.Search<Car>(s => s.FilterOn(x=>x.Emissions > 2 && x.Emissions < 6));
            Check.That(allUsers.Documents).HasSize(3);
        }

        [Fact]
        public void Integer_Two_Side_Range_Test()
        {
             var sc = new SearchDescriptor<Car>().FilterOn(x => x.Age > 2 && x.Age < 6);
            CheckSD(sc, "Integer_Two_Side_Range_Test");
        }

        [Fact]
        public void Three_Ands_Test()
        {
            var sc = new SearchDescriptor<Car>().FilterOn(x => x.Sold == true && x.Age < 6 && x.Emissions < 5);
            CheckSD(sc, "Three_Ands_Test");
        }

        [Fact]
        public void Filter_ValueWithin_Test()
        {
            var list = new List<string> {"name1", "name2"};
            var sc = new SearchDescriptor<Car>().FilteredOn(Filters.ValueWithin<User>(x => x.Name, list));
            CheckSD(sc, "Filter_ValueWithin_Test");
        }

        [Fact]
        public void Filter_ValueWithin_OnExistingFilter()
        {
            AddSimpleTestData();
            var filter = Filters.CreateFilter<User>(x => x.Age > 8);
            var sc = new SearchDescriptor<User>().FilteredOn(filter.AndValueWithin<User>(x => x.Name, new List<string> { "name1", "name2" }));
            var json = Encoding.UTF8.GetString(Client.Serializer.Serialize(sc));
            var allUsers = Client.Search<User>(sc);
            Check.That(json).Contains("\"name1\"");
            Check.That(allUsers.Documents).HasSize(1);
        }

        [Fact]
        public void Guid_Filter_Test()
        {
            var indexName = "cars3";
            Client.DeleteIndex(indexName);
            var createIndexResult = Client.CreateIndex(indexName, i => i.AddMapping<Car>(x => x
            .Properties(prop => prop.String(str => str.Name(s => s.Guid).Index(FieldIndexOption.NotAnalyzed)))));
            Check.That(createIndexResult.Acknowledged).IsTrue();
            for (int i = 0; i < 10; i++)
            {
                var car = new Car
                {
                    Timestamp = new DateTime(2010, (i % 12) + 1, 1),
                    Name = "Car" + i,
                    Price = 10,
                    Sold = i % 2 == 0,
                    CarType = "Type" + i % 2,
                    Emissions = i + 1
                };
                
                if (i == 1)
                {
                    car.Guid = "17c175f0-15ae-4f94-8d34-66574d7784d4";
                }

                Client.Index(car, ind => ind.Index(indexName));
            }
            Client.Flush(x => x.Index(indexName));

            var sc = new SearchDescriptor<Car>().Index(indexName).FilteredOn(x => x.Guid == "17c175f0-15ae-4f94-8d34-66574d7784d4");
            var result = Client.Search<Car>(sc);
            Check.That(result.Documents).HasSize(1);
        }
    }
}

