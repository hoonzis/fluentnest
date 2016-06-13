using System;
using System.Collections.Generic;
using FluentNest;
using Nest;
using NFluent;
using TestModel;
using Xunit;

namespace Tests
{
    public class FilterTests : TestsBase
    {
        private readonly IndexName userIndex = Infer.Index<User>();
        private const string MyFavoriteGuid = "17c175f0-15ae-4f94-8d34-66574d7784d4";

        private void AddSimpleTestData()
        {
            client.DeleteIndex(CarIndex);
            client.CreateIndex(CarIndex, x => x.Mappings(
                 m => m.Map<Car>(t => t.Properties(prop => prop.String(str => str.Name(s => s.Guid).Index(FieldIndexOption.NotAnalyzed))))));
            
            client.DeleteIndex(userIndex);
            var createIndexResult = client.CreateIndex(userIndex, x=>x.Mappings(
                m => m.Map<User>(t => t.Properties(prop => prop.String(str => str.Name(s => s.Email).Index(FieldIndexOption.NotAnalyzed))))));

            Check.That(createIndexResult.Acknowledged).IsTrue();
            for (int i = 0; i < 10; i++)
            {
                var car = new Car
                {
                    Timestamp = new DateTime(2010,(i%12)+1,1),
                    Name = "Car" + i,
                    Price = 10,
                    Sold = i % 2 == 0 ? true : false,
                    CarType = "Type" + i%2,
                    Emissions = i+1,
                    Guid = Guid.NewGuid().ToString()
                };
                if (i == 1)
                {
                    car.Guid = MyFavoriteGuid;
                }
                client.Index(car, ind => ind.Index(CarIndex));
            }

            for (int i = 0; i < 10; i++)
            {
                var user = new User
                {
                    Email = "Email@email"+i%2+".com",
                    Name = "name"+i%3,
                    Age = i+1,
                    Enabled = i%2 == 0 ? true : false,
                    Active = i % 2 == 0 ? true : false
                };
                client.Index(user, c => c.Index(userIndex));
            }
            client.Flush(Indices.AllIndices);
        }


        [Fact]
        public void DateComparisonAndTerm()
        {
            AddSimpleTestData();

            var startDate = new DateTime(2010, 1, 1);
            var endDate = new DateTime(2010, 3, 1);
            var result = client.Search<Car>(s => s.FilterOn(x => x.Timestamp >= startDate && x.Timestamp <= endDate && x.CarType == "type0"));


            Check.That(result.Documents).HasSize(2);
        }

        [Fact]
        public void DateEqualityTest()
        {
            AddSimpleTestData();

            var startDate = new DateTime(2010, 1, 1);
            var result = client.Search<Car>(s => s.FilterOn(x => x.Timestamp == startDate));
            
            Check.That(result.Documents).HasSize(1);
        }

        [Fact]
        public void TestSimpleComparisonFilter()
        {
            AddSimpleTestData();
            var startDate = new DateTime(2010, 1, 1);
            var endDate = new DateTime(2010, 3, 1);
            var result = client.Search<Car>(s => s
                .FilterOn(x => x.Timestamp > startDate && x.Timestamp < endDate));
            Check.That(result.Documents).HasSize(1);
        }

        [Fact]
        public void TestEqualityFilter()
        {
            AddSimpleTestData();

            var carType = "Type0".ToLower();
            //Standard Nest way of getting the docuements. Values are lowered by ES
            var result = client.Search<Car>(s => s.Query(x => x.Term(f => f.CarType, carType)));
            Check.That(result.Documents).HasSize(5);
            
            //Best way
            result = client.Search<Car>(s => s.FilterOn(x => x.CarType == carType));
            Check.That(result.Documents).HasSize(5);

        }

        [Fact]
        public void TestRangeFilters_And_And()
        {
            AddSimpleTestData();

            var startDate = new DateTime(2010, 1, 1);
            var endDate = new DateTime(2010, 5, 1);

            var result = client.Search<Car>(s => s.Query(
                q => q.Bool(b => b.Must(left => left.DateRange(f => f.Field(fd => fd.Timestamp).GreaterThan(startDate)), 
                                        right => right.DateRange(f => f.Field(fd => fd.Timestamp).LessThan(endDate)))
                    )
                ));
            Check.That(result.Documents).HasSize(3);

            //Much better
            result = client.Search<Car>(s => s.FilterOn(f => f.Timestamp > startDate && f.Timestamp < endDate));
            Check.That(result.Documents).HasSize(3);
        }

        [Fact]
        public void FilterOnSpecialCharacter()
        {
            AddSimpleTestData();

            var allUsers = client.Search<User>(s => s.Index(userIndex));
            Check.That(allUsers.Documents).HasSize(10);

            //these two searches should provide the same result
            var result =
                client.Search<User>(
                    s =>
                        s.Index(userIndex)
                            .Query(
                                q =>
                                    q.Bool(
                                        b => b.Must(x => x.Term(term => term.Email, "Email@email1.com")))));
            Check.That(result.Documents).HasSize(5);

            result = client.Search<User>(s => s.Index(userIndex).FilterOn(f => f.Email == "Email@email1.com"));
            Check.That(result.Documents).HasSize(5);
        }

        [Fact]
        public void TestConsecutiveFilters()
        {
            AddSimpleTestData();

            var filter = Filters
                .CreateFilter<User>(x => x.Name == "name1" && x.Age >= 5)
                .AndFilteredOn<User>(x => x.Email == "Email@email1.com");

            var users = client.Search<User>(s => s.Query(_ => filter));
            Check.That(users.Documents).HasSize(1);            
        }

        [Fact]
        public void TestBooleanFilter()
        {
            AddSimpleTestData();

            var filter = Filters
                .CreateFilter<User>(x => x.Enabled == true);

            var allUsers = client.Search<User>(s => s.Index(userIndex).Query(_ => filter));
            Check.That(allUsers.Documents).HasSize(5);
        }

        [Fact]
        public void MultipleFiltersAndSomeAggregations()
        {
            AddSimpleTestData();
            
            var filter = Filters
                .CreateFilter<User>(x => x.Name == "name1" && x.Age >= 5)
                .AndFilteredOn<User>(x => x.Email == "Email@email1.com");
            
            var result = client.Search<User>(sc => sc
                .FilterOn(filter)
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
            var users = client.Search<User>(s => s.FilterOn(x=> x.Name == "name1" || x.Age >= 5));
            Check.That(users.Documents).HasSize(7);
        }

        [Fact]
        public void Noq_equal_Filter_Test()
        {
            AddSimpleTestData();

            var filter = Filters
                .CreateFilter<User>(x => x.Name != "name1" && x.Name != "name2");

            var allUsers = client.Search<User>(s => s.Index(userIndex).Query(_ => filter));
            Check.That(allUsers.Documents).HasSize(4);
        }

        [Fact]
        public void Bool_filter_test()
        {
            AddSimpleTestData();
            var allUsers = client.Search<User>(s=>s.Index(userIndex).FilterOn(f=>f.Active));
            Check.That(allUsers.Documents).HasSize(5);
        }

        [Fact]
        public void Time_equality_filter()
        {
            AddSimpleTestData();
            
            var allUsers = client.Search<Car>(s=>s.FilterOn(x=>x.Timestamp == new DateTime(2010,1,1)));
            Check.That(allUsers.Documents).HasSize(1);
        }

        [Fact]
        public void Decimal_Filter_Comparison_Test()
        {
            AddSimpleTestData();
            var allUsers = client.Search<Car>(s => s.FilterOn(x=>x.Emissions > 2 && x.Emissions < 6));
            Check.That(allUsers.Documents).HasSize(3);
        }

        [Fact]
        public void Filter_ValueWithin_Test()
        {
            AddSimpleTestData();
            var list = new List<string> {"name1", "name2"};
            var users = client.Search<User>(sc => sc.FilterOn(Filters.ValueWithin<User>(x => x.Name, list)));
            Check.That(users.Documents).HasSize(6);
        }

        [Fact]
        public void Filter_ValueWithin_OnExistingFilter()
        {
            AddSimpleTestData();
            var filter = Filters.CreateFilter<User>(x => x.Age > 8);
            var sc = new SearchDescriptor<User>().FilterOn(filter.AndValueWithin<User>(x=>x.Name, new List<string> { "name1", "name2" } ));
            var allUsers = client.Search<User>(sc);
            Check.That(allUsers.Documents).HasSize(1);
        }

        [Fact]
        public void Guid_Filter_Test()
        {
            var result = client.Search<Car>(sc => sc.Index(CarIndex).FilterOn(x => x.Guid == MyFavoriteGuid));
            Check.That(result.Documents).HasSize(1);
        }
    }
}

