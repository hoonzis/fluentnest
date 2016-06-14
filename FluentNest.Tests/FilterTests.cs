using System;
using System.Collections.Generic;
using FluentNest.Tests.Model;
using Nest;
using NFluent;
using Tests;
using Xunit;

namespace FluentNest.Tests
{
    public class FilterTests : TestsBase
    {
        private const string MyFavoriteGuid = "test-test";

        private void AddSimpleTestData()
        {
            Client.DeleteIndex(CarIndex);
            Client.CreateIndex(CarIndex, x => x.Mappings(m => m
            .Map<Car>(t => t
                .Properties(prop => prop.String(str => str.Name(s => s.Guid).Index(FieldIndexOption.NotAnalyzed)))
                .Properties(prop => prop.String(str => str.Name(s => s.Email).Index(FieldIndexOption.NotAnalyzed)))
            )));

            for (int i = 0; i < 10; i++)
            {
                var car = new Car
                {
                    Timestamp = new DateTime(2010,(i%12)+1,1),
                    Name = "name"+i%3,
                    Price = 10,
                    Sold = i % 2 == 0,
                    CarType = "Type" + i%2,
                    Emissions = i+1,
                    Guid = "test-" + i,
                    Email = "Email@email" + i % 2 + ".com",
                    Age = i + 1,
                    Enabled = i % 2 == 0,
                    Active = i % 2 == 0
                };
                if (i == 1)
                {
                    car.Guid = MyFavoriteGuid;
                }
                Client.Index(car, ind => ind.Index(CarIndex));
            }
            Client.Flush(CarIndex);
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
            var result = Client.Search<Car>(s => s.Query(x => x.Term(f => f.CarType, carType)));
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
                q => q.Bool(b => b.Must(left => left.DateRange(f => f.Field(fd => fd.Timestamp).GreaterThan(startDate)), 
                                        right => right.DateRange(f => f.Field(fd => fd.Timestamp).LessThan(endDate)))
                    )
                ));
            Check.That(result.Documents).HasSize(3);

            //Much better
            result = Client.Search<Car>(s => s.FilterOn(f => f.Timestamp > startDate && f.Timestamp < endDate));
            Check.That(result.Documents).HasSize(3);
        }

        [Fact]
        public void FilterOnSpecialCharacter()
        {
            AddSimpleTestData();

            //these two searches should provide the same result
            var result =
                Client.Search<Car>(
                    s =>
                        s.Index(CarIndex)
                            .Query(
                                q =>
                                    q.Bool(
                                        b => b.Must(x => x.Term(term => term.Email, "Email@email1.com")))));
            Check.That(result.Documents).HasSize(5);

            result = Client.Search<Car>(s => s.Index(CarIndex).FilterOn(f => f.Email == "Email@email1.com"));
            Check.That(result.Documents).HasSize(5);
        }

        [Fact]
        public void TestConsecutiveFilters()
        {
            AddSimpleTestData();

            var filter = Filters
                .CreateFilter<Car>(x => x.Name == "name1" && x.Age >= 5)
                .AndFilteredOn<Car>(x => x.Email == "Email@email1.com");

            var Cars = Client.Search<Car>(s => s.Query(_ => filter));
            Check.That(Cars.Documents).HasSize(1);            
        }

        [Fact]
        public void TestBooleanFilter()
        {
            AddSimpleTestData();

            var filter = Filters
                .CreateFilter<Car>(x => x.Enabled == true);

            var allCars = Client.Search<Car>(s => s.Index(CarIndex).Query(_ => filter));
            Check.That(allCars.Documents).HasSize(5);
        }

        [Fact]
        public void MultipleFiltersAndSomeAggregations()
        {
            AddSimpleTestData();
            
            var filter = Filters
                .CreateFilter<Car>(x => x.Name == "name1" && x.Age >= 5)
                .AndFilteredOn<Car>(x => x.Email == "Email@email1.com");
            
            var result = Client.Search<Car>(sc => sc
                .FilterOn(filter)
                .Aggregations(agg => agg
                .SumBy(x=>x.Age)
            ));

            var sumValue = result.Aggs.GetSum<Car, int>(x => x.Age);

            var aggsContainer = result.Aggs.AsContainer<Car>();
            var sum2 = aggsContainer.GetSum(x => x.Age);
            Check.That(sumValue).Equals(8);
            Check.That(sum2).Equals(8);
        }

        [Fact]
        public void Or_Filter_Test()
        {
            AddSimpleTestData();
            var Cars = Client.Search<Car>(s => s.FilterOn(x=> x.Name == "name1" || x.Age >= 5));
            Check.That(Cars.Documents).HasSize(7);
        }

        [Fact]
        public void Noq_equal_Filter_Test()
        {
            AddSimpleTestData();

            var filter = Filters
                .CreateFilter<Car>(x => x.Name != "name1" && x.Name != "name2");

            var allCars = Client.Search<Car>(s => s.Index(CarIndex).Query(_ => filter));
            Check.That(allCars.Documents).HasSize(4);
        }

        [Fact]
        public void Bool_filter_test()
        {
            AddSimpleTestData();
            var allCars = Client.Search<Car>(s=>s.Index(CarIndex).FilterOn(f=>f.Active));
            Check.That(allCars.Documents).HasSize(5);
        }

        [Fact]
        public void Time_equality_filter()
        {
            AddSimpleTestData();
            
            var allCars = Client.Search<Car>(s=>s.FilterOn(x=>x.Timestamp == new DateTime(2010,1,1)));
            Check.That(allCars.Documents).HasSize(1);
        }

        [Fact]
        public void Decimal_Filter_Comparison_Test()
        {
            AddSimpleTestData();
            var allCars = Client.Search<Car>(s => s.FilterOn(x=>x.Emissions > 2 && x.Emissions < 6));
            Check.That(allCars.Documents).HasSize(3);
        }

        [Fact]
        public void Filter_ValueWithin_Test()
        {
            AddSimpleTestData();
            var list = new List<string> {"name1", "name2"};
            var Cars = Client.Search<Car>(sc => sc.FilterOn(Filters.ValueWithin<Car>(x => x.Name, list)));
            Check.That(Cars.Documents).HasSize(6);
        }

        [Fact]
        public void Filter_ValueWithin_OnExistingFilter()
        {
            AddSimpleTestData();
            var filter = Filters.CreateFilter<Car>(x => x.Age > 8);
            var sc = new SearchDescriptor<Car>().FilterOn(filter.AndValueWithin<Car>(x=>x.Name, new List<string> { "name1", "name2" } ));
            var allCars = Client.Search<Car>(sc);
            Check.That(allCars.Documents).HasSize(1);
        }

        [Fact]
        public void Guid_Filter_Test()
        {
            AddSimpleTestData();
            var result = Client.Search<Car>(sc => sc.Index(CarIndex).FilterOn(x => x.Guid == MyFavoriteGuid));
            Check.That(result.Documents).HasSize(1);
        }
    }
}

