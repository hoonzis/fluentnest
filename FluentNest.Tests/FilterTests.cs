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

        private string AddSimpleTestData()
        {
            var indexName = "index_" + Guid.NewGuid();
       
            Client.CreateIndex(indexName, x => x.Mappings(m => m
            .Map<Car>(t => t
                .Properties(prop => prop.Keyword(str => str.Name(s => s.Guid)))
                .Properties(prop => prop.Keyword(str => str.Name(s => s.Email)))
            )));

            var cars = new List<Car>();
            for (int i = 0; i < 10; i++)
            {
                var car = new Car
                {
                    Id = Guid.NewGuid(),
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
                cars.Add(car);
            }
            Client.Bulk(x => x.CreateMany(cars).Index(indexName));
            Client.Flush(indexName);
            return indexName;
        }


        [Fact]
        public void DateComparisonAndTerm()
        {
            var index = AddSimpleTestData();

            var startDate = new DateTime(2010, 1, 1);
            var endDate = new DateTime(2010, 3, 1);
            var result = Client.Search<Car>(s => s.Index(index).FilterOn(x => x.Timestamp >= startDate && x.Timestamp <= endDate && x.CarType == "type0"));
            Client.DeleteIndex(index);
            
            Check.That(result.Documents).HasSize(2);
        }

        [Fact]
        public void DateEqualityTest()
        {
            var index = AddSimpleTestData();

            var startDate = new DateTime(2010, 1, 1);
            var result = Client.Search<Car>(s => s.Index(index).FilterOn(x => x.Timestamp == startDate));
            Client.DeleteIndex(index);
            Check.That(result.Documents).HasSize(1);
        }
        
        [Fact]
        public void TestEqualityFilter()
        {
            var index = AddSimpleTestData();

            var carType = "Type0".ToLower();
            // Standard Nest way of getting the documents. Values are lowered by ES
            var result = Client.Search<Car>(s => s.Index(index).Query(x => x.Term(f => f.CarType, carType)));
            Check.That(result.Documents).HasSize(5);
            
            // Best way
            result = Client.Search<Car>(s => s.Index(index).FilterOn(x => x.CarType == carType));
            Check.That(result.Documents).HasSize(5);
            Client.DeleteIndex(index);
        }

        [Fact]
        public void TestRangeFilters_And_And()
        {
            var index = AddSimpleTestData();

            var startDate = new DateTime(2010, 1, 1);
            var endDate = new DateTime(2010, 5, 1);

            var result = Client.Search<Car>(s => s.Index(index).Query(
                q => q.Bool(b => b.Must(left => left.DateRange(f => f.Field(fd => fd.Timestamp).GreaterThan(startDate)), 
                                        right => right.DateRange(f => f.Field(fd => fd.Timestamp).LessThan(endDate)))
                    )
                ));
            Check.That(result.Documents).HasSize(3);

            //Much better
            result = Client.Search<Car>(s => s.Index(index).FilterOn(f => f.Timestamp > startDate && f.Timestamp < endDate));
            Check.That(result.Documents).HasSize(3);
            Client.DeleteIndex(index);
        }

        [Fact]
        public void FilterOnSpecialCharacter()
        {
            var index = AddSimpleTestData();

            // these two searches should provide the same result
            var result =
                Client.Search<Car>(
                    s =>
                        s.Index(index)
                            .Query(
                                q =>
                                    q.Bool(
                                        b => b.Must(x => x.Term(term => term.Email, "Email@email1.com")))));
            Check.That(result.Documents).HasSize(5);

            result = Client.Search<Car>(s => s.Index(index).FilterOn(f => f.Email == "Email@email1.com"));
            Check.That(result.Documents).HasSize(5);
            Client.DeleteIndex(index);
        }

        [Fact]
        public void TestConsecutiveFilters()
        {
            var index = AddSimpleTestData();

            var filter = Filters
                .CreateFilter<Car>(x => x.Name == "name1" && x.Age >= 5)
                .AndFilteredOn<Car>(x => x.Email == "Email@email1.com");

            var cars = Client.Search<Car>(s => s.Index(index).Query(_ => filter));
            Client.DeleteIndex(index);
            Check.That(cars.Documents).HasSize(1);
        }

        [Fact]
        public void TestConsecutiveFiltersOnBoolean()
        {
            var index = AddSimpleTestData();

            var filter = Filters
                .CreateFilter<Car>(x => x.Name == "name1" && x.Age >= 5)
                .AndFilteredOn<Car>(x => x.Active);

            var cars = Client.Search<Car>(s => s.Index(index).Query(_ => filter));
            Client.DeleteIndex(index);
            Check.That(cars.Documents).HasSize(1);
        }

        [Fact]
        public void MultipleFiltersAndSomeAggregations()
        {
            var index = AddSimpleTestData();
            
            var filter = Filters
                .CreateFilter<Car>(x => x.Name == "name1" && x.Age >= 5)
                .AndFilteredOn<Car>(x => x.Email == "Email@email1.com");
            
            var result = Client.Search<Car>(sc => sc
                .Index(index)
                .FilterOn(filter)
                .Aggregations(agg => agg
                .SumBy(x=>x.Age)
            ));

            var sumValue = result.Aggregations.GetSum<Car, int>(x => x.Age);

            var aggsContainer = result.Aggregations.AsContainer<Car>();
            var sum2 = aggsContainer.GetSum(x => x.Age);
            Client.DeleteIndex(index);
            Check.That(sumValue).Equals(8);
            Check.That(sum2).Equals(8);
        }

        [Fact]
        public void Or_Filter_Test()
        {
            var index = AddSimpleTestData();
            var cars = Client.Search<Car>(s => s.Index(index).FilterOn(x=> x.Name == "name1" || x.Age >= 5));
            Client.DeleteIndex(index);
            Check.That(cars.Documents).HasSize(7);
        }

        [Fact]
        public void Noq_equal_Filter_Test()
        {
            var index = AddSimpleTestData();

            var filter = Filters
                .CreateFilter<Car>(x => x.Name != "name1" && x.Name != "name2");

            var allCars = Client.Search<Car>(s => s.Index(index).Query(_ => filter));
            Check.That(allCars.Documents).HasSize(4);
            Client.DeleteIndex(index);
        }

        [Fact]
        public void Bool_filter_test()
        {
            var index = AddSimpleTestData();
            var allCars = Client.Search<Car>(s=>s.Index(index).FilterOn(f=>f.Active));
            Check.That(allCars.Documents).HasSize(5);

            var filter = Filters.CreateFilter<Car>(x => x.Enabled);

            var allCars2 = Client.Search<Car>(s => s.Index(index).Query(_ => filter));
            Check.That(allCars2.Documents).HasSize(5);
            Client.DeleteIndex(index);
        }
        
        [Fact]
        public void Decimal_Two_Side_Range_Test()
        {
            var index = AddSimpleTestData();
            var allCars = Client.Search<Car>(s => s.Index(index).FilterOn(x=>x.Emissions > 2 && x.Emissions < 6));
            Client.DeleteIndex(index);
            Check.That(allCars.Documents).HasSize(3);
        }

        [Fact]
        public void Guid_Filter_Test()
        {
            var index = AddSimpleTestData();
            var result = Client.Search<Car>(sc => sc.Index(index).FilterOn(x => x.Guid == MyFavoriteGuid));
            Check.That(result.Documents).HasSize(1);
            Client.DeleteIndex(index);
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
            var sc = new SearchDescriptor<Car>().FilterOn(Filters.ValueWithin<Car>(x => x.Name, list));
            CheckSD(sc, "Filter_ValueWithin_Test");
        }

        [Fact]
        public void Filter_ValueWithin_AddedOnExistingFilter()
        {
            var filter = Filters.CreateFilter<Car>(x => x.Age > 8);
            var sc = new SearchDescriptor<Car>().FilterOn(filter.AndValueWithin<Car>(x=>x.Name, new List<string> { "name1", "name2" } ));
            CheckSD(sc, "Filter_ValueWithin_AddedOnExistingFilter");
        }
    }
}

