using System;
using System.Collections.Generic;
using System.Linq;
using FluentNest.Tests.Model;
using Nest;
using NFluent;
using Tests;
using Xunit;
using Elasticsearch.Net;

namespace FluentNest.Tests
{
    public class FilterTests : TestsBase
    {
        private const string MyFavoriteGuid = "test-test";
       

        private string AddSimpleTestData()
        {
            var indexName = "index_" + Guid.NewGuid();

            if (Client.IndexExists(Indices.Parse(indexName)).Exists)
            {
                Client.DeleteIndex(Indices.Parse(indexName));
            }


            Client.CreateIndex(indexName, x => x.Mappings(m => m
            .Map<Car>(t => t
                .Properties(prop => prop.Keyword(str => str.Name(s => s.Guid)))
                .Properties(prop => prop.Keyword(str => str.Name(s => s.Email)))
                .Properties(prop => prop.Keyword(str => str.Name(s => s.PreviousOwners)))
                .Properties(prop => prop.Nested<Tyres>(str => str.Name(s => s.TyresInstalled).AutoMap()))
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
                    Active = i % 2 == 0,
                    Weight = i % 3 == 0 ? 10m : (decimal?)null,
                    PreviousOwners = i % 2 == 0 ? null : i % 3 == 0 ? new string[0] : Enumerable.Range(0, i).Select(n => $"Owner n°{n}").ToArray(),
                    TyresInstalled = GetTyres("Default",1,false,5m)
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

        private List<Tyres> GetTyres(string company,int companyid, bool isPunctured, decimal weight,int tyresCount=4)
        {
            var typres = new List<Tyres>();
            for(int i = 0; i < tyresCount;i++)
            {
                typres.Add(
                    new Tyres()
                    {
                        Age = i + 1,
                        Company = company,
                        CompanyId = companyid,
                        InstallationDate = new DateTime(2010, (i % 12) + 1, 1),
                        IsPuncture = isPunctured,
                        Weight = weight,
                    });
            }

            return typres;
        }


        [Fact]
        public void NestedComparisonAndTerm()
        {
            var index= AddSimpleTestData();
            int i = 5;

            var tyres = GetTyres("Default", 1, false, 5m,3);
            tyres.AddRange(GetTyres("MRF", 1, true, 5m, 1));
            
           var indexResponse= Client.Index(new Car()
            {
                Id = Guid.NewGuid(),
                Timestamp = new DateTime(2010, (i % 12) + 1, 1),
                Name = "name" + i ,
                Price = 10,
                Sold = i % 2 == 0,
                CarType = "Type" + i ,
                Emissions = i + 1,
                Guid = "test-" + i,
                Email = "Email@email" + i % 2 + ".com",
                Age = i + 1,
                Enabled = i % 2 == 0,
                Active = i % 2 == 0,
                Weight = i % 3 == 0 ? 10m : (decimal?) null,
                PreviousOwners = i % 2 == 0 ? null :
                    i % 3 == 0 ? new string[0] : Enumerable.Range(0, i).Select(n => $"Owner n°{n}").ToArray(),
                TyresInstalled = tyres
            }, x=>x.Index(index));

            indexResponse= Client.Index(new Car()
            {
                Id = Guid.NewGuid(),
                Timestamp = new DateTime(2010, (i % 12) + 1, 1),
                Name = "name" + i % 3,
                Price = 10,
                Sold = i % 2 == 0,
                CarType = "Type" + i % 2,
                Emissions = i + 1,
                Guid = "test-" + i,
                Email = "Email@email" + i % 2 + ".com",
                Age = i + 1,
                Enabled = i % 2 == 0,
                Active = i % 2 == 0,
                Weight = i % 3 == 0 ? 10m : (decimal?)null,
                PreviousOwners = i % 2 == 0 ? null :
                    i % 3 == 0 ? new string[0] : Enumerable.Range(0, i).Select(n => $"Owner n°{n}").ToArray(),
                TyresInstalled = tyres
            }, x => x.Index(index));
            Client.Flush(index);

            var result = Client.Search<Car>(s => s.Index(index).FilterOn(x => x.TyresInstalled.Any(t=>t.IsPuncture)));
            Check.That(result.Documents).HasSize(2);
            Client.DeleteIndex(index);
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
        public void NullableDateComparisonAndTerm()
        {
            var index = AddSimpleTestData();

            DateTime? startDate = null;
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
        public void Null_filter_test()
        {
            var index = AddSimpleTestData();
            var allCars = Client.Search<Car>(sd => sd.Index(index).Query(x => x.Bool(b => b.MustNot(n => n.Exists(c => c.Field(s => s.Weight))))));
            Check.That(allCars.Documents).HasSize(6);

            var filter = Filters.CreateFilter<Car>(x => x.Weight == null);

            var weightlessCars = Client.Search<Car>(s => s.Index(index).Query(_ => filter));
            Check.That(weightlessCars.Documents).HasSize(6);
            Client.DeleteIndex(index);
        }

        [Fact]
        public void NotNull_filter_test()
        {
            var index = AddSimpleTestData();
            var allCars = Client.Search<Car>(sd => sd.Index(index).Query(x => x.Bool(b => b.Must(n => n.Exists(c => c.Field(s => s.Weight))))));
            Check.That(allCars.Documents).HasSize(4);

            var filter = Filters.CreateFilter<Car>(x => x.Weight != null);

            var weightlessCars = Client.Search<Car>(s => s.Index(index).Query(_ => filter));
            Check.That(weightlessCars.Documents).HasSize(4);
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
        public void Filter_ValueWithin_SingleItem()
        {
            var item = "Owner n°0";
            var index = AddSimpleTestData();
            var result = Client.Search<Car>(sc => sc.Index(index).FilterOn(Filters.ValueWithin<Car>(x => x.PreviousOwners, item)).TypedKeys(null));
            Check.That(result.Documents).Not.HasSize(0);
            foreach (var previousOwners in result.Documents.Select(d => d.PreviousOwners))
            {
                Check.That(previousOwners).Contains(item);
            }

            Client.DeleteIndex(index);
        }

        [Fact]
        public void Filter_ValueWithin_MultipleItems()
        {
            var items = new[] { "Owner n°0", "Onwer n°1" };
            var index = AddSimpleTestData();
            var result = Client.Search<Car>(sc => sc.Index(index).FilterOn(Filters.ValueWithin<Car>(x => x.PreviousOwners, items)).TypedKeys(null));
            Check.That(result.Documents).Not.HasSize(0);
            foreach (var previousOwners in result.Documents.Select(d => d.PreviousOwners))
            {
                Check.That(previousOwners.Join(items, a => a, b => b, (a, b) => 0)).Not.HasSize(0);
            }

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
            var sc = new SearchDescriptor<Car>().FilterOn(x => x.Sold && x.Age < 6 && x.Emissions < 5);
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

        [Fact]
        public void Custom_Field_Name_Test()
        {
            var sc = new SearchDescriptor<Car>().FilterOn(x => x.GetFieldNamed<bool>("sold") && x.GetFieldNamed<int>("age") < 6 && x.GetFieldNamed<decimal>("emissions") < 5);
            CheckSD(sc, "Three_Ands_Test");
        }

        [Fact]
        public void Concatenated_custom_Field_Name_Test()
        {
            var old = "old";

            DoTest(old);

            void DoTest(string o)
            {
                var sc = new SearchDescriptor<Car>().FilterOn(x => x.GetFieldNamed<bool>("s" + o) && x.GetFieldNamed<int>("age") < 6 && x.GetFieldNamed<decimal>("emissions") < 5);
                CheckSD(sc, "Three_Ands_Test");
            }
        }
    }
}

