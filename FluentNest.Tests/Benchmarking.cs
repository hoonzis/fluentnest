using System;
using System.Collections.Generic;
using System.Diagnostics;
using FluentNest.Tests.Model;
using Nest;
using NFluent;
using Tests;
using Xunit;

namespace FluentNest.Tests
{
    public class Benchmarking : TestsBase
    {
        private string AddSimpleTestData()
        {
            var indexName = "index_" + Guid.NewGuid();

            Client.CreateIndex(indexName, x => x.Mappings(m => m
            .Map<Car>(t => t
                .Properties(prop => prop.String(str => str.Name(s => s.Guid).Index(FieldIndexOption.NotAnalyzed)))
                .Properties(prop => prop.String(str => str.Name(s => s.Email).Index(FieldIndexOption.NotAnalyzed)))
            )));

            var cars = new List<Car>();
            for (int i = 0; i < 10000; i++)
            {
                var car = new Car
                {
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
                    EngineType = i % 2 == 0 ? EngineType.Diesel : EngineType.Standard
                };
                cars.Add(car);
            }
            Client.Bulk(x => x.CreateMany(cars).Index(indexName));
            Client.Flush(indexName);
            return indexName;
        }

        // This should confirm the theory that a two side range is faster then a bool with 2-one sided ranges
        [Fact]
        public void WithMergedRange()
        {
            var stopWatch = new Stopwatch();
            var index = AddSimpleTestData();
            var sc = new SearchDescriptor<Car>().Index(index).FilterOn(x => x.Emissions > 2 && x.Emissions < 6 && x.Price < 20);
            var json = Serialize(sc);
            Console.WriteLine(json);

            stopWatch.Start();
            var allCars = Client.Search<Car>(sc);
            stopWatch.Stop();

            Console.WriteLine("Query time:" + stopWatch.Elapsed);
            Check.That(allCars.Documents).HasSize(3);
        }

        [Fact]
        public void WithoutMergedRange()
        {
            var stopWatch = new Stopwatch();
            var index = AddSimpleTestData();
            var sc = new SearchDescriptor<Car>().Index(index).FilterOn(x => x.Emissions > 2 && x.Price < 20 && x.Emissions < 6);
            var json = Serialize(sc);
            Console.WriteLine(json);

            stopWatch.Start();
            var allCars2 = Client.Search<Car>(sc);
            stopWatch.Stop();

            Console.WriteLine("Query time:" + stopWatch.Elapsed);
            Check.That(allCars2.Documents).HasSize(3);
        }


        [Fact]
        public void WithoutMergedAndFilters()
        {
            var stopWatch = new Stopwatch();
            var index = AddSimpleTestData();
            Filters.OptimizeAndFilters = false;

            var sc =
                new SearchDescriptor<Car>().Index(index).FilterOn(
                    x =>
                        x.Emissions < 6 && x.Sold == true && x.Price > 4 && x.EngineType == EngineType.Diesel &&
                        x.Length < 4);

            var json = Serialize(sc);
            Console.WriteLine(json);

            stopWatch.Start();
            var allCars = Client.Search<Car>(sc);
            stopWatch.Stop();

            Console.WriteLine("Query time:" + stopWatch.Elapsed); Console.Write("Query time:" + stopWatch.Elapsed);
            Check.That(allCars.Documents).HasSize(3);
        }

        [Fact]
        public void WithMergedAndFilters()
        {
            var stopWatch = new Stopwatch();
            var index = AddSimpleTestData();
            Filters.OptimizeAndFilters = true;
            var sc =
                new SearchDescriptor<Car>().Index(index).FilterOn(
                    x =>
                        x.Emissions < 6 && x.Sold == true && x.Price > 4 && x.EngineType == EngineType.Diesel &&
                        x.Length < 4);

            var json = Serialize(sc);
            Console.WriteLine(json);

            stopWatch.Start();
            var cars = Client.Search<Car>(sc);
            stopWatch.Stop();

            Console.WriteLine("Query time:" + stopWatch.Elapsed);
            Check.That(cars.Documents).HasSize(3);
        }
    }
}
