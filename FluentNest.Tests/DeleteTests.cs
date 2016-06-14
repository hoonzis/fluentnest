using System;
using FluentNest.Tests.Model;
using Nest;
using NFluent;
using Tests;
using Xunit;

namespace FluentNest.Tests
{
    public class DeleteTests : TestsBase
    {
        public void AddSimpleTestData()
        {
            Client.DeleteIndex(CarIndex);
            Client.CreateIndex(CarIndex, x => x.Mappings(
                m => m.Map<Car>(t => t
            .Properties(prop => prop.String(str => str.Name(s => s.EngineType).Index(FieldIndexOption.NotAnalyzed))))));

            for (int i = 0; i < 10; i++)
            {
                var car = new Car
                {
                    Timestamp = new DateTime(2010, i + 1, 1),
                    Sold = i % 2 == 0,
                    CarType = "Type" + i % 3,
                    Length = i,
                    EngineType = i % 2 == 0 ? EngineType.Diesel : EngineType.Standard,
                    Weight = 5
                };

                Client.Index(car);
            }
            Client.Flush(CarIndex);
        }

        [Fact]
        public void DeleteByQuery()
        {
            AddSimpleTestData();
            var deleteResult = Client.DeleteByQuery<Car>(CarIndex, typeof(Car),s  => s.FilterOn(x => x.Sold));
            Check.That(deleteResult.IsValid).IsTrue();
            Client.Refresh(CarIndex);
            var result = Client.Search<Car>(sc=>sc.Index(CarIndex).MatchAll());
            Check.That(result.Hits).HasSize(5);
        }

        [Fact]
        public void DeleteByQuery_FilterCreatedSeparately()
        {
            AddSimpleTestData();
            var filter = Filters.CreateFilter<Car>(x => x.EngineType == EngineType.Diesel);
            var deleteResult = Client.DeleteByQuery<Car>(CarIndex, Types.AllTypes, s => s.FilterOn(filter));
            Check.That(deleteResult.IsValid).IsTrue();
            Client.Refresh(CarIndex);
            var result = Client.Search<Car>(sc => sc.MatchAll());
            Check.That(result.Hits).HasSize(5);
        }
    }
}
