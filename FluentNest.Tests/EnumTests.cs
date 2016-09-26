using System;
using System.Collections.Generic;
using System.Linq;
using FluentNest.Tests.Model;
using Nest;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using NFluent;
using Tests;
using Xunit;

namespace FluentNest.Tests
{
    public class EnumTests : TestsBase
    {
        private class StringEnumContractSerializer : JsonNetSerializer
        {
            public StringEnumContractSerializer(IConnectionSettingsValues settings)
                : base(settings)
            {

            }
            protected override IList<Func<Type, JsonConverter>> ContractConverters
                => new List<Func<Type, JsonConverter>>()
                {
                    t => t.IsEnum ? new StringEnumConverter() : null
                };
        }

        public EnumTests()
            : base(x => new StringEnumContractSerializer(x))
        {

        }

        public string AddSimpleTestData()
        {
            var indexName = "index_" + Guid.NewGuid();
            Client.CreateIndex(indexName, x => x.Mappings(
                m => m.Map<Car>(t => t
                    .Properties(prop => prop.String(str => str.Name(s => s.EngineType).Index(FieldIndexOption.NotAnalyzed))))));

            for (int i = 0; i < 10; i++)
            {
                var car = new Car
                {
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
        public void Filtering_on_enum_property_should_work()
        {
            var index = AddSimpleTestData();
            var result = Client.Search<Car>(s => s.Index(index).FilterOn(x => x.EngineType == EngineType.Diesel));
            Check.That(result.Hits.Count()).IsEqualTo(5);
            Client.DeleteIndex(index);
        }
    }
}
