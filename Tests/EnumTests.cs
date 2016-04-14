using System;
using System.Collections.Generic;
using System.Linq;
using FluentNest;
using Nest;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using NFluent;
using TestModel;
using Xunit;

namespace Tests
{
    public class EnumTests : TestsBase
    {

        private class StringEnumContractSerializer : JsonNetSerializer
        {
            public StringEnumContractSerializer(IConnectionSettingsValues settings)
                : base (settings)
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

        [Fact]
        public void Filtering_on_enum_property_should_work()
        {
            AddSimpleTestData();
            var result = client.Search<Car>(s => s.Query(_ => Filters.CreateFilter<Car>(x => x.EngineType == EngineType.Diesel)));

            Check.That(result.Hits.Count()).IsEqualTo(5);
        }
    }
}
