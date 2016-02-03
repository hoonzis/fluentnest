using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Elasticsearch.Net;
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
            var filter = NestHelperMethods.CreateFilter<Car>(x => x.EngineType == EngineType.Diesel);

            var result = client.Search<Car>(s => s
                .Take(100).Query(_ => filter));

            Check.That(result.Hits.Count()).IsEqualTo(5);
        }
    }
}
