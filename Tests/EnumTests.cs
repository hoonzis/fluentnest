using System;
using System.Linq;
using FluentNest;
using Nest;
using Newtonsoft.Json.Converters;
using NFluent;
using TestModel;
using Xunit;

namespace Tests
{
    public class EnumTests : TestsBase
    {
        private static readonly Func<ConnectionSettings, ConnectionSettings> AddEnumStringConverter =
            setting => setting.AddContractJsonConverters(t => t.IsEnum ? new StringEnumConverter() : null);

        public EnumTests()
            : base(AddEnumStringConverter)
        {

        }

        [Fact]
        public void Filtering_on_enum_property_should_work()
        {
            AddSimpleTestData();
            var result = client.Search<Car>(s => s.Filter(Filters.CreateFilter<Car>(x => x.EngineType == EngineType.Diesel)));

            Check.That(result.Hits.Count()).IsEqualTo(5);
        }
    }
}
