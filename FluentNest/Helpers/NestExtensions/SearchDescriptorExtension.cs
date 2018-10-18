

namespace FluentNest
{
    using System;
    using System.Linq.Expressions;
    using Nest;

    public static class SearchDescriptorNestExtension
    {
        public static SearchDescriptor<T> FilterOn<T>(this SearchDescriptor<T> searchDescriptor, Expression<Func<T, bool>> filterRule) where T : class
        {
            var filterDescriptor = filterRule.Body.GenerateFilterDescription<T>();
            return searchDescriptor.Query(_ => filterDescriptor);
        }

        public static SearchDescriptor<T> FilterOn<T>(this SearchDescriptor<T> searchDescriptor, QueryContainer container) where T : class
        {
            return searchDescriptor.Query(q => container);
        }
    }
}
