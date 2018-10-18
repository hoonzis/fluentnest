

namespace FluentNest
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using Nest;

    public static class QueryContainerNestExtension
    {

        public static QueryContainer AndFilteredOn<T>(this QueryContainer queryDescriptor, Expression<Func<T, bool>> filterRule) where T : class
        {
            var filterDescriptor = new QueryContainerDescriptor<T>();
            var newPartOfQuery = filterRule.Body.GenerateFilterDescription<T>();
            return filterDescriptor.Bool(x => x.Must(queryDescriptor, newPartOfQuery));
        }

        public static QueryContainer AndValueWithin<T>(this QueryContainer queryDescriptor, Expression<Func<T, Object>> fieldGetter, IEnumerable<string> list) where T : class
        {
            var filterDescriptor = new QueryContainerDescriptor<T>();
            var newFilter = new QueryContainerDescriptor<T>();
            var newPartOfQuery = newFilter.Terms(terms => terms.Terms(list).Field(fieldGetter));
            return filterDescriptor.Bool(x => x.Must(queryDescriptor, newPartOfQuery));
        }

        public static QueryContainer AndValueWithin<T>(this QueryContainer queryDescriptor, Expression<Func<T, Object>> fieldGetter, string item) where T : class
        {
            var filterDescriptor = new QueryContainerDescriptor<T>();
            var newFilter = new QueryContainerDescriptor<T>();
            var newPartOfQuery = newFilter.Term(term => term.Value(item).Field(fieldGetter));
            return filterDescriptor.Bool(x => x.Must(queryDescriptor, newPartOfQuery));
        }
    }
}
