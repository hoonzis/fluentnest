namespace FluentNest
{
    using System;
    using System.Linq.Expressions;
    using Nest;

    public static class DeleteByQueryDescriptorNestExtension
    {
        public static DeleteByQueryDescriptor<T> FilterOn<T>(this DeleteByQueryDescriptor<T> deleteDescriptor, QueryContainer container) where T : class
        {
            return deleteDescriptor.Query(q => container);
        }

        public static DeleteByQueryDescriptor<T> FilterOn<T>(this DeleteByQueryDescriptor<T> deleteDescriptor, Expression<Func<T, bool>> filterRule) where T : class
        {
            var filterDescriptor = filterRule.Body.GenerateFilterDescription<T>();
            return deleteDescriptor.Query(_ => filterDescriptor);
        }
    }
}
