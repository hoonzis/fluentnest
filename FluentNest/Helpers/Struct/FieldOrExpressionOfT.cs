using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using Nest;

namespace FluentNest
{
    public struct FieldOrExpression<T>
    {
        public Field Field;
        public Expression<Func<T, object>> Expression;

        public FieldOrExpression(Expression<Func<T, object>> expression, string  fieldName)
        {
            Field = new Field(fieldName);
            Expression = expression;
        }
    }
}
