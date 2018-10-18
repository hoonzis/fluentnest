
namespace FluentNest
{
    using System;
    using System.Linq.Expressions;

    public static class ExpressionFuncExtension
    {
        public static string GetName<T, TK>(this Expression<Func<T, TK>> exp)
        {
            var fromGetFieldName = exp.Body.GetNameFromGetFieldNamed();
            if (fromGetFieldName != null)
            {
                return fromGetFieldName;
            }

            if (exp.Body is MemberExpression memberBody)
            {
                return memberBody.Member.Name;
            }

            if (exp.Body is UnaryExpression unaryBody)
            {
                if (unaryBody.Operand is MemberExpression unaryExpression)
                {
                    return unaryExpression.Member.Name;
                }
            }

            throw new NotImplementedException($"Left side expression too complicated - could not deduce name from {exp}");
        }

        public static string GetAggName<T, TK>(this Expression<Func<T, TK>> exp, AggType type)
        {
            var name = GetName(exp);
            return type + name;
        }
    }
}
