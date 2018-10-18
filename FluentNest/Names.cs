namespace FluentNest
{
    using System;

    public static class Names
    {
        public static TField GetFieldNamed<TField>(this object target, string name)
        {
            throw new InvalidOperationException("This method should be used in extensions.");
        }
    }
}
