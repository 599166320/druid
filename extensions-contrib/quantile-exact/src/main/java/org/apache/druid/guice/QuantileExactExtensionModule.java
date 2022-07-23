package org.apache.druid.guice;
import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.aggregation.sql.QuantileExactSqlAggregator;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.Collections;
import java.util.List;
public class QuantileExactExtensionModule implements DruidModule
{
    @Override
    public List<? extends Module> getJacksonModules()
    {
        return Collections.singletonList(new QuantileExactSerializersModule());
    }
    @Override
    public void configure(Binder binder)
    {
        SqlBindings.addAggregator(binder, QuantileExactSqlAggregator.class);
    }
}
