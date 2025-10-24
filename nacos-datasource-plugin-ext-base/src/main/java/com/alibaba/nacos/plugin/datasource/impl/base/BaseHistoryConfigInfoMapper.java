package com.alibaba.nacos.plugin.datasource.impl.base;

import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.plugin.datasource.constants.FieldConstant;
import com.alibaba.nacos.plugin.datasource.dialect.DatabaseDialect;
import com.alibaba.nacos.plugin.datasource.impl.mysql.HistoryConfigInfoMapperByMySql;
import com.alibaba.nacos.plugin.datasource.manager.DatabaseDialectManager;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

public class BaseHistoryConfigInfoMapper extends HistoryConfigInfoMapperByMySql {
    private DatabaseDialect databaseDialect;

    @Override
    public MapperResult removeConfigHistory(MapperContext context) {
        String limitTopHisConfigSql = databaseDialect.getLimitTopSqlWithMark("SELECT id FROM his_config_info WHERE gmt_modified < ? ");
        String sql = "WITH temp_table as (" + limitTopHisConfigSql + " ) "
                + "DELETE FROM his_config_info WHERE id in (SELECT id FROM temp_table) ";
        return new MapperResult(sql, CollectionUtils.list(context.getWhereParameter(FieldConstant.START_TIME),
                context.getWhereParameter(FieldConstant.LIMIT_SIZE)));
    }

    @Override
    public MapperResult pageFindConfigHistoryFetchRows(MapperContext context) {
        String sql = "SELECT nid,data_id,group_id,tenant_id,app_name,src_ip,src_user,op_type,gmt_create,gmt_modified FROM his_config_info WHERE data_id = ? AND group_id = ? AND tenant_id = ? ORDER BY nid DESC  ";
        sql = databaseDialect.getLimitPageSqlWithOffset(sql, context.getStartRow(), context.getPageSize());
        return new MapperResult(sql, CollectionUtils.list(new Object[]{context.getWhereParameter("dataId"), context.getWhereParameter("groupId"), context.getWhereParameter("tenantId")}));
    }

    public BaseHistoryConfigInfoMapper() {
        databaseDialect = DatabaseDialectManager.getInstance().getDialect(getDataSource());
    }
}