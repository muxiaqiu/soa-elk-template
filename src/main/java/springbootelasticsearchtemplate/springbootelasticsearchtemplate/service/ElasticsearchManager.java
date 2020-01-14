package springbootelasticsearchtemplate.springbootelasticsearchtemplate.service;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;

import java.io.IOException;

/**
 * Created by Nasir on 12-09-2015.
 */
public class ElasticsearchManager {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchManager.class);

    public synchronized static void createIndexAndMapping(ElasticsearchTemplate elasticsearchTemplate, String index, String type) {
        //创建mapping
        if (!elasticsearchTemplate.indexExists(index)) {
            elasticsearchTemplate.getClient().admin().indices().prepareCreate(index).execute().actionGet();
        }

        if (elasticsearchTemplate.typeExists(index, type)) {
            return;
        }

        if (index.startsWith("idx_log_http_")) {
            createIndexAndMappingForHttp(elasticsearchTemplate, index, type);
        } else if (index.startsWith("idx_log_dubbo_access_")) {
            createIndexAndMappingForDubboAccess(elasticsearchTemplate, index, type);
        } else if (index.startsWith("idx_log_order_")) {
            createIndexAndMappingForOrder(elasticsearchTemplate, index, type);
        } else if (index.startsWith("idx_log_rcmsg_")) {
            createIndexAndMappingForRCMsg(elasticsearchTemplate, index, type);
        } else if (index.startsWith("idx_log_tctj_")) {
            createIndexAndMappingForTongChengTuiJian(elasticsearchTemplate, index, type);
        } else if (index.startsWith("idx_log_yd_")) {
            createIndexAndMappingForYunDou(elasticsearchTemplate, index, type);
        } else if (index.startsWith("idx_log_nginx_")) {
            createIndexAndMappingForNginx(elasticsearchTemplate, index, type);
        } else if (index.startsWith("idx_log_sql_")) {
            createIndexAndMappingForSQL(elasticsearchTemplate, index, type);
        } else if (index.startsWith("idx_log_vc_")) {
            createIndexAndMappingForVoiceCaptcha(elasticsearchTemplate, index, type);
        } else if (index.startsWith("idx_log_ubc_")) {
            createIndexAndMappingForUBC(elasticsearchTemplate, index, type);
        } else {
            createIndexAndMappingForDefault(elasticsearchTemplate, index, type);
        }
    }

    private static XContentBuilder createMappingBuilder(String type) throws IOException {
        return XContentFactory.jsonBuilder()
                .startObject()
                .startObject(type)
//                .startObject("_source").field("enabled", false).endObject()
                .startObject("properties")
                .startObject("@thread").field("type", "text").field("index", "true").endObject()
                .startObject("@es_time").field("type", "text").field("index", "true").endObject()
                .startObject("@processor").field("type", "text").field("index", "true").endObject()
                .startObject("@leader").field("type", "text").field("index", "true").endObject()
                .startObject("@host").field("type", "text").field("index", "true").endObject()
                .startObject("@host_time").field("type", "text").field("index", "true").endObject()
                .startObject("@appname").field("type", "text").field("index", "true").endObject()
                .startObject("@detail").field("type", "text").field("index", "true").endObject()
                .startObject("@port").field("type", "text").field("index", "true").endObject()
                //alias
                .startObject("@alias").field("type", "text").field("index", "true").endObject()
                //geo
                .startObject("coordinate").field("type", "geo_point").endObject()
                .startObject("country").field("type", "text").field("index", "true").endObject()
                .startObject("city").field("type", "text").field("index", "true").endObject()
                .startObject("code").field("type", "text").field("index", "true").endObject()
                .startObject("continent").field("type", "text").field("index", "true").endObject()
                .startObject("iso_code").field("type", "text").field("index", "true").endObject()
                .startObject("geo_name_id").field("type", "text").field("index", "true").endObject()
                .startObject("ip").field("type", "ip").endObject()
                .startObject("status").field("type", "long").endObject();
    }

    private static boolean createIndexAndMappingForDefault(ElasticsearchTemplate elasticsearchTemplate, String index, String type) {
        XContentBuilder mapping = null;
        try {
            mapping = createMappingBuilder(type)
                    .endObject()
                    .endObject()
                    .endObject();

            return putMapping(elasticsearchTemplate, index, type, mapping);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return false;
    }

    private static boolean createIndexAndMappingForHttp(ElasticsearchTemplate elasticsearchTemplate, String index, String type) {
        XContentBuilder mapping = null;
        try {
            mapping = createMappingBuilder(type)
//                    .startObject("ip").field("type", "ip").field("store", "yes").endObject()
                    .startObject("method").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("access_type").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("params").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("local_name").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("local_addr").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("context_path").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .endObject()
                    .endObject()
                    .endObject();

            return putMapping(elasticsearchTemplate, index, type, mapping);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return false;
    }

    private static boolean createIndexAndMappingForDubboAccess(ElasticsearchTemplate elasticsearchTemplate, String index, String type) {
        XContentBuilder mapping = null;
        try {
            mapping = createMappingBuilder(type)
                    .startObject("method").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("access_type").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("params").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("group").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("version").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("dubbo_remote").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()

//                    .startObject("exception")
//                    .startObject("detailMessage").field("type", "string").field("index", "not_analyzed").endObject()
//                    .startObject("stackTrace")
//                    .startObject("fileName").field("type", "string").field("index", "not_analyzed").endObject()
//                    .startObject("methodName").field("type", "string").field("index", "not_analyzed").endObject()
//                    .endObject()
//                    .endObject()

                    .endObject()
                    .endObject()
                    .endObject();

            return putMapping(elasticsearchTemplate, index, type, mapping);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return false;
    }

    private static boolean createIndexAndMappingForOrder(ElasticsearchTemplate elasticsearchTemplate, String index, String type) {
        XContentBuilder mapping = null;
        try {
            mapping = createMappingBuilder(type)
                    .startObject("id").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("channel_id").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
//                    .startObject("status").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()

                    .startObject("data_type").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("fund_manager").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()

                    .startObject("adviser_id").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("adviser_name").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()

                    .startObject("bank_accounat").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("bank_account_name").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("bank_name").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()

                    .startObject("product_id").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("product_label").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("product_name").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("product_name_short").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()

                    .startObject("scheme_id").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("scheme_name").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("scheme_unit_id").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()

                    .startObject("unit_id").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("unit_name").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("yield_type").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()

                    .startObject("credentials_name").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("credentials_number").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()

                    .startObject("cust_id").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("cust_name").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()

                    .startObject("current_org_id").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("current_org_name").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()

                    .endObject()
                    .endObject()
                    .endObject();

            return putMapping(elasticsearchTemplate, index, type, mapping);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return false;
    }

    private static boolean createIndexAndMappingForRCMsg(ElasticsearchTemplate elasticsearchTemplate, String index, String type) {
        XContentBuilder mapping = null;
        try {
            mapping = createMappingBuilder(type)
                    .startObject("push_data").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("response_body").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("push_content").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("from_user_id").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("data_type").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("to_user_id").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()

                    .endObject()
                    .endObject()
                    .endObject();

            return putMapping(elasticsearchTemplate, index, type, mapping);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return false;
    }

    private static boolean createIndexAndMappingForTongChengTuiJian(ElasticsearchTemplate elasticsearchTemplate, String index, String type) {
        XContentBuilder mapping = null;
        try {
            mapping = createMappingBuilder(type)
                    .startObject("adviser_id").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("adviser_name").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("org_id").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("org_name").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("cust_city").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("adv_city").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()

                    .endObject()
                    .endObject()
                    .endObject();

            return putMapping(elasticsearchTemplate, index, type, mapping);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return false;
    }

    private static boolean createIndexAndMappingForYunDou(ElasticsearchTemplate elasticsearchTemplate, String index, String type) {
        XContentBuilder mapping = null;
        try {
            mapping = createMappingBuilder(type)
                    .startObject("adviser_id").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("adviser_name").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("org_id").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("org_name").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("point_type").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()

                    .endObject()
                    .endObject()
                    .endObject();

            return putMapping(elasticsearchTemplate, index, type, mapping);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return false;
    }

    private static boolean createIndexAndMappingForNginx(ElasticsearchTemplate elasticsearchTemplate, String index, String type) {
        XContentBuilder mapping = null;
        try {
            mapping = createMappingBuilder(type)
                    .startObject("request").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("request_body").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("referer").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("agent").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("server_name").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("bytes").field("type", "long").field("store", "yes").endObject()
                    .startObject("request_time").field("type", "double").field("store", "yes").endObject()
                    .startObject("upstr_resp_time").field("type", "double").field("store", "yes").endObject()
                    .endObject()
                    .endObject()
                    .endObject();

            return putMapping(elasticsearchTemplate, index, type, mapping);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return false;
    }

    private static boolean createIndexAndMappingForSQL(ElasticsearchTemplate elasticsearchTemplate, String index, String type) {
        XContentBuilder mapping = null;
        try {
            mapping = createMappingBuilder(type)
                    .startObject("method").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("real").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("sql").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("result").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()

                    .endObject()
                    .endObject()
                    .endObject();

            return putMapping(elasticsearchTemplate, index, type, mapping);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return false;
    }

    private static boolean createIndexAndMappingForVoiceCaptcha(ElasticsearchTemplate elasticsearchTemplate, String index, String type) {
        XContentBuilder mapping = null;
        try {
            mapping = createMappingBuilder(type)
                    .startObject("captcha_code").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("data_type").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("to").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("resp_code").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()

                    .endObject()
                    .endObject()
                    .endObject();

            return putMapping(elasticsearchTemplate, index, type, mapping);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return false;
    }

    private static boolean createIndexAndMappingForUBC(ElasticsearchTemplate elasticsearchTemplate, String index, String type) {
        XContentBuilder mapping = null;
        try {
            mapping = createMappingBuilder(type)
                    .startObject("uid").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("grp").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("act").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("m").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("mid").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("mos").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("mv").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("v").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("vtp").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("arg1").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("arg2").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("arg3").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("arg4").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("arg5").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("arg6").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("arg7").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .startObject("arg8").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()

                    .endObject()
                    .endObject()
                    .endObject();

            return putMapping(elasticsearchTemplate, index, type, mapping);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return false;
    }

    private static boolean putMapping(ElasticsearchTemplate elasticsearchTemplate, String index, String type, XContentBuilder mapping) {
        PutMappingRequest request = Requests.putMappingRequest(index).type(type).source(mapping);
        PutMappingResponse putMappingResponse = elasticsearchTemplate.getClient().admin().indices().putMapping(request).actionGet();
        return putMappingResponse.isAcknowledged();
    }
}
