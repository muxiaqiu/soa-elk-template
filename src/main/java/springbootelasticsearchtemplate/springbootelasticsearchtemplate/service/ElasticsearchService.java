package springbootelasticsearchtemplate.springbootelasticsearchtemplate.service;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.stereotype.Service;
import springbootelasticsearchtemplate.springbootelasticsearchtemplate.util.Constants;
import springbootelasticsearchtemplate.springbootelasticsearchtemplate.util.StackTraceUtil;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by Nasir on 12-09-2015.
 */
@Service
public class ElasticsearchService {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchService.class);

    @Autowired
    private ElasticsearchTemplate logElasticsearchTemplate;

    @Autowired
    private ElasticsearchTemplate allElasticsearchTemplate;

    private ElasticsearchTemplate getESTemplate(String esTemplateType) {
        if (esTemplateType.equals(Constants.ES_ALL)) {
            return allElasticsearchTemplate;
        } else {
            return logElasticsearchTemplate;
        }
    }

    private ElasticsearchTemplate getESTemplateByIndex(String index) {
        if (index.startsWith("idx_log_http_")) {
            return logElasticsearchTemplate;
        } else if (index.startsWith("idx_log_dubbo_access_")) {
            return allElasticsearchTemplate;
        } else if (index.startsWith("idx_log_order_")) {
            return logElasticsearchTemplate;
        } else if (index.startsWith("idx_log_rcmsg_")) {
            return allElasticsearchTemplate;
        } else if (index.startsWith("idx_log_tctj_")) {
            return logElasticsearchTemplate;
        } else if (index.startsWith("idx_log_yd_")) {
            return logElasticsearchTemplate;
        } else if (index.startsWith("idx_log_nginx_")) {
            return allElasticsearchTemplate;
        }
        return allElasticsearchTemplate;
    }

    public boolean indicesExist(String index) {
        IndicesExistsRequest request = new IndicesExistsRequest(index);
        IndicesExistsResponse response = getESTemplateByIndex(index).getClient().admin().indices()
                .exists(request).actionGet();
        return response.isExists();
    }

    public boolean indicesAliases(String esTemplateType) {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        IndicesAliasesResponse response = getESTemplate(esTemplateType).getClient().admin().indices().aliases(request).actionGet();
        return response.isAcknowledged();
    }

    public String[] GetIndex(String esTemplateType) {
        GetIndexRequest request = new GetIndexRequest();
        GetIndexResponse response = getESTemplate(esTemplateType).getClient().admin().indices().getIndex(request).actionGet();
        return response.getIndices();
    }

    public void insert(String index, String type, Map source) {
        getESTemplateByIndex(index).getClient().prepareIndex(index, type).setSource(source).execute().actionGet();
    }

    public void insert(String index, String type, List<Map> sources) {
        BulkRequestBuilder bulkRequest = getESTemplateByIndex(index).getClient().prepareBulk();
        int count = sources.size();
        for (int i = 0; i < count; i++) {
            bulkRequest.add(getESTemplateByIndex(index).getClient().prepareIndex(index, type).setSource(sources.get(i)));
        }
        bulkRequest.execute().actionGet();
    }

    public void insertAsync(String index, String type, Map source) {
        getESTemplateByIndex(index).getClient().prepareIndex(index, type).setSource(source).execute();
    }

    public void insertBulkAsync(String index, String type, List<Map> sources) {
        try {
            ElasticsearchTemplate elasticsearchTemplate = getESTemplateByIndex(index);
            ElasticsearchManager.createIndexAndMapping(elasticsearchTemplate, index, type);

            BulkRequestBuilder bulkRequest = elasticsearchTemplate.getClient().prepareBulk();
            int count = sources.size();
            for (int i = 0; i < count; i++) {
                bulkRequest.add(elasticsearchTemplate.getClient().prepareIndex(index, type).setSource(sources.get(i)));
            }

            BulkResponse response = bulkRequest.execute().actionGet();
            if (response.hasFailures()) {
                logger.error(response.buildFailureMessage() + "\n" + "Data: " + sources.toString());
            }
        } catch (Exception ex) {
            logger.error(StackTraceUtil.getStackTraceEx(ex));
        }
    }

    public void putMapping(String index, String type) {
        try {
            XContentBuilder mapping = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject(index)
                    .startObject("properties")
                    .startObject("title").field("type", "string").field("store", "yes").endObject()
                    .startObject("description").field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("price").field("type", "double").endObject()
                    .startObject("onSale").field("type", "boolean").endObject()
                    .startObject("type").field("type", "integer").endObject()
                    .startObject("createDate").field("type", "date").endObject()
                    .endObject()
                    .endObject()
                    .endObject();

            getESTemplateByIndex(index).putMapping(index, type, mapping);
        } catch (IOException ex) {
            logger.error(StackTraceUtil.getStackTraceEx(ex));
        }
    }

    private static XContentBuilder getMapping() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("test")
                .startObject("properties")
                .startObject("id").field("type", "long").field("store", "yes").endObject()
                .startObject("type").field("type", "string").field("index", "not_analyzed").endObject()
                .startObject("catIds").field("type", "integer").endObject()
                .endObject()
                .endObject()
                .endObject();
        return mapping;
    }

    public void insertBulk(String index, String type, List<Map> sources) {
        ElasticsearchTemplate elasticsearchTemplate = getESTemplateByIndex(index);
        ElasticsearchManager.createIndexAndMapping(elasticsearchTemplate, index, type);

        BulkRequestBuilder bulkRequest = elasticsearchTemplate.getClient().prepareBulk();
        int count = sources.size();
        for (int i = 0; i < count; i++) {
            bulkRequest.add(elasticsearchTemplate.getClient().prepareIndex(index, type).setSource(sources.get(i)));
        }
        bulkRequest.execute().actionGet();
    }


    // 创建index
    public void createIndice(String index, String type) throws IOException {
        // 创建index请求构造
        CreateIndexRequestBuilder createIndexRequestBuilder = getESTemplateByIndex(index).getClient().admin().indices().prepareCreate(index);
        // 设置该index的Mapping，可暂时不设置，后面建完index之后再设置也可
        createIndexRequestBuilder.addMapping(type, createMapping(type));
        // 设置该index的Settings配置，常用的有shard数量、副本数
        createIndexRequestBuilder.setSettings(createSetting());
        // 执行创建index请求
        createIndexRequestBuilder.execute().actionGet();
    }

    // 设置index的Settings
    public Settings createSetting() {
        Settings settings = Settings.builder().put("number_of_shards", 5).put("number_of_replicas", 0).build();
        return settings;
    }

    // 设置index的Mappings，具体内容在后面解释
    public XContentBuilder createMapping(String type) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .startObject(type)
                .startObject("properties")
                .startObject("name").field("type", "string").field("index", "not_analyzed").endObject()
                .startObject("age").field("type", "integer").field("index", "not_analyzed").endObject()
                .startObject("birthday").field("type", "date").field("index", "not_analyzed").endObject()
                .endObject()
                .endObject()
                .endObject();
        return builder;
    }
}
