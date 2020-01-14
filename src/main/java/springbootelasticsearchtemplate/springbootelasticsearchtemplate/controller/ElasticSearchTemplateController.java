package springbootelasticsearchtemplate.springbootelasticsearchtemplate.controller;

import com.google.gson.Gson;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.*;
import springbootelasticsearchtemplate.springbootelasticsearchtemplate.pojo.Car;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import  org.elasticsearch.index.query.Operator;

@RestController
@RequestMapping(value ="/es")
public class ElasticSearchTemplateController {

    private static final String CAR_INDEX_NAME = "carindex";
    private static final String CAR_INDEX_TYPE = "carType";

    @Autowired
    private ElasticsearchTemplate template;

    /***
     * 插入一条数据
     */
    @GetMapping("/saveOne")
    public void  saveOne(){
        Car car  = new Car();
        car.setId(1000L);
        car.setModel("您好");
        car.setBrand("hahahahahhha");
        //可以根据其他数据进行更新
        IndexQuery indexQuery =   new IndexQueryBuilder().withId(car.getId().toString()).withObject(car).build();
        template.index(indexQuery);
    }

    /**
     * 批量生成实体类
     * */
    private List<Car> assembleTestData() {
        List<Car> cars1 = new ArrayList<Car>();
        for (int i = 0; i < 10000; i++) {
            cars1.add(new Car(RandomUtils.nextLong(1, 11111), RandomStringUtils.randomAscii(20),
                    RandomStringUtils.randomAlphabetic(15),
                    BigDecimal.valueOf(78000)));
        }
        return cars1;
    }

    /***
     * 批量插入数据
     */
    @GetMapping(value = "/batchInsert")
    public void batchInsert(){

        int counter = 0;

            //判断index 是否存在
            if (!template.indexExists(CAR_INDEX_NAME)) {
                template.createIndex(CAR_INDEX_NAME);
            }

            Gson gson = new Gson();
            List<IndexQuery> queries = new ArrayList<IndexQuery>();
            List<Car> cars = this.assembleTestData();
            if(cars != null && cars.size()>0){
                for (Car car : cars) {
                    IndexQuery indexQuery = new IndexQuery();
                    indexQuery.setId(car.getId().toString());
                    indexQuery.setSource(gson.toJson(car));
                    indexQuery.setIndexName(CAR_INDEX_NAME);
                    indexQuery.setType(CAR_INDEX_TYPE);
                    queries.add(indexQuery);
                    //分批提交索引
                    if (counter % 500 == 0) {
                        template.bulkIndex(queries);
                        queries.clear();
                        System.out.println("bulkIndex counter : " + counter);
                    }
                    counter++;
                }

            }
            //不足批的索引最后不要忘记提交
            if (queries.size() > 0) {
                template.bulkIndex(queries);
            }
            template.refresh(CAR_INDEX_NAME);
        }
    /**
     * 单字符串模糊查询，默认排序。将从所有字段中查找包含传来的word分词后字符串的数据集
     * （按照默认的排序方式，即匹配相关度排序）
     * 默认请求地址：http://localhost:7090/estempalte/es/singleQueryForList?searchSourch=L&size=20
     */
    @GetMapping("/singleQueryForList")
    public List<Car> singleQueryForList(String searchSourch, @PageableDefault Pageable pageable) {
        SearchQuery query = new NativeSearchQueryBuilder().
                withQuery(QueryBuilders.queryStringQuery(searchSourch)).withPageable(pageable).build();

        List<Car> list = template.queryForList(query, Car.class);
        return list;
    }
    /**
     * 单字符串模糊查询，默认排序。将从所有字段中查找包含传来的word分词后字符串的数据集
     * （按照默认的排序方式，即匹配相关度排序）
     * 默认请求地址：http://localhost:7090/estempalte/es/singleQueryForList?searchSourch=L&size=20
     */
    @GetMapping("/singleQueryForListOrderBy")
    public List<Car> singleQueryForListOrderBy(String searchSourch,
                                               @PageableDefault(sort = "model",direction = Sort.Direction.ASC) Pageable pageable) {
        SearchQuery query = new NativeSearchQueryBuilder().
                withQuery(QueryBuilders.queryStringQuery(searchSourch)).withPageable(pageable).build();

        List<Car> list = template.queryForList(query, Car.class);
        return list;
    }

    /**
     * 某字段按字符串完全查询
     * 个字段中模糊包含目标字符串，使用matchQuery
     * 单字段对某字符串模糊查询
     */
    @RequestMapping("/singleMatch")
    public Object singleMatch(String content, @PageableDefault Pageable pageable) {
        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(QueryBuilders.matchQuery("model", content)).withPageable(pageable).build();
        return   template.queryForList(searchQuery, Car.class);
    }

    /**
     *
     * 和match查询类似，match_phrase查询首先解析查询字符串来产生一个词条列表。然后会搜索所有的词条，但只保留包含了所有搜索词条的文档，并且词条的位置要邻接。一个针对短语“中华共和国”的查询不会匹配“中华人民共和国”，因为没有含有邻接在一起的“中华”和“共和国”词条。
     这种完全匹配比较严格，类似于数据库里的“%落日熔金%”这种，使用场景比较狭窄。如果我们希望能不那么严格，譬如搜索“中华共和国”，希望带“我爱中华人民共和国”的也能出来，就是分词后，中间能间隔几个位置的也能查出来，可以使用slop参数。
     * PhraseMatch查询，短语匹配
     * 单字段对某短语进行匹配查询，短语分词的顺序会影响结果
     */
    @RequestMapping("/singlePhraseMatch")
    public Object singlePhraseMatch(String content, @PageableDefault Pageable pageable) {
       // SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(QueryBuilders.matchPhraseQuery("model", content)).withPageable(pageable).build();
       //尽管在使用了slop的短语匹配中，所有的单词都需要出现，但是单词的出现顺序可以不同。如果slop的值足够大，那么单词的顺序可以是任意的。
        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(QueryBuilders.matchPhraseQuery("model", content).slop(2)).withPageable(pageable).build();
        return template.queryForList(searchQuery, Car.class);
    }
    /**
     * Term查询
     *term匹配，即不分词匹配，你传来什么值就会拿你传的值去做完全匹配
     */

    @RequestMapping("/singleTerm")
    public List<Car> singleTerm( String  model,@PageableDefault Pageable pageable){
        System.out.println("model===="+model);
        SearchQuery searchQuery = new NativeSearchQueryBuilder().
                withQuery(QueryBuilders.termQuery("amount",model)).withPageable(pageable).build();
        return  template.queryForList(searchQuery,Car.class);
    }

    /**
     * 多字段匹配
     * 多字段中完全匹配
     */
    @RequestMapping("/multiMatch")
    public Object singleUserId(String title, @PageableDefault(direction = Sort.Direction.DESC) Pageable pageable) {
        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(QueryBuilders.multiMatchQuery(title, "model", "brand")).withPageable(pageable).build();
        return template.queryForList(searchQuery, Car.class);
    }

    /**
     * 单字段包含所有输入
     * 无论是matchQuery，multiMatchQuery，queryStringQuery等，都可以设置operator。默认为Or，设置为And后，就会把符合包含所有输入的才查出来。
     如果是and的话，譬如用户输入了5个词，但包含了4个，也是显示不出来的。我们可以通过设置精度来控制。
     */
    @RequestMapping("/contain")
    public Object contain(String title) {
        SearchQuery searchQuery = new NativeSearchQueryBuilder().
                withQuery(
                        QueryBuilders.matchQuery("model", title).
                        operator(MatchQueryBuilder.DEFAULT_OPERATOR.AND)).build();
        return template.queryForList(searchQuery, Car.class);
    }
    /**
     *
     * 合并查询
     * */
    @GetMapping("/bool")
    public List<Car> bool(String model,BigDecimal amount){
        SearchQuery searchQuery = new NativeSearchQueryBuilder().
                withQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("model",model)).
                        should(QueryBuilders.rangeQuery("amount").lt(amount))).build();
        return template.queryForList(searchQuery, Car.class);

    }





}
