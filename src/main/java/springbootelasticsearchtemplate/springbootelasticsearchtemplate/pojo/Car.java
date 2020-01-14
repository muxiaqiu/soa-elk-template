package springbootelasticsearchtemplate.springbootelasticsearchtemplate.pojo;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;

import java.io.Serializable;
import java.math.BigDecimal;
@Document(indexName = "carindex", type = "carType", shards = 1, replicas = 0)
public class Car implements Serializable {
    /**
     * serialVersionUID:
     * @since JDK 1.6
     */
    private static final long serialVersionUID = 1L;
    @Id
    private Long id;
    @Field
    private String brand;
    @Field
    private String model;
    @Field
    private BigDecimal amount;

    public Car(){

    }


    public Car(Long id, String brand, String model, BigDecimal amount) {
        this.id = id;
        this.brand = brand;
        this.model = model;
        this.amount = amount;
    }


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }
}