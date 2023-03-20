package org.embulk.output.opensearch;

import java.util.List;

public class IndexDataJson
{
    private long id;
    private Long account;
    private String time;
    private String purchase;
    private boolean flg;
    private double score;
    private String comment;
    private Product product;
    private List<Product> products;

    public IndexDataJson(long id, Long account, String time, String purchase, boolean flg, double score, String comment, Product product, List<Product> products)
    {
        this.id = id;
        this.account = account;
        this.time = time;
        this.purchase = purchase;
        this.flg = flg;
        this.score = score;
        this.comment = comment;
        this.product = product;
        this.products = products;
    }

    public IndexDataJson()
    {
    }

    public long getId()
    {
        return id;
    }

    public void setId(long id)
    {
        this.id = id;
    }

    public Long getAccount()
    {
        return account;
    }

    public void setAccount(Long account)
    {
        this.account = account;
    }

    public String getTime()
    {
        return time;
    }

    public void setTime(String time)
    {
        this.time = time;
    }

    public String getPurchase()
    {
        return purchase;
    }

    public void setPurchase(String purchase)
    {
        this.purchase = purchase;
    }

    public boolean getFlg()
    {
        return flg;
    }

    public void setFlg(boolean flg)
    {
        this.flg = flg;
    }

    public double getScore()
    {
        return score;
    }

    public void setScore(double score)
    {
        this.score = score;
    }

    public String getComment()
    {
        return comment;
    }

    public void setComment(String comment)
    {
        this.comment = comment;
    }

    public Product getProduct()
    {
        return product;
    }

    public void setProduct(Product product)
    {
        this.product = product;
    }

    public List<Product> getProducts()
    {
        return products;
    }

    public void setProducts(List<Product> products)
    {
        this.products = products;
    }
}
