package com.codingserbia.dto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.codingserbia.writable.ProductWritable;

public class CustomerCategoryProductBag {

    public LongWritable customerCategoryId;

    public Text customerCategoryDescription;

    private Map<LongWritable, ProductWritable> products;

    private Map<LongWritable, Long> purchasesByProduct;

    private Map<LongWritable, Long> viewsByProduct;

    private int numberOfViews = 0;

    private int numberOfSessions = 0;

    private int numberOfPurchases = 0;

    public CustomerCategoryProductBag() {
        customerCategoryId = new LongWritable(0L);
        customerCategoryDescription = new Text();
        products = new HashMap<LongWritable, ProductWritable>();
        purchasesByProduct = new HashMap<LongWritable, Long>();
        viewsByProduct = new HashMap<LongWritable, Long>();
    }

    public ProductWritable getProductWritable(LongWritable id) {
        return products.get(id);
    }

    public boolean contains(LongWritable productId) {
        return getProductWritable(productId) != null;
    }

    public void add(ProductWritable product) {
        products.put(product.id, product);
        viewsByProduct.put(product.id, 1L);
        numberOfViews++;

        if (product.bought.get()) {
            purchasesByProduct.put(product.id, 1L);
            numberOfPurchases++;
        }
    }

    public void processOccurance(ProductWritable product) {
        if (product.bought.get()) {
            Long productNumberOfPurchases = purchasesByProduct.get(product.id);
            if (productNumberOfPurchases == null) {
                productNumberOfPurchases = 1L;
            } else {
                productNumberOfPurchases++;
            }
            purchasesByProduct.put(product.id, productNumberOfPurchases);
            numberOfPurchases++;
        }

        Long productNumberOfViews = viewsByProduct.get(product.id);
        productNumberOfViews++;
        viewsByProduct.put(product.id, productNumberOfViews);

        numberOfViews++;
    }

    public List<ProductWritable> getTopProductsBought(int numberOfProducts) {
        List<ProductWritable> topProducts = new ArrayList<ProductWritable>();

        Set<Entry<LongWritable, Long>> entrySet = purchasesByProduct.entrySet();

        List<Entry<LongWritable, Long>> entries = new ArrayList<Entry<LongWritable, Long>>();
        for (Iterator<Entry<LongWritable, Long>> iterator = entrySet.iterator(); iterator.hasNext();) {
            entries.add(iterator.next());
        }
        Collections.sort(entries, new Comparator<Entry<LongWritable, Long>>() {
            @Override
            public int compare(Entry<LongWritable, Long> entry1, Entry<LongWritable, Long> entry2) {
                return entry2.getValue().intValue() - entry1.getValue().intValue();
            }
        });

        int resultSize = numberOfProducts;
        if (resultSize > entries.size()) {
            resultSize = entries.size();
        }

        for (Entry<LongWritable, Long> e : entries.subList(0, resultSize)) {
            topProducts.add(products.get(e.getKey()));
        }

        return topProducts;
    }

    public void increaseNumberOfSessions() {
        numberOfSessions++;
    }

    public float calculateAverageNumberOfViews() {
        if (numberOfSessions == 0) {
            return 0f;
        }

        return (float) numberOfViews / (float) numberOfSessions;
    }

    public float calculateAverageNumberOfPurchases() {
        if (numberOfSessions == 0) {
            return 0f;
        }

        return (float) numberOfPurchases / (float) numberOfSessions;
    }

    public double calculateAveragePurchase() {
        if (numberOfPurchases == 0) {
            return .0;
        }

        double amountInTotal = .0;

        for (Iterator<LongWritable> iterator = purchasesByProduct.keySet().iterator(); iterator.hasNext();) {
            amountInTotal += products.get(iterator.next()).price.get();
        }

        return amountInTotal / numberOfPurchases;
    }

}
