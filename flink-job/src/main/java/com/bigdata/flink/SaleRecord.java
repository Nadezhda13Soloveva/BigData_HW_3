package com.bigdata.flink;

import java.sql.Date;

public class SaleRecord {
    // Customer
    public int customerId;
    public String customerFirstName;
    public String customerLastName;
    public int customerAge;
    public String customerEmail;
    public String customerCountry;
    public String customerPostalCode;
    public String customerPetType;
    public String customerPetName;
    public String customerPetBreed;

    // Seller
    public int sellerId;
    public String sellerFirstName;
    public String sellerLastName;
    public String sellerEmail;
    public String sellerCountry;
    public String sellerPostalCode;

    // Product
    public int productId;
    public String productName;
    public String productCategory;
    public double productPrice;
    public int productQuantity;
    public double productWeight;
    public String productColor;
    public String productSize;
    public String productBrand;
    public String productMaterial;
    public String productDescription;
    public double productRating;
    public int productReviews;
    public Date productReleaseDate;
    public Date productExpiryDate;

    // Store
    public String storeName;
    public String storeLocation;
    public String storeCity;
    public String storeState;
    public String storeCountry;
    public String storePhone;
    public String storeEmail;

    // Supplier
    public String supplierName;
    public String supplierContact;
    public String supplierEmail;
    public String supplierPhone;
    public String supplierAddress;
    public String supplierCity;
    public String supplierCountry;

    // Fact
    public long sourceSaleId;
    public Date saleDate;
    public int saleQuantity;
    public double saleTotalPrice;
}