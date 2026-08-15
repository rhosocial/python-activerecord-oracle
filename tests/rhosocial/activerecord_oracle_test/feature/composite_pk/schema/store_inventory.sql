CREATE TABLE store_inventory (
    store_id NUMBER(10) NOT NULL,
    product_id NUMBER(10) NOT NULL,
    batch_id VARCHAR2(100) NOT NULL,
    stock NUMBER(10) DEFAULT 0 NOT NULL,
    PRIMARY KEY (store_id, product_id, batch_id)
)
