CREATE TABLE order_items (
    order_id NUMBER(10) NOT NULL,
    product_id NUMBER(10) NOT NULL,
    quantity NUMBER(10) DEFAULT 1 NOT NULL,
    unit_price NUMBER(10,2) DEFAULT 0.00 NOT NULL,
    PRIMARY KEY (order_id, product_id)
)
