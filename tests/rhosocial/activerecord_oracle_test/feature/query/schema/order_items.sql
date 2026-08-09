CREATE TABLE order_items (
    order_id NUMBER NOT NULL,
    product_id NUMBER NOT NULL,
    quantity NUMBER DEFAULT 1 NOT NULL,
    unit_price NUMBER(10, 2) NOT NULL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    PRIMARY KEY (order_id, product_id)
)
