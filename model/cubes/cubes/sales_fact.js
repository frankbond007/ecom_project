cube(`SalesFact`, {
  sql: `SELECT * FROM ecommerce.sales_fact`,

  joins: {
    CustomerDimension: {
      sql: `${CUBE}.customer_id = ${CustomerDimension}.customer_id`,
      relationship: `belongsTo`
    },
    ProductDimension: {
      sql: `${CUBE}.product_id = ${ProductDimension}.product_id`,
      relationship: `belongsTo`
    },
    MerchantDimension: {
      sql: `${CUBE}.merchant_id = ${MerchantDimension}.merchant_id`,
      relationship: `belongsTo`
    },
    DateDimension: {
      sql: `${CUBE}.day_id = ${DateDimension}.day_id`,
      relationship: `belongsTo`
    }
  },

  measures: {
    count: {
      type: `count`,
      drillMembers: [order_id, purchase_date]
    },
    salesAmount: {
      sql: `sales_amount`,
      type: `sum`
    },
    discountAmount: {
      sql: `discount_amount`,
      type: `sum`
    },
    profit: {
      sql: `profit`,
      type: `sum`
    },
    unitsSold: {
      sql: `units_sold`,
      type: `sum`
    },
    orderQuantity: {
      sql: `order_quantity`,
      type: `sum`
    }
  },

  dimensions: {
    order_id: {
      sql: `order_id`,
      type: `number`,
      primaryKey: true
    },
    purchase_date: {
      sql: `purchase_date`,
      type: `time`
    },
    option_ids: {
      sql: `option_ids`,
      type: `string`
    }
  }
});
