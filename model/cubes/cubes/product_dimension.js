cube(`ProductDimension`, {
  sql: `SELECT * FROM ecommerce.product_dimension`,

  measures: {
    count: {
      type: `count`
    }
  },

  dimensions: {
    product_id: {
      sql: `product_id`,
      type: `number`,
      primaryKey: true
    },
    product_name: {
      sql: `product_name`,
      type: `string`
    },
    merchant_id: {
      sql: `merchant_id`,
      type: `number`
    },
    product_category: {
      sql: `product_category`,
      type: `string`
    },
    price: {
      sql: `price`,
      type: `number`
    },
    brand_name: {
      sql: `brand_name`,
      type: `string`
    },
    store_id: {
      sql: `store_id`,
      type: `number`
    }
  }
});
