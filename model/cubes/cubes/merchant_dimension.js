cube(`MerchantDimension`, {
  sql: `SELECT * FROM ecommerce.merchant_dimension`,

  measures: {
    count: {
      type: `count`
    }
  },

  dimensions: {
    merchant_id: {
      sql: `merchant_id`,
      type: `number`,
      primaryKey: true
    },
    merchant_name: {
      sql: `merchant_name`,
      type: `string`
    },
    merchant_city: {
      sql: `merchant_city`,
      type: `string`
    },
    merchant_state: {
      sql: `merchant_state`,
      type: `string`
    },
    merchant_country: {
      sql: `merchant_country`,
      type: `string`
    }
  }
});
