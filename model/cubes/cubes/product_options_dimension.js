cube(`ProductOptionsDimension`, {
  sql: `SELECT * FROM ecommerce.product_options_dimension`,

  joins: {
    ProductDimension: {
      sql: `${CUBE}.product_id = ${ProductDimension}.product_id`,
      relationship: `belongsTo`
    }
  },

  measures: {
    count: {
      type: `count`
    }
  },

  dimensions: {
    product_option_id: {
      sql: `product_option_id`,
      type: `number`,
      primaryKey: true
    },
    option_type: {
      sql: `option_type`,
      type: `string`
    },
    option_value: {
      sql: `option_value`,
      type: `string`
    },
    additional_cost: {
      sql: `additional_cost`,
      type: `number`
    }
  }
});
