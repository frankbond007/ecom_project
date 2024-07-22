cube(`CustomerDimension`, {
  sql: `SELECT * FROM ecommerce.customer_dimension`,

  measures: {
    count: {
      type: `count`
    }
  },

  dimensions: {
    customer_id: {
      sql: `customer_id`,
      type: `number`,
      primaryKey: true
    },
    customer_name: {
      sql: `customer_name`,
      type: `string`
    },
    gender: {
      sql: `gender`,
      type: `string`
    },
    email: {
      sql: `email`,
      type: `string`
    },
    phone: {
      sql: `phone`,
      type: `string`
    },
    instagram_username: {
      sql: `instagram_username`,
      type: `string`
    },
    city: {
      sql: `city`,
      type: `string`
    },
    state: {
      sql: `state`,
      type: `string`
    },
    country: {
      sql: `country`,
      type: `string`
    }
  }
});
