cube(`PaymentFact`, {
  sql: `SELECT * FROM ecommerce.payment_fact`,

  joins: {
    InvoiceFact: {
      sql: `${CUBE}.invoice_id = ${InvoiceFact}.invoice_id`,
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
      drillMembers: [payment_id, payment_date]
    },
    paymentAmount: {
      sql: `payment_amount`,
      type: `sum`
    }
  },

  dimensions: {
    payment_id: {
      sql: `payment_id`,
      type: `number`,
      primaryKey: true
    },
    payment_date: {
      sql: `payment_date`,
      type: `time`
    },
    payment_method: {
      sql: `payment_method`,
      type: `string`
    }
  }
});
