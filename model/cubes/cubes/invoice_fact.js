cube(`InvoiceFact`, {
  sql: `SELECT * FROM ecommerce.invoice_fact`,

  joins: {
    SalesFact: {
      sql: `${CUBE}.order_id = ${SalesFact}.order_id`,
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
      drillMembers: [invoice_id, invoice_date]
    },
    invoiceAmount: {
      sql: `invoice_amount`,
      type: `sum`
    }
  },

  dimensions: {
    invoice_id: {
      sql: `invoice_id`,
      type: `number`,
      primaryKey: true
    },
    invoice_date: {
      sql: `invoice_date`,
      type: `time`
    },
    payment_status: {
      sql: `payment_status`,
      type: `string`
    }
  }
});
