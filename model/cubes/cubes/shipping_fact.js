cube(`ShippingFact`, {
  sql: `SELECT * FROM ecommerce.shipping_fact`,

  joins: {
    SalesFact: {
      sql: `${CUBE}.order_id = ${SalesFact}.order_id`,
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
      drillMembers: [shipping_id, shipping_date, delivery_date]
    }
  },

  dimensions: {
    shipping_id: {
      sql: `shipping_id`,
      type: `number`,
      primaryKey: true
    },
    shipping_date: {
      sql: `shipping_date`,
      type: `time`
    },
    carrier: {
      sql: `carrier`,
      type: `string`
    },
    tracking_number: {
      sql: `tracking_number`,
      type: `string`
    },
    geocode: {
      sql: `geocode`,
      type: `string`
    },
    address: {
      sql: `address`,
      type: `string`
    },
    shipment_status: {
      sql: `shipment_status`,
      type: `string`
    },
    max_expected_delivery_date: {
      sql: `max_expected_delivery_date`,
      type: `time`
    },
    delivery_date: {
      sql: `delivery_date`,
      type: `time`
    }
  }
});
