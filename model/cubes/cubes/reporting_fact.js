cube(`ReportingFact`, {
  sql: `
    SELECT
      sf.order_id AS order_id,
      sf.customer_id AS customer_id,
      sf.product_id AS product_id,
      sf.merchant_id AS merchant_id,
      cd.customer_name AS customer_name,
      cd.email AS email,
      cd.phone AS phone,
      pd.product_name AS product_name,
      pod.option_value AS product_option,
      sf.units_sold AS quantity,
      cd.instagram_username AS instagram_username,
      cd.country AS country,
      sh.address AS address,
      sh.geocode AS geocode,
      sh.carrier AS carrier,
      md.merchant_name AS merchant_name,
      sh.tracking_number AS shipment_tracking_number,
      pf.payment_method AS payment_method,
      sh.shipment_status AS order_status,
      pd.store_id AS store_name
    FROM
      sales_fact sf
    JOIN
      customer_dimension cd ON sf.customer_id = cd.customer_id
    JOIN
      product_dimension pd ON sf.product_id = pd.product_id
    LEFT JOIN
      product_options_dimension pod ON sf.product_id = pod.product_id
    JOIN
      merchant_dimension md ON sf.merchant_id = md.merchant_id
    LEFT JOIN
      shipping_fact sh ON sf.order_id = sh.order_id
    LEFT JOIN
      invoice_fact if ON sf.order_id = if.order_id
    LEFT JOIN
      payment_fact pf ON if.invoice_id = pf.invoice_id
  `,

  joins: {
    CustomerDimension: {
      relationship: `belongsTo`,
      sql: `${CUBE}.customer_id = ${CustomerDimension}.customer_id`
    },
    ProductDimension: {
      relationship: `belongsTo`,
      sql: `${CUBE}.product_id = ${ProductDimension}.product_id`
    },
    ProductOptionsDimension: {
      relationship: `belongsTo`,
      sql: `${CUBE}.product_id = ${ProductOptionsDimension}.product_id`
    },
    MerchantDimension: {
      relationship: `belongsTo`,
      sql: `${CUBE}.merchant_id = ${MerchantDimension}.merchant_id`
    },
    ShippingFact: {
      relationship: `belongsTo`,
      sql: `${CUBE}.order_id = ${ShippingFact}.order_id`
    },
    InvoiceFact: {
      relationship: `belongsTo`,
      sql: `${CUBE}.order_id = ${InvoiceFact}.order_id`
    },
    PaymentFact: {
      relationship: `belongsTo`,
      sql: `${CUBE}.invoice_id = ${PaymentFact}.invoice_id`
    }
  },

  measures: {
    count: {
      type: `count`
    },
    totalSales: {
      sql: `sales_amount`,
      type: `sum`
    }
  },

  dimensions: {
    order_id: {
      sql: `order_id`,
      type: `number`,
      primaryKey: true
    },
    customer_id: {
      sql: `customer_id`,
      type: `number`
    },
    product_id: {
      sql: `product_id`,
      type: `number`
    },
    merchant_id: {
      sql: `merchant_id`,
      type: `number`
    },
    customer_name: {
      sql: `customer_name`,
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
    product_name: {
      sql: `product_name`,
      type: `string`
    },
    product_option: {
      sql: `product_option`,
      type: `string`
    },
    quantity: {
      sql: `quantity`,
      type: `number`
    },
    instagram_username: {
      sql: `instagram_username`,
      type: `string`
    },
    country: {
      sql: `country`,
      type: `string`
    },
    address: {
      sql: `address`,
      type: `string`
    },
    geocode: {
      sql: `geocode`,
      type: `string`
    },
    carrier: {
      sql: `carrier`,
      type: `string`
    },
    merchant_name: {
      sql: `merchant_name`,
      type: `string`
    },
    shipment_tracking_number: {
      sql: `shipment_tracking_number`,
      type: `string`
    },
    payment_method: {
      sql: `payment_method`,
      type: `string`
    },
    order_status: {
      sql: `order_status`,
      type: `string`
    },
    store_name: {
      sql: `store_name`,
      type: `string`
    }
  }
});

