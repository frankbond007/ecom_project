cube(`DateDimension`, {
  sql: `SELECT * FROM ecommerce.date_dimension`,

  measures: {
    count: {
      type: `count`
    }
  },

  dimensions: {
    date: {
      sql: `date`,
      type: `time`,
      primaryKey: true
    },
    year: {
      sql: `year`,
      type: `number`
    },
    month: {
      sql: `month`,
      type: `number`
    },
    day: {
      sql: `day`,
      type: `number`
    },
    dayOfWeek: {
      sql: `day_of_week`,
      type: `number`
    },
    monthName: {
      sql: `month_name`,
      type: `string`
    },
    quarter: {
      sql: `quarter`,
      type: `number`
    },
    weekOfYear: {
      sql: `week_of_year`,
      type: `number`
    },
    dayOfYear: {
      sql: `day_of_year`,
      type: `number`
    },
    isWeekend: {
      sql: `is_weekend`,
      type: `string`
    },
    isHoliday: {
      sql: `is_holiday`,
      type: `string`
    },
    holidayName: {
      sql: `holiday_name`,
      type: `string`
    },
    isSpecialEvent: {
      sql: `is_special_event`,
      type: `string`
    },
    specialEventName: {
      sql: `special_event_name`,
      type: `string`
    },
    fiscalYear: {
      sql: `fiscal_year`,
      type: `number`
    },
    fiscalQuarter: {
      sql: `fiscal_quarter`,
      type: `number`
    },
    fiscalMonth: {
      sql: `fiscal_month`,
      type: `number`
    }
  }
});
