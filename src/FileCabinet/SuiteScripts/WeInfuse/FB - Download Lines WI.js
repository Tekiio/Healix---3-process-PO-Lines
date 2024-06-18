/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/cache', 'N/config', 'N/file', 'N/https', 'N/log', 'N/record', 'N/runtime', 'N/search', './FB - Const Lib.js', './extras/moment.js'], /**
 * @param {cache} cache
 * @param {config} config
 * @param {file} file
 * @param {https} https
 * @param {log} log
 * @param {record} record
 * @param {runtime} runtime
 * @param {search} search
 * @param {constLib} constLib
 * @param {moment} moment
 */ (cache, config, file, https, log, record, runtime, search, constLib, moment) => {
  const { JSON_STRUCTURE, RECORDS, LIST, CONST_CACHE, STATUS, ERRORS } = constLib
  const MAIN_CONFIG = {}
  const ALL_STATUS = []
  const ALL_ERRORS = []
  const TRANSACTION_TYPE = {}
  /**
   * Defines the function that is executed at the beginning of the map/reduce process and generates the input data.
   * @param {Object} inputContext
   * @param {boolean} inputContext.isRestarted - Indicates whether the current invocation of this function is the first
   *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
   * @param {Object} inputContext.ObjectRef - Object that references the input data
   * @typedef {Object} ObjectRef
   * @property {string|number} ObjectRef.id - Internal ID of the record instance that contains the input data
   * @property {string} ObjectRef.type - Type of the record instance that contains the input data
   * @returns {Array|Object|Search|ObjectRef|File|Query} The input data to use in the map/reduce process
   * @since 2015.2
   */

  const getInputData = inputContext => {
    let logRecord = ''
    const data = []
    try {
      getConfig()
      logRecord = createLogProcess()
      writeCache(CONST_CACHE.KEY, logRecord)
      createLogAction(logRecord, 'Init Process')
      createLogAction(logRecord, 'Get Configuration')
      createLogAction(logRecord, 'Get Token')
      log.debug({title: 'MAIN_CONFIG', details: MAIN_CONFIG});
      const token = getLookerToken(logRecord)
      if (token) {
        log.audit('Step:', 'Get Looker')
        const lookerInfo = getLookerInformation(token, logRecord)
        createLogAction(logRecord, 'Get Looker Data')
        const results = getQueryResults(lookerInfo.query_id, token, logRecord)
        results.forEach(result => {
          data.push(result)
        })
      }
      log.audit('Total of results:', data.length)
      createLogAction(logRecord, 'Data found', `Total of results found: ${data.length}`)
      // COMMENT: All data
      return data
      // return ['{"group.id":757,"group.name":"C426 North Texas Center for Rheumatology","locations.id":1628,"locations.name":"C426A West Park","purchase_orders.id":505737,"purchase_orders.order_number":"","purchase_orders.po_number":"P246156","purchase_orders.inventory_adjustment_id":null,"purchase_orders.order_method":"NetSuite","purchase_orders.order_date":"2024-06-07","purchase_orders.date_received_date":null,"purchase_orders.checked_in_user_name":null,"purchase_orders.vendor_id":30,"vendors.name":"Amerisource Bergen","line_items.product_id":243141,"line_items.backordered":"No","formulary_entries.name":"sterile water for inj","ndcs.label_name":"WATER FOR INJECTION VIAL","line_items.ndc_code":"00409488717","billings.outer_ndc_code":"00409488710","line_items.price":0.6,"line_items.expiration_date":null,"line_items.created_date":"2024-06-07","purchase_orders.type":"Purchase Order","line_items.id":8713086,"inventory_items.inventory_item_id":null,"line_items.qty":1,"line_items.created_time":"2024-06-07 22:31:24","line_items.updated_time":"2024-06-07 22:31:24","inventory_items.created_time":null,"inventory_items.updated_time":null,"line_items.lot":null,"inventory_items.qty_received":1}']
    } catch (err) {
      log.error('Error on getInputData', err)
      if (data.length === 0) {
        createLogAction(logRecord, 'Error on consult information', err.message)
        createLogAction(logRecord, 'End Process')
        updateRecord(RECORDS.WI_LOG.ID, logRecord, { [RECORDS.WI_LOG.FIELDS.END]: formatDate(new Date(), true) })
        clearCacheValue(CONST_CACHE.KEY)
      }
    }
  }

  /**
   * Defines the function that is executed when the map entry point is triggered. This entry point is triggered automatically
   * when the associated getInputData stage is complete. This function is applied to each key-value pair in the provided
   * context.
   * @param {Object} mapContext - Data collection containing the key-value pairs to process in the map stage. This parameter
   *     is provided automatically based on the results of the getInputData stage.
   * @param {Iterator} mapContext.errors - Serialized errors that were thrown during previous attempts to execute the map
   *     function on the current key-value pair
   * @param {number} mapContext.executionNo - Number of times the map function has been executed on the current key-value
   *     pair
   * @param {boolean} mapContext.isRestarted - Indicates whether the current invocation of this function is the first
   *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
   * @param {string} mapContext.key - Key to be processed during the map stage
   * @param {string} mapContext.value - Value to be processed during the map stage
   * @since 2015.2
   */

  const map = mapContext => {
    try {
      let existingError = false
      let isBackOrder = false
      const { key } = mapContext
      let { value } = mapContext
      const { WEINFUSE } = RECORDS
      value = JSON.parse(value)
      getConfig()
      getStatusWeInfuse()
      getErrorsWeInfuse()
      getTypeTransactions()
      const logRecord = getCacheValue(CONST_CACHE.KEY)
      if (Number(key) === 0) {
        log.debug('map ~ value:', value)
        createLogAction(logRecord, 'Map data', 'Mapping of data results')
      }
      const lineRecord = {}
      const objAssign = assignData(lineRecord, value, existingError, isBackOrder, logRecord)
      isBackOrder = objAssign.isBackOrder
      // log.debug({title: 'value', details: value});
      // log.debug({title: 'lineRecord', details: lineRecord});
      // log.debug('map ~ isBackOrder:', isBackOrder)
      existingError = objAssign.existingError
      log.debug('map ~ existingError:', existingError)
      if (!lineRecord[WEINFUSE.FIELDS.STATUS]) {
        lineRecord[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.PENDING)
      }
      if (!lineRecord[WEINFUSE.FIELDS.STATUS_STAGE]) {
        lineRecord[WEINFUSE.FIELDS.STATUS_STAGE] = 1
      }

      const filtersWeInfuse = []
      if (isBackOrder === false) {
        filtersWeInfuse.push([WEINFUSE.FIELDS.LINE_ID, search.Operator.IS, lineRecord[WEINFUSE.FIELDS.LINE_ID]])
      } else {
        filtersWeInfuse.push([
          [WEINFUSE.FIELDS.PO_NUMBER, search.Operator.IS, lineRecord[WEINFUSE.FIELDS.PO_NUMBER]],
          'AND',
          [WEINFUSE.FIELDS.GROUP_ID, search.Operator.IS, lineRecord[WEINFUSE.FIELDS.GROUP_ID]],
          'AND',
          [WEINFUSE.FIELDS.LOCATION_ID, search.Operator.IS, lineRecord[WEINFUSE.FIELDS.LOCATION_ID]],
          'AND',
          [WEINFUSE.FIELDS.NDC, search.Operator.IS, lineRecord[WEINFUSE.FIELDS.NDC]],
          'AND',
          [WEINFUSE.FIELDS.DATE, search.Operator.ON, formatDate(lineRecord[WEINFUSE.FIELDS.DATE], false, false)],
          'AND',
          [WEINFUSE.FIELDS.LOT, search.Operator.ISEMPTY, ''],
          'AND',
          [WEINFUSE.FIELDS.BACKORDER, search.Operator.IS, 'T'],
          'AND',
          [WEINFUSE.FIELDS.STATUS_STAGE, search.Operator.IS, 1]
        ])
      }
      const wiRecord = searchWeInfuseRecord(filtersWeInfuse)
      log.debug('map | wiRecord:', wiRecord)
      if (wiRecord.count === 0) {
        if (isBackOrder === false) {
          lineRecord.ID = ''
        } else {
          existingError = true
        }
      } else {
        lineRecord.ID = wiRecord.body.id
        log.debug('map ~ lineRecord.ID:', lineRecord.ID)
        lineRecord[WEINFUSE.FIELDS.WE_INFUSE] = wiRecord.body.we_in
        lineRecord[WEINFUSE.FIELDS.LINE_ID] = wiRecord.body.line_id
        lineRecord[WEINFUSE.FIELDS.PO] = wiRecord.body.po
        lineRecord[WEINFUSE.FIELDS.TO] = wiRecord.body.to
        lineRecord[WEINFUSE.FIELDS.SO] = wiRecord.body.so
        lineRecord[WEINFUSE.FIELDS.RECEIPT] = wiRecord.body.receipt
        lineRecord[WEINFUSE.FIELDS.FULFILLMENT] = wiRecord.body.fulfillment
        lineRecord[WEINFUSE.FIELDS.STATUS] = wiRecord.body.status
        lineRecord[WEINFUSE.FIELDS.STATUS_STAGE] = wiRecord.body.statusStage
        log.debug('map ~ lineRecord[WEINFUSE.FIELDS.STATUS_STAGE]:', lineRecord[WEINFUSE.FIELDS.STATUS_STAGE])
        if (lineRecord[WEINFUSE.FIELDS.STATUS_STAGE] === 1) {
          if (lineRecord[WEINFUSE.FIELDS.PO] || lineRecord[WEINFUSE.FIELDS.TO] || lineRecord[WEINFUSE.FIELDS.SO]) {
            if (isBackOrder === true) {
              lineRecord[WEINFUSE.FIELDS.EXPIRATION] = formatDate(value[JSON_STRUCTURE.LINE_ITEMS_EXPIRATION_DATE], false, false)
              lineRecord[WEINFUSE.FIELDS.LOT] = value[JSON_STRUCTURE.LINE_ITEMS_LOT]
            }
            if (!lineRecord[WEINFUSE.FIELDS.DATE_RECEIVED]) {
              lineRecord[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.PENDING)
              lineRecord[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.NOT_DATE_RECEIVE)
              // existingError = true
              // updateWeInfuseRecord(lineRecord.ID, {
              //   [WEINFUSE.FIELDS.STATUS]: lineRecord[WEINFUSE.FIELDS.STATUS],
              //   [WEINFUSE.FIELDS.ERRORS]: lineRecord[WEINFUSE.FIELDS.ERRORS]
              // })
            } else if (lineRecord[WEINFUSE.FIELDS.DATE_RECEIVED] && value[JSON_STRUCTURE.LINE_ITEMS_BACKORDERED] === 'No') {
              if (!lineRecord[WEINFUSE.FIELDS.LOT]) {
                lineRecord[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.ERROR)
                lineRecord[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.LOT_NOT_FOUND)
                // updateWeInfuseRecord(lineRecord.ID, {
                //   [WEINFUSE.FIELDS.STATUS]: lineRecord[WEINFUSE.FIELDS.STATUS],
                //   [WEINFUSE.FIELDS.ERRORS]: lineRecord[WEINFUSE.FIELDS.ERRORS]
                // })
                createLogAction(
                  logRecord,
                  STATUS.ERROR,
                  `A problem occurred while validate the information, the field ${WEINFUSE.FIELDS.LOT} shouldn't be ${
                    value[JSON_STRUCTURE.LINE_ITEMS_LOT] === '' ? 'empty' : value[JSON_STRUCTURE.LINE_ITEMS_LOT]
                  }`
                )
                // existingError = true
              }
              if (!lineRecord[WEINFUSE.FIELDS.EXPIRATION]) {
                lineRecord[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.ERROR)
                lineRecord[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.NOT_EXPIRATION_DATE)
                // updateWeInfuseRecord(lineRecord.ID, {
                //   [WEINFUSE.FIELDS.STATUS]: lineRecord[WEINFUSE.FIELDS.STATUS],
                //   [WEINFUSE.FIELDS.ERRORS]: lineRecord[WEINFUSE.FIELDS.ERRORS]
                // })
                createLogAction(
                  logRecord,
                  STATUS.ERROR,
                  `A problem occurred while validate the information, the field ${WEINFUSE.FIELDS.EXPIRATION} shouldn't be ${
                    value[JSON_STRUCTURE.LINE_ITEMS_EXPIRATION_DATE] === '' ? 'empty' : value[JSON_STRUCTURE.LINE_ITEMS_EXPIRATION_DATE]
                  }`
                )
                // existingError = true
              }
            }
          }
        } else {
          log.debug('map ~ lineRecord[WEINFUSE.FIELDS.STATUS_STAGE] on else:', lineRecord[WEINFUSE.FIELDS.STATUS_STAGE])
          if (lineRecord[WEINFUSE.FIELDS.STATUS_STAGE] === 0) {
            if (lineRecord[WEINFUSE.FIELDS.PO] || lineRecord[WEINFUSE.FIELDS.TO] || lineRecord[WEINFUSE.FIELDS.SO]) {
              if (lineRecord[WEINFUSE.FIELDS.RECEIPT] || lineRecord[WEINFUSE.FIELDS.FULFILLMENT]) {
                lineRecord[WEINFUSE.FIELDS.STATUS_STAGE] = 4
              } else {
                lineRecord[WEINFUSE.FIELDS.STATUS_STAGE] = 3
              }
            } else {
              lineRecord[WEINFUSE.FIELDS.STATUS_STAGE] = 2
            }
          }
        }
      }
      const tranId = lineRecord[WEINFUSE.FIELDS.TRANID]
      const newKey = `${value[JSON_STRUCTURE.GROUP_ID]}_${value[JSON_STRUCTURE.GROUP_NAME]}_${
        value[JSON_STRUCTURE.PURCHASE_ORDERS_ORDER_DATE]
      }_${tranId}`
      log.debug('map | lineRecord:', { existingError, lineRecord })
      if (existingError === false) {
        if (lineRecord[WEINFUSE.FIELDS.STATUS] === assignStatus(STATUS.ERROR)) {
          lineRecord[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.PENDING)
        }
        mapContext.write({ key: newKey, value: { lineRecord, value } })
      } else {
        if (lineRecord[WEINFUSE.FIELDS.STATUS] === assignStatus(STATUS.ERROR)) {
          lineRecord[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.PENDING)
        }
        mapContext.write({ key: newKey, value: { lineRecord, value } })
      }
    } catch (err) {
      log.error('Error on map', err)
    }
  }

  /**
   * Defines the function that is executed when the reduce entry point is triggered. This entry point is triggered
   * automatically when the associated map stage is complete. This function is applied to each group in the provided context.
   * @param {Object} reduceContext - Data collection containing the groups to process in the reduce stage. This parameter is
   *     provided automatically based on the results of the map stage.
   * @param {Iterator} reduceContext.errors - Serialized errors that were thrown during previous attempts to execute the
   *     reduce function on the current group
   * @param {number} reduceContext.executionNo - Number of times the reduce function has been executed on the current group
   * @param {boolean} reduceContext.isRestarted - Indicates whether the current invocation of this function is the first
   *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
   * @param {string} reduceContext.key - Key to be processed during the reduce stage
   * @param {List<String>} reduceContext.values - All values associated with a unique key that was passed to the reduce stage
   *     for processing
   * @since 2015.2
   */
  const reduce = reduceContext => {
    try {
      const { key, values } = reduceContext
      const { WEINFUSE } = RECORDS
      getStatusWeInfuse()
      getErrorsWeInfuse()
      const data = values.map(line => JSON.parse(line))
      if (data.length) {
        data.forEach((line, index) => {
          if (index === 0) {
            log.debug('value', line)
          }
          const response = { id: '', type: '' }
          if (line.lineRecord.ID !== '') {
            if (line.lineRecord[WEINFUSE.FIELDS.PO] || line.lineRecord[WEINFUSE.FIELDS.TO] || line.lineRecord[WEINFUSE.FIELDS.SO]) {
              if (
                line.lineRecord[WEINFUSE.FIELDS.STATUS] !== assignStatus(STATUS.ERROR) &&
                line.lineRecord[WEINFUSE.FIELDS.ERRORS] !== assignErrors(ERRORS.NOT_DATE_RECEIVE)
              ) {
                line.lineRecord[WEINFUSE.FIELDS.STATUS_STAGE] = 3
              }
            }
            delete line.lineRecord[WEINFUSE.FIELDS.DATE]
            response.id = updateWeInfuseRecord(line.lineRecord.ID, line.lineRecord)
            response.type = 'update'
          } else {
            line.lineRecord[WEINFUSE.FIELDS.STATUS_STAGE] = 2
            response.id = createWeInfuseRecord(line.lineRecord, line.value)
            response.type = 'create'
          }
          reduceContext.write({ key, value: response })
        })
      }
    } catch (err) {
      log.error('Error on reduce', err)
    }
  }

  /**
   * Defines the function that is executed when the summarize entry point is triggered. This entry point is triggered
   * automatically when the associated reduce stage is complete. This function is applied to the entire result set.
   * @param {Object} summaryContext - Statistics about the execution of a map/reduce script
   * @param {number} summaryContext.concurrency - Maximum concurrency number when executing parallel tasks for the map/reduce
   *     script
   * @param {Date} summaryContext.dateCreated - The date and time when the map/reduce script began running
   * @param {boolean} summaryContext.isRestarted - Indicates whether the current invocation of this function is the first
   *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
   * @param {Iterator} summaryContext.output - Serialized keys and values that were saved as output during the reduce stage
   * @param {number} summaryContext.seconds - Total seconds elapsed when running the map/reduce script
   * @param {number} summaryContext.usage - Total number of governance usage units consumed when running the map/reduce
   *     script
   * @param {number} summaryContext.yields - Total number of yields when running the map/reduce script
   * @param {Object} summaryContext.inputSummary - Statistics about the input stage
   * @param {Object} summaryContext.mapSummary - Statistics about the map stage
   * @param {Object} summaryContext.reduceSummary - Statistics about the reduce stage
   * @since 2015.2
   */
  const summarize = summaryContext => {
    try {
      const { output } = summaryContext
      let counterCreates = 0
      let counterUpdates = 0
      let counterFails = 0
      output.iterator().each((key, value) => {
        const line = JSON.parse(value)
        if (line?.type && line?.id) {
          switch (line.type) {
            case 'update':
              counterUpdates++
              break
            case 'create':
              counterCreates++
              break
          }
        } else {
          counterFails++
        }
        return true
      })
      log.audit('Results', `Staging resume, creates: ${counterCreates}, updates: ${counterUpdates}, fails: ${counterFails}`)
      const logRecord = getCacheValue(CONST_CACHE.KEY)
      createLogAction(logRecord, 'End Process')
      updateRecord(RECORDS.WI_LOG.ID, logRecord, { [RECORDS.WI_LOG.FIELDS.END]: formatDate(new Date(), true) })
      clearCacheValue(CONST_CACHE.KEY)
    } catch (err) {
      log.error('Error on summarize', err)
    }
  }

  /**
   * This function retrieves configuration data from a NetSuite record and stores it in an object.
   */
  const getConfig = () => {
    try {
      const { FIELDS } = RECORDS.CONFIG
      const objSearch = search.create({
        type: RECORDS.CONFIG.ID,
        filters: [[FIELDS.ISINACTIVE, search.Operator.IS, 'F']],
        columns: Object.values(FIELDS).map(x => {
          return { name: x }
        })
      })
      const results = objSearch.run().getRange({ start: 0, end: 1 })
      results.forEach(result => {
        Object.entries(FIELDS).forEach(([key, value]) => {
          MAIN_CONFIG[value] = result.getValue({ name: value })
        })
      })
      const companyInfo = config.load({ type: config.Type.COMPANY_PREFERENCES })
      MAIN_CONFIG.format_date = companyInfo.getValue({ fieldId: 'DATEFORMAT' })
    } catch (err) {
      log.error('Error on getConfig', err)
    }
  }

  /**
   * This function retrieves a Looker access token using a client ID and client secret.
   * @returns The function `getLookerToken` returns a string value representing the Looker access
   * token. If an error occurs during the process, the function logs an error message and returns an
   * empty string.
   */
  const getLookerToken = logRecord => {
    const { CONFIG } = RECORDS
    let token = ''
    try {
      const headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
      }
      const response = https.post({
        url: MAIN_CONFIG[CONFIG.FIELDS.URL1],
        body: `client_id=${MAIN_CONFIG[CONFIG.FIELDS.CLIENT_ID]}&client_secret=${MAIN_CONFIG[CONFIG.FIELDS.CLIENT_SECRET]}`,
        headers
      })

      const body = JSON.parse(response.body) ?? {}
      token = body?.access_token
    } catch (err) {
      log.error('Error on getLookerToken', err)
      createLogAction(logRecord, 'Error on get looker token', err.message)
    }
    return token
  }

  /**
   * This function retrieves Looker information using an access token and returns it as a JSON object.
   * @returns The function `getLookerInformation` returns the parsed JSON response obtained from a GET
   * request to a Looker API endpoint, using an access token passed as an argument. If an error occurs
   * during the execution of the function, it returns `false`.
   */
  const getLookerInformation = (accessToken, logRecord) => {
    try {
      const { CONFIG } = RECORDS
      const headerObj = {
        Authorization: `Bearer ${accessToken}`
      }
      const objScript = runtime.getCurrentScript()
      const urlToReport =
        MAIN_CONFIG[CONFIG.FIELDS.URL2] +
        (objScript.deploymentId === 'customdeploy_on_demd' ? MAIN_CONFIG[CONFIG.FIELDS.ID_REPORT_ON_DEMAND] : MAIN_CONFIG[CONFIG.FIELDS.ID_REPORT])
      const response = https.get({
        url: urlToReport,
        headers: headerObj
      })
      return JSON.parse(response.body)
    } catch (err) {
      log.error({
        title: 'Error occurred in getLookerInformation',
        details: err
      })
      createLogAction(logRecord, 'Error on get looker information', err.message)
      return false
    }
  }

  /**
   * The function `getQueryResults` retrieves query results using a query ID and access token, and
   * returns the results as a parsed JSON object or false if an error occurs.
   * @param queryId - The queryId parameter is a string that represents the unique identifier of a
   * query. It is used to retrieve the results of a specific query.
   * @param accessToken - The `accessToken` parameter is a string that represents the access token
   * required to authenticate the user making the API request. It is used to authorize the user and
   * grant access to the requested resources.
   * @returns The function `getQueryResults` returns the parsed JSON response from an HTTP GET request
   * made to a URL that includes a query ID and an access token in the headers. If an error occurs, it
   * returns `false`.
   */
  const getQueryResults = (queryId, accessToken, logRecord) => {
    try {
      const { CONFIG } = RECORDS
      let urlReplace = MAIN_CONFIG[CONFIG.FIELDS.URL3].replace('{queryId}', queryId)
      log.debug('getQueryResults | accessToken:', accessToken)
      const headerObj = {
        Authorization: `Bearer ${accessToken}`
      }
      /* ------------------------------------------------------------------------------------ */
      // Assign the new limit of the query for results
      urlReplace = urlReplace.concat(`?limit=${MAIN_CONFIG[CONFIG.FIELDS.LIMIT_RESULTS]}`)
      /* ------------------------------------------------------------------------------------ */
      log.debug('getQueryResults | urlReplace:', urlReplace)
      const response = https.get({
        url: urlReplace,
        headers: headerObj
      })
      return JSON.parse(response.body)
    } catch (err) {
      log.error({
        title: 'Error occurred in getQueryResults',
        details: err
      })
      createLogAction(logRecord, 'Error on get query results', err.message)
      return false
    }
  }
  /**
   * The function writes a key-value pair to a temporary cache.
   * @param key - The key is a unique identifier that is used to store and retrieve the value from the
   * cache. It is usually a string or a number.
   * @param value - The `value` parameter is the data that needs to be stored in the cache with the
   * corresponding `key`.
   */
  const writeCache = (key, value) => {
    try {
      const objCache = cache.getCache({ name: 'temporaryCache', scope: cache.Scope.PRIVATE })
      objCache.put({ key, value })
    } catch (err) {
      log.error('Error on writeCache', err)
    }
  }

  /**
   * This function retrieves a value from a private cache using a specified key.
   * @returns The function `getCacheValue` returns the value stored in the cache for the given `key`.
   * If there is an error, it logs an error message and does not return anything.
   */
  const getCacheValue = key => {
    try {
      const objCache = cache.getCache({ name: 'temporaryCache', scope: cache.Scope.PRIVATE })
      const result = objCache.get({ key })
      return result
    } catch (err) {
      log.error('Error on getCacheValue', err)
    }
  }

  /**
   * This function clears a specific value from a private cache called "temporaryCache".
   */
  const clearCacheValue = key => {
    try {
      const objCache = cache.getCache({ name: 'temporaryCache', scope: cache.Scope.PRIVATE })
      objCache.remove({ key })
    } catch (err) {
      log.error('Error on clearCacheValue', err)
    }
  }

  /**
   * This function creates a new record in NetSuite's WI_LOG record type and returns the ID of the
   * created record.
   * @returns The function `createLogProcess` returns the `recordId` of the newly created record.
   */
  const createLogProcess = () => {
    try {
      const { WI_LOG } = RECORDS
      const objRecord = record.create({ type: WI_LOG.ID })
      objRecord.setValue({ fieldId: WI_LOG.FIELDS.START, value: formatDate(new Date(), true) })
      const recordId = objRecord.save()
      return recordId
    } catch (err) {
      log.error('Error on createLogProcess', err)
    }
  }

  /**
   * The function creates a log action record with main, action, and detail values.
   * @param main - The main category or topic of the log action being created.
   * @param action - The "action" parameter is a string that represents the name or description of the
   * action being logged. It is used as a value for the "NAME" field in the NetSuite record being
   * created.
   * @param [detail] - The `detail` parameter is an optional string parameter that represents
   * additional details or information related to the log action being created. If provided, it will be
   * saved in the "DETAILS" field of the log record. If not provided, the "DETAILS" field will be left
   * blank.
   */
  const createLogAction = (main, action, detail = '') => {
    try {
      const { LOG_LINES } = RECORDS
      const objRecord = record.create({ type: LOG_LINES.ID })
      objRecord.setValue({ fieldId: LOG_LINES.FIELDS.LOG_MAIN, value: main })
      objRecord.setValue({ fieldId: LOG_LINES.FIELDS.NAME, value: action })
      objRecord.setValue({ fieldId: LOG_LINES.FIELDS.DETAILS, value: detail })
      objRecord.save()
    } catch (err) {
      log.error('Error on createLogAction', err)
    }
  }

  /**
   * This is a JavaScript function that formats a given date and time according to a specified format.
   * @param d - The parameter `d` is a date object or a string representing a date that needs to be
   * formatted.
   * @param [dateTime=false] - The `dateTime` parameter is a boolean flag that determines whether the
   * formatted date should include the time as well. If `dateTime` is `true`, the formatted date will
   * include the time in the format `HH:mm:ss`. If `dateTime` is `false` or not provided, the formatted
   * date
   * @returns The function `formatDate` returns a new `Date` object that represents the formatted date
   * and time (if `dateTime` is `true`) of the input `d` parameter, using the date format specified in
   * the `MAIN_CONFIG` object. If an error occurs during the execution of the function, it logs an
   * error message and returns `undefined`.
   */
  const formatDate = (d, dateTime = false, newDate = true) => {
    try {
      // log.debug('formatDate ~ d:', [d, typeof d])
      if (d === null || d === '') {
        return ''
      }
      const localFormat = dateTime === true ? `${MAIN_CONFIG.format_date} HH:mm:ss` : MAIN_CONFIG.format_date
      const newFormat = moment(d).format(localFormat)
      if (newDate) {
        return new Date(newFormat)
      } else {
        return newFormat
      }
    } catch (err) {
      log.error('Error on formatDate', err)
    }
  }

  /**
   * The function `getTypeTransactions` retrieves transaction types from a search and stores them in an
   * object.
   */
  const getTypeTransactions = () => {
    try {
      const { WEINFUSE_TYPE_TRANSACTION } = LIST
      const objSearch = search.create({
        type: WEINFUSE_TYPE_TRANSACTION.ID,
        filters: [],
        columns: Object.values(WEINFUSE_TYPE_TRANSACTION.FIELDS).map(f => {
          return { name: f }
        })
      })
      objSearch.run().each(result => {
        TRANSACTION_TYPE[result.getValue({ name: WEINFUSE_TYPE_TRANSACTION.FIELDS.NAME })] = result.getValue({
          name: WEINFUSE_TYPE_TRANSACTION.FIELDS.INTERNALID
        })
        return true
      })
    } catch (err) {
      log.error('Error on getTypeTransactions', err)
    }
  }

  /**
   * This function retrieves data from a NetSuite search for WeInfuse status and logs any errors.
   */
  const getStatusWeInfuse = () => {
    try {
      const objSearch = search.create({
        type: LIST.WEINFUSE_STATUS.ID,
        filters: [],
        columns: Object.values(LIST.WEINFUSE_STATUS.FIELDS).map(f => {
          return { name: f }
        })
      })
      objSearch.run().each(result => {
        const line = {}
        Object.values(LIST.WEINFUSE_STATUS.FIELDS).forEach(f => {
          line[f] = result.getValue({ name: f })
        })
        ALL_STATUS.push(line)
        return true
      })
    } catch (err) {
      log.error('Error on getStatusWeInfuse', err)
    }
  }
  /**
   * This function retrieves data from a NetSuite search for WeInfuse status and logs any errors.
   */
  const getErrorsWeInfuse = () => {
    try {
      const objSearch = search.create({
        type: LIST.WEINFUSE_ERRORS.ID,
        filters: [],
        columns: Object.values(LIST.WEINFUSE_ERRORS.FIELDS).map(f => {
          return { name: f }
        })
      })
      objSearch.run().each(result => {
        const line = {}
        Object.values(LIST.WEINFUSE_ERRORS.FIELDS).forEach(f => {
          line[f] = result.getValue({ name: f })
        })
        ALL_ERRORS.push(line)
        return true
      })
    } catch (err) {
      log.error('Error on getErrorsWeInfuse', err)
    }
  }

  /**
   * The function assigns an ID to a given event based on its corresponding text in an array of status
   * objects.
   * @param evt - The parameter `evt` is a string representing the text of a status. The function
   * `assignStatus` searches for the corresponding ID of the status in the `ALL_STATUS` array and
   * returns it. If the status is not found, it logs an error message.
   * @returns The function `assignStatus` is returning the `id` property of an object in the
   * `ALL_STATUS` array that has a `text` property matching the input `evt`. If there is no match, the
   * function will catch the error and log it, but it will not return anything.
   */
  const assignStatus = evt => {
    try {
      const { WEINFUSE_STATUS } = LIST
      return ALL_STATUS.find(x => x[WEINFUSE_STATUS.FIELDS.NAME] === evt)[WEINFUSE_STATUS.FIELDS.INTERNALID]
    } catch (err) {
      log.error('Error on assignStatus', err)
    }
  }

  const assignErrors = evt => {
    try {
      const { WEINFUSE_ERRORS } = LIST
      const id = ALL_ERRORS.find(x => x[WEINFUSE_ERRORS.FIELDS.NAME] === evt)
      if (id?.[WEINFUSE_ERRORS.FIELDS.INTERNALID]) {
        return id[WEINFUSE_ERRORS.FIELDS.INTERNALID]
      } else {
        return ''
      }
    } catch (err) {
      log.error('Error on assignErrors', err)
    }
  }

  /**
   * The `assignData` function in JavaScript is used to assign data to various fields in a `lineRecord`
   * object based on the values provided in the `value` parameter, with error handling and validation
   * checks.
   * @param lineRecord - `lineRecord` is an object that represents a record in the system. It contains
   * various fields and their corresponding values.
   * @param value - The `value` parameter is an object that contains the data to be assigned to the
   * `lineRecord` object. It is used to retrieve specific values from the object using the keys defined
   * in the `JSON_STRUCTURE` object.
   * @param existingError - The `existingError` parameter is a boolean variable that indicates whether
   * there is already an existing error in the data. It is initially passed as a parameter to the
   * `assignData` function and can be modified within the function based on certain conditions.
   * @param isBackOrder - The `isBackOrder` parameter is a boolean variable that indicates whether the
   * purchase order is a backorder or not. It is initially passed as a parameter to the `assignData`
   * function and can be modified within the function based on certain conditions.
   */
  const assignData = (lineRecord, value, existingError, isBackOrder, logRecord) => {
    try {
      const { WEINFUSE } = RECORDS
      let PO_NUMBER = value[JSON_STRUCTURE.PURCHASE_ORDERS_PO_NUMBER]
      /* The above code is checking if the string variable `PO_NUMBER` contains the substring "-
        backorder". If it does, it splits the string at the first occurrence of "- backorder" and
        assigns the part before it to the `PO_NUMBER` variable. */
      if (PO_NUMBER.indexOf('- backorder') !== -1) {
        PO_NUMBER = PO_NUMBER.split('- backorder')[0]
        isBackOrder = true
      }
      PO_NUMBER = PO_NUMBER.trim()
      lineRecord[WEINFUSE.FIELDS.BACKORDER] = value[JSON_STRUCTURE.LINE_ITEMS_BACKORDERED] === 'Yes'
      // Match primary information
      lineRecord[WEINFUSE.FIELDS.WE_INFUSE] = value[JSON_STRUCTURE.PURCHASE_ORDERS_ID]
        ? value[JSON_STRUCTURE.PURCHASE_ORDERS_ID]
        : value[JSON_STRUCTURE.PURCHASE_ORDERS_INVENTORY_ADJUSTMENT_ID]
      lineRecord[WEINFUSE.FIELDS.LINE_ID] = value[JSON_STRUCTURE.LINE_ITEMS_ID]
      lineRecord[WEINFUSE.FIELDS.TRANID] = PO_NUMBER || value[JSON_STRUCTURE.PURCHASE_ORDERS_INVENTORY_ADJUSTMENT_ID]
      lineRecord[WEINFUSE.FIELDS.NDC] = value[JSON_STRUCTURE.INVENTORY_ITEMS_OUTER_NDC_CODE]
      lineRecord[WEINFUSE.FIELDS.QUANTITY] = value[JSON_STRUCTURE.LINE_ITEMS_QTY]
      lineRecord[WEINFUSE.FIELDS.PRICE] = value[JSON_STRUCTURE.LINE_ITEMS_PRICE]
      lineRecord[WEINFUSE.FIELDS.DATE] = value[JSON_STRUCTURE.PURCHASE_ORDERS_ORDER_DATE]
        ? formatDate(value[JSON_STRUCTURE.PURCHASE_ORDERS_ORDER_DATE], false, false)
        : ''
      lineRecord[WEINFUSE.FIELDS.TYPE] = TRANSACTION_TYPE[value[JSON_STRUCTURE.PURCHASE_ORDERS_TYPE]] || ''
      Object.entries(lineRecord).forEach(([field, val]) => {
        if (val === '' || val === null) {
          lineRecord[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.ERROR)
          createLogAction(
            logRecord,
            STATUS.ERROR,
            `A problem occurred while validate the information, the field ${field} shouldn't be ${val === '' ? 'empty' : val}`
          )
          existingError = true
        }
      })

      /**
       * Looker Information
       */
      lineRecord[WEINFUSE.FIELDS.GROUP_ID] = value[JSON_STRUCTURE.GROUP_ID]
      lineRecord[WEINFUSE.FIELDS.GROUP_NAME] = value[JSON_STRUCTURE.GROUP_NAME]
      lineRecord[WEINFUSE.FIELDS.LOCATION_ID] = value[JSON_STRUCTURE.LOCATIONS_ID]
      lineRecord[WEINFUSE.FIELDS.LOCATION_NAME] = value[JSON_STRUCTURE.LOCATIONS_NAME]
      lineRecord[WEINFUSE.FIELDS.OUTER_NDC] = value[JSON_STRUCTURE.INVENTORY_ITEMS_OUTER_NDC_CODE]
      lineRecord[WEINFUSE.FIELDS.PO_ID] = value[JSON_STRUCTURE.PURCHASE_ORDERS_ID]
      lineRecord[WEINFUSE.FIELDS.ORDER_NUMBER] = value[JSON_STRUCTURE.PURCHASE_ORDERS_ORDER_NUMBER]
      lineRecord[WEINFUSE.FIELDS.PO_NUMBER] = PO_NUMBER
      lineRecord[WEINFUSE.FIELDS.INV_ADJ_ID] = value[JSON_STRUCTURE.PURCHASE_ORDERS_INVENTORY_ADJUSTMENT_ID]
      lineRecord[WEINFUSE.FIELDS.DATE_RECEIVED] = value[JSON_STRUCTURE.PURCHASE_ORDERS_DATE_RECEIVED_DATE]
        ? formatDate(value[JSON_STRUCTURE.PURCHASE_ORDERS_DATE_RECEIVED_DATE], false, false)
        : ''
      lineRecord[WEINFUSE.FIELDS.VENDOR_NAME] = value[JSON_STRUCTURE.VENDORS_NAME]
      lineRecord[WEINFUSE.FIELDS.NDC_NAME] = value[JSON_STRUCTURE.NDCS_LABEL_NAME]
      lineRecord[WEINFUSE.FIELDS.PO_TYPE] = value[JSON_STRUCTURE.PURCHASE_ORDERS_TYPE]
      // lineRecord[WEINFUSE.FIELDS.PURCHASE_ORDERS_PO_NUMBER] = value[JSON_STRUCTURE.PURCHASE_ORDERS_PO_NUMBER]

      lineRecord[WEINFUSE.FIELDS.EXPIRATION] = value[JSON_STRUCTURE.LINE_ITEMS_EXPIRATION_DATE]
        ? formatDate(value[JSON_STRUCTURE.LINE_ITEMS_EXPIRATION_DATE], false, false)
        : ''
      lineRecord[WEINFUSE.FIELDS.LOT] = value[JSON_STRUCTURE.LINE_ITEMS_LOT] ? value[JSON_STRUCTURE.LINE_ITEMS_LOT] : ''

      // Match the item
      const getItem = searchItem(value[JSON_STRUCTURE.INVENTORY_ITEMS_OUTER_NDC_CODE])
      if (getItem.err) {
        lineRecord[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.ERROR)
        lineRecord[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.ITEM_NOT_FOUND)
        lineRecord[WEINFUSE.FIELDS.ON_ERROR] = true
        existingError = true
        createLogAction(logRecord, STATUS.ERROR, `A problem occurred while searching for the item ${value[JSON_STRUCTURE.INVENTORY_ITEMS_NDC_CODE]}`)
      } else if (getItem.count > 1 || getItem.count === 0) {
        lineRecord[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.ERROR)
        lineRecord[WEINFUSE.FIELDS.ERRORS] = getItem.count === 0 ? assignErrors(ERRORS.ITEM_NOT_FOUND) : assignErrors(ERRORS.ITEM_DUPLICATED)
        lineRecord[WEINFUSE.FIELDS.ON_ERROR] = true
        existingError = true
      } else {
        if (getItem.body[0][RECORDS.ITEM.FIELDS.ISINACTIVE]) {
          lineRecord[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.ERROR)
          lineRecord[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.INACTIVE_ITEM)
          lineRecord[WEINFUSE.FIELDS.ON_ERROR] = true
          existingError = true
        } else {
          lineRecord[WEINFUSE.FIELDS.ITEM] = getItem.body[0][RECORDS.ITEM.FIELDS.INTERNALID]
          lineRecord[WEINFUSE.FIELDS.PURCHASE_UNIT_ID] = getItem.body[0][RECORDS.ITEM.FIELDS.PURCHASE_UNIT_ID]
          lineRecord[WEINFUSE.FIELDS.PURCHASE_UNIT] = getItem.body[0][RECORDS.ITEM.FIELDS.PURCHASE_UNIT]
          lineRecord[WEINFUSE.FIELDS.UNIT_TYPE] = getItem.body[0][RECORDS.ITEM.FIELDS.UNIT_TYPE]
        }
      }

      // Match Unit Type
      const getUnitType = searchUnitsType(lineRecord[WEINFUSE.FIELDS.UNIT_TYPE], lineRecord[WEINFUSE.FIELDS.PURCHASE_UNIT])
      if (getUnitType.err) {
        lineRecord[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.ERROR)
        lineRecord[WEINFUSE.FIELDS.ERRORS] = !lineRecord[WEINFUSE.FIELDS.ERRORS]
          ? assignErrors(ERRORS.UNIT_NOT_FOUND)
          : lineRecord[WEINFUSE.FIELDS.ERRORS]
        lineRecord[WEINFUSE.FIELDS.ON_ERROR] = true
        existingError = true
        log.error({title: 'getUnitType.err', details: getUnitType.err});
        createLogAction(logRecord, STATUS.ERROR, `A problem occurred while searching for the unit ${value[JSON_STRUCTURE.INVENTORY_ITEMS_NDC_CODE]}`)
      }

      // Match the ship to
      const getShipTo = searchShipTo(value[JSON_STRUCTURE.LOCATIONS_NAME])
      log.debug('assignData ~ getShipTo:', getShipTo)
      if (getShipTo.err) {
        lineRecord[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.ERROR)
        lineRecord[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.SHIP_TO_NOT_FOUND)
        createLogAction(logRecord, STATUS.ERROR, `A problem ocurred while searching the ship to information ${value[JSON_STRUCTURE.LOCATIONS_NAME]}`)
        lineRecord[WEINFUSE.FIELDS.ON_ERROR] = true
        existingError = true
      } else if (getShipTo.count > 1 || getShipTo.count === 0) {
        lineRecord[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.ERROR)
        lineRecord[WEINFUSE.FIELDS.ERRORS] = getShipTo.count === 0 ? assignErrors(ERRORS.SHIP_TO_NOT_FOUND) : assignErrors(ERRORS.SHIP_TO_DUPLICATED)
        lineRecord[WEINFUSE.FIELDS.ON_ERROR] = true
        existingError = true
      } else {
        if (getShipTo.body[0][RECORDS.SHIPTO.FIELDS.ISINACTIVE]) {
          lineRecord[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.ERROR)
          lineRecord[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.INACTIVE_SHIP_TO)
          lineRecord[WEINFUSE.FIELDS.ON_ERROR] = true
          existingError = true
        } else {
          lineRecord[WEINFUSE.FIELDS.SHIP_TO] = getShipTo.body[0][RECORDS.SHIPTO.FIELDS.INTERNALID]
          lineRecord[WEINFUSE.FIELDS.SUBSIDIARY] = getShipTo.body[0][RECORDS.SHIPTO.FIELDS.SUBSIDIARY]
          lineRecord[WEINFUSE.FIELDS.CUSTOMER] = getShipTo.body[0][RECORDS.SHIPTO.FIELDS.ID]
          lineRecord[WEINFUSE.FIELDS.CUSTOMER_CATEGORY] = getShipTo.body[0][RECORDS.SHIPTO.FIELDS.CATEGORY]
        }
      }

      // Match the location
      if (lineRecord[WEINFUSE.FIELDS.CUSTOMER] && lineRecord[WEINFUSE.FIELDS.CUSTOMER_CATEGORY] === MAIN_CONFIG[RECORDS.CONFIG.FIELDS.IS_CORE]) {
        lineRecord[WEINFUSE.FIELDS.LOCATION] = MAIN_CONFIG[RECORDS.CONFIG.FIELDS.CORE_LOCATION]
      } else {
        const getLocation = searchLocation(lineRecord[WEINFUSE.FIELDS.SHIP_TO])
        log.debug('assignData ~ getLocation:', getLocation)
        if (getLocation.err) {
          lineRecord[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.ERROR)
          lineRecord[WEINFUSE.FIELDS.ERRORS] = !lineRecord[WEINFUSE.FIELDS.ERRORS]
            ? assignErrors(ERRORS.LOCATION_NOT_FOUND)
            : lineRecord[WEINFUSE.FIELDS.ERRORS]
          lineRecord[WEINFUSE.FIELDS.ON_ERROR] = true
          createLogAction(logRecord, STATUS.ERROR, `A problem occurred while searching for the location ${value[JSON_STRUCTURE.LOCATIONS_NAME]}`)
          existingError = true
        } else if (getLocation.count > 1 || getLocation.count === 0) {
          lineRecord[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.ERROR)
          lineRecord[WEINFUSE.FIELDS.ERRORS] = !lineRecord[WEINFUSE.FIELDS.ERRORS]
            ? getLocation.count === 0
              ? assignErrors(ERRORS.LOCATION_NOT_FOUND)
              : assignErrors(ERRORS.LOCATION_DUPLICATED)
            : lineRecord[WEINFUSE.FIELDS.ERRORS]
          lineRecord[WEINFUSE.FIELDS.ON_ERROR] = true
          existingError = true
        } else {
          if (getLocation.body[0][RECORDS.LOCATION.FIELDS.ISINACTIVE]) {
            lineRecord[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.ERROR)
            lineRecord[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.INACTIVE_LOCATION)
            lineRecord[WEINFUSE.FIELDS.ON_ERROR] = true
            existingError = true
          } else {
            lineRecord[WEINFUSE.FIELDS.LOCATION] = getLocation.body[0][RECORDS.LOCATION.FIELDS.INTERNALID]
          }
        }
      }

      // Match vendor
      if (value[JSON_STRUCTURE.VENDORS_NAME] !== 'Healix') {
        const getVendor = searchVendor(value[JSON_STRUCTURE.VENDORS_NAME])
        if (getVendor.err) {
          lineRecord[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.ERROR)
          lineRecord[WEINFUSE.FIELDS.ERRORS] = !lineRecord[WEINFUSE.FIELDS.ERRORS]
            ? assignErrors(ERRORS.VENDOR_NOT_FOUND)
            : lineRecord[WEINFUSE.FIELDS.ERRORS]
          createLogAction(logRecord, STATUS.ERROR, `A problem occurred while searching for the vendor ${value[JSON_STRUCTURE.VENDORS_NAME]}`)
        } else if (getVendor.count > 1 || getVendor.count === 0) {
          lineRecord[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.ERROR)
          lineRecord[WEINFUSE.FIELDS.ERRORS] = !lineRecord[WEINFUSE.FIELDS.ERRORS]
            ? getVendor.count === 0
              ? assignErrors(ERRORS.VENDOR_NOT_FOUND)
              : assignErrors(ERRORS.VENDOR_DUPLICATED)
            : lineRecord[WEINFUSE.FIELDS.ERRORS]
          existingError = true
        } else {
          if (getVendor.body[0][RECORDS.VENDOR.FIELDS.ISINACTIVE]) {
            lineRecord[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.ERROR)
            lineRecord[WEINFUSE.FIELDS.ERRORS] = !lineRecord[WEINFUSE.FIELDS.ERRORS]
              ? assignErrors(ERRORS.INACTIVE_VENDOR)
              : lineRecord[WEINFUSE.FIELDS.ERRORS]
            lineRecord[WEINFUSE.FIELDS.ON_ERROR] = true
            existingError = true
          } else {
            lineRecord[WEINFUSE.FIELDS.VENDOR] = getVendor.body[0][RECORDS.VENDOR.FIELDS.INTERNALID]
          }
        }
      }
    } catch (err) {
      log.error('Error on assignData', err)
    }
    return { existingError, isBackOrder }
  }

  /**
   * The function searches for locations in a NetSuite account based on a given label.
   * @param {string} shipTo
   * @returns The function `searchLocation` returns an object with three properties: `count`, `err`,
   * and `body`. The `count` property is a number representing the number of search results found. The
   * `err` property is a boolean indicating whether an error occurred during the search. The `body`
   * property is an array of objects representing the search results, with each object containing the
   * internal ID and name
   */
  const searchLocation = (shipTo = '') => {
    const response = { count: 0, err: false, body: [] }
    try {
      if (shipTo === '' || shipTo === null) return response
      const { LOCATION } = RECORDS
      const objSearch = search.create({
        type: search.Type.LOCATION,
        filters: [[LOCATION.FIELDS.ISINACTIVE, search.Operator.IS, 'F'], 'AND', [LOCATION.FIELDS.SHIPTO, search.Operator.IS, shipTo]],
        columns: Object.values(LOCATION.FIELDS).map(f => {
          const filter = {}
          if (f.indexOf('.') !== -1) {
            f = f.split('.')
            filter.name = f[1]
            filter.join = f[0]
          } else {
            filter.name = f
          }
          return filter
        })
      })
      const countResults = objSearch.runPaged().count
      if (countResults > 0) {
        response.count = countResults
        const results = objSearch.run().getRange({ start: 0, end: 10 })
        response.body = results.map(result => {
          return {
            [LOCATION.FIELDS.SUBSIDIARY]: result.getValue({ name: LOCATION.FIELDS.SUBSIDIARY }),
            [LOCATION.FIELDS.INTERNALID]: result.getValue({ name: LOCATION.FIELDS.INTERNALID }),
            [LOCATION.FIELDS.NAME]: result.getValue({ name: LOCATION.FIELDS.NAME }),
            [LOCATION.FIELDS.ISINACTIVE]: result.getValue({ name: LOCATION.FIELDS.ISINACTIVE })
          }
        })
      }
    } catch (err) {
      log.error('Error on searchLocation', err)
      response.err = true
    }
    return response
  }

  /**
   * The function searches for an item in NetSuite based on its name and returns information about the
   * item if it exists.
   * @returns The function `searchItem` returns an object with three properties: `count`, `err`, and
   * `body`. The `count` property is a number representing the number of search results found. The
   * `err` property is a boolean indicating whether an error occurred during the search. The `body`
   * property is an array of objects representing the search results, with each object containing the
   * internal ID, name
   */
  const searchItem = (description = '') => {
    const response = { count: 0, err: false, body: [] }
    try {
      if (description === '' || description === null) return response
      const { ITEM } = RECORDS
      const objSearch = search.create({
        type: search.Type.ITEM,
        filters: [
          [`formulatext:{${ITEM.FIELDS.NAME}}`, search.Operator.IS, description],
          'OR',
          [ITEM.FIELDS.ALT_NAME, search.Operator.IS, description]
        ],
        columns: Object.values(ITEM.FIELDS).map(field => {
          return { name: field }
        })
      })
      const countResults = objSearch.runPaged().count
      if (countResults > 0) {
        response.count = countResults
        const results = objSearch.run().getRange({ start: 0, end: 10 })
        response.body = results.map(result => {
          return {
            [ITEM.FIELDS.INTERNALID]: result.getValue({ name: ITEM.FIELDS.INTERNALID }),
            [ITEM.FIELDS.NAME]: result.getValue({ name: ITEM.FIELDS.NAME }),
            [ITEM.FIELDS.ISINACTIVE]: result.getValue({ name: ITEM.FIELDS.ISINACTIVE }),
            [ITEM.FIELDS.PURCHASE_UNIT]: result.getText({ name: ITEM.FIELDS.PURCHASE_UNIT }),
            [ITEM.FIELDS.PURCHASE_UNIT_ID]: result.getValue({ name: ITEM.FIELDS.PURCHASE_UNIT }),
            [ITEM.FIELDS.UNIT_TYPE]: result.getValue({ name: ITEM.FIELDS.UNIT_TYPE })
          }
        })
      }
    } catch (err) {
      log.error('Error on searchItem', err)
      response.err = true
    }
    return response
  }

  /**
   * The function `searchShipTo` searches for customer ship-to addresses based on a provided label and
   * returns the count and details of the matching addresses.
   * @returns The function `searchShipTo` returns an object with the following properties:
   */
  const searchShipTo = (label = '') => {
    const response = { count: 0, err: false, body: [] }
    try {
      if (label === '' || label === null) return response
      const { SHIPTO } = RECORDS
      label = label.split(' ')
      const objSearch = search.create({
        type: search.Type.CUSTOMER,
        filters: [[SHIPTO.FIELDS.ISINACTIVE, search.Operator.IS, 'F'], 'AND', [SHIPTO.FIELDS.ADDRESS_LABEL, search.Operator.IS, label[0]]],
        columns: Object.values(SHIPTO.FIELDS).map(f => ({ name: f }))
      })
      const countResults = objSearch.runPaged().count
      if (countResults > 0) {
        response.count = countResults
        const results = objSearch.run().getRange({ start: 0, end: 10 })
        response.body = results.map(result => ({
          [SHIPTO.FIELDS.ISINACTIVE]: result.getValue({ name: SHIPTO.FIELDS.ISINACTIVE }),
          [SHIPTO.FIELDS.INTERNALID]: result.getValue({ name: SHIPTO.FIELDS.INTERNALID }),
          [SHIPTO.FIELDS.ADDRESS_LABEL]: result.getValue({ name: SHIPTO.FIELDS.ADDRESS_LABEL }),
          [SHIPTO.FIELDS.ID]: result.getValue({ name: SHIPTO.FIELDS.ID }),
          [SHIPTO.FIELDS.CATEGORY]: result.getValue({ name: SHIPTO.FIELDS.CATEGORY }),
          [SHIPTO.FIELDS.SUBSIDIARY]: result.getValue({ name: SHIPTO.FIELDS.SUBSIDIARY })
        }))
      }
    } catch (err) {
      log.error('Error on searchShipTo', err)
      response.err = true
    }
    return response
  }

  /**
   * The function `searchVendor` searches for vendors based on a given label and returns the count,
   * body, and error status of the search.
   * @returns The function `searchVendor` returns an object with the following properties:
   */
  const searchVendor = label => {
    const response = { count: 0, body: [], err: false }
    try {
      if (label === '' || label === null) return response
      const { VENDOR } = RECORDS
      const objSearch = search.create({
        type: search.Type.VENDOR,
        filters: [[VENDOR.FIELDS.NAME, search.Operator.CONTAINS, label], 'AND', [VENDOR.FIELDS.REPRESENTING, search.Operator.ANYOF, '@NONE@']],
        columns: Object.values(VENDOR.FIELDS).map(f => {
          return { name: f }
        })
      })
      const countResults = objSearch.runPaged().count
      if (countResults > 0) {
        response.count = countResults
        const results = objSearch.run().getRange({ start: 0, end: 10 })
        response.body = results.map(result => {
          return {
            [VENDOR.FIELDS.INTERNALID]: result.getValue({ name: VENDOR.FIELDS.INTERNALID }),
            [VENDOR.FIELDS.NAME]: result.getValue({ name: VENDOR.FIELDS.NAME }),
            [VENDOR.FIELDS.ISINACTIVE]: result.getValue({ name: VENDOR.FIELDS.ISINACTIVE }),
            [VENDOR.FIELDS.CATEGORY]: result.getValue({ name: VENDOR.FIELDS.CATEGORY })
          }
        })
      }
    } catch (err) {
      log.error('Error on searchVendor', err)
      response.err = true
    }
    return response
  }

  /**
   * The function `searchUnitsType` searches for unit types based on specified criteria and returns the
   * count and data of the matching unit types, or an error message if no unit types are found.
   * @param [unit] - The `unit` parameter is used to specify the internal ID of the unit type you want
   * to search for. It is an optional parameter and can be left empty.
   * @param [purchase] - The `purchase` parameter is used to filter the search results based on the
   * unit name. It is a string that represents the name of the unit to be searched for.
   * @returns The function `searchUnitsType` returns an object with the following properties:
   */
  const searchUnitsType = (unit = '', purchase = '') => {
    const response = { count: 0, data: [], err: false, msg: '' }
    try {
      if (unit === '' && purchase === '') {
        response.err = true
        response.msg = 'Unit type not found'
        return response
      }
      const { UNIT_TYPE } = LIST
      const objSearch = search.create({
        type: search.Type.UNITS_TYPE,
        filters: [[UNIT_TYPE.FIELDS.INTERNALID, search.Operator.ANYOF, unit], 'AND', [UNIT_TYPE.FIELDS.UNITNAME, search.Operator.IS, purchase]],
        columns: [{ name: UNIT_TYPE.FIELDS.INTERNALID }, { name: UNIT_TYPE.FIELDS.CONVERSIONRATE }]
      })
      const resultCount = objSearch.runPaged().count
      if (resultCount > 0) {
        response.count = resultCount
        const results = objSearch.run().getRange({ start: 0, end: 10 })
        results.forEach(result => {
          response.data.push(result.getValue({ name: UNIT_TYPE.FIELDS.INTERNALID }))
        })
      } else {
        response.err = true
        response.msg = 'Unit type not found'
      }
    } catch (err) {
      log.error('Error on searchUnitsType', err)
      response.err = true
      response.msg = 'Ocurred an error for search unit type'
    }
    return response
  }

  /**
   * The function `searchWeInfuseRecord` searches for a record in the WEINFUSE table based on the
   * provided filters and returns the count and the ID of the first matching record.
   * @returns The function `searchWeInfuseRecord` returns an object with two properties: `count` and
   * `body`. The `count` property represents the total number of records found in the search, and the
   * `body` property contains the details of the first record found, including its `id`.
   */
  const searchWeInfuseRecord = filters => {
    const data = { count: 0, body: {} }
    try {
      const { WEINFUSE } = RECORDS
      const objSearch = search.create({
        type: WEINFUSE.ID,
        filters: [], // filters.map(filter => filter),
        columns: [
          { name: WEINFUSE.FIELDS.INTERNALID },
          { name: WEINFUSE.FIELDS.WE_INFUSE },
          { name: WEINFUSE.FIELDS.LINE_ID },
          { name: WEINFUSE.FIELDS.PO },
          { name: WEINFUSE.FIELDS.TO },
          { name: WEINFUSE.FIELDS.SO },
          { name: WEINFUSE.FIELDS.RECEIPT },
          { name: WEINFUSE.FIELDS.FULFILLMENT },
          { name: WEINFUSE.FIELDS.STATUS },
          { name: WEINFUSE.FIELDS.STATUS_STAGE },
          { name: WEINFUSE.FIELDS.RECEIPT },
          { name: WEINFUSE.FIELDS.FULFILLMENT }
        ]
      })
      objSearch.filterExpression = filters
      data.count = objSearch.runPaged().count
      const results = objSearch.run().getRange({ start: 0, end: 1 })
      results.forEach(result => {
        data.body.id = result.getValue({ name: WEINFUSE.FIELDS.INTERNALID })
        data.body.we_in = result.getValue({ name: WEINFUSE.FIELDS.WE_INFUSE })
        data.body.line_id = result.getValue({ name: WEINFUSE.FIELDS.LINE_ID })
        data.body.po = result.getValue({ name: WEINFUSE.FIELDS.PO }) || ''
        data.body.to = result.getValue({ name: WEINFUSE.FIELDS.TO }) || ''
        data.body.so = result.getValue({ name: WEINFUSE.FIELDS.SO }) || ''
        data.body.receipt = result.getValue({ name: WEINFUSE.FIELDS.RECEIPT }) || ''
        data.body.fulfillment = result.getValue({ name: WEINFUSE.FIELDS.FULFILLMENT }) || ''
        data.body.status = result.getValue({ name: WEINFUSE.FIELDS.STATUS }) || ''
        data.body.statusStage = Number(result.getValue({ name: WEINFUSE.FIELDS.STATUS_STAGE })) || 0
        data.body.receipt = result.getValue({ name: WEINFUSE.FIELDS.RECEIPT }) || ''
        data.body.fulfillment = result.getValue({ name: WEINFUSE.FIELDS.FULFILLMENT }) || ''
      })
    } catch (err) {
      log.error('Error on searchWeInfuseRecord', err)
    }
    return data
  }

  /**
   * The function creates a new record in NetSuite's WEINFUSE record type with data provided in the
   * input parameter.
   * @returns The function `createWeInfuseRecord` returns the ID of the newly created record in
   * NetSuite.
   */
  const createWeInfuseRecord = (data, jsonOrg) => {
    try {
      const { WEINFUSE } = RECORDS
      const { FIELDS } = WEINFUSE
      const objRecord = record.create({ type: WEINFUSE.ID, isDynamic: true })
      Object.entries(data).forEach(([fieldId, value]) => {
        if ([FIELDS.DATE, FIELDS.EXPIRATION, FIELDS.DATE_RECEIVED].includes(fieldId)) {
          if (value) {
            value = formatDate(value)
            // log.debug('Object.entries | value:', value)
          }
        }
        objRecord.setValue({
          fieldId,
          value,
          ignoreFieldChange: false
        })
      })
      objRecord.setValue({ fieldId: FIELDS.OBJECT_LINE, value: JSON.stringify(jsonOrg), ignoreFieldChange: false })

      const recordId = objRecord.save()
      return recordId
    } catch (err) {
      log.error('Error on createWeInfuseRecord', err)
    }
  }

  /**
   * The function updates a record in the WEINFUSE table with the given ID and values.
   * @param {string|number} id - The id parameter is the internal id of the record that needs to be updated in
   * NetSuite.
   * @param {object} values - The `values` parameter is an object that contains the field values to be updated
   * for a specific record in NetSuite. The keys of the object represent the field IDs and the values
   * represent the new values to be set for those fields.
   * @returns The function `updateWeInfuseRecord` returns the `recordId` of the updated record if
   * successful, or `null` if there was an error.
   */
  const updateWeInfuseRecord = (id, values) => {
    try {
      const { WEINFUSE } = RECORDS
      Object.entries(values).forEach(([fieldId, value]) => {
        // log.debug('for format', [WEINFUSE.FIELDS.DATE, WEINFUSE.FIELDS.EXPIRATION, WEINFUSE.FIELDS.DATE_RECEIVED])
        if ([WEINFUSE.FIELDS.DATE, WEINFUSE.FIELDS.EXPIRATION, WEINFUSE.FIELDS.DATE_RECEIVED].includes(fieldId)) {
          if (value) {
            // log.debug('date prev', value)
            value = moment(new Date(value)).format('MM/DD/YYYY')
            // log.debug('Object.entries | value:', value)
          }
        }
      })
      const recordId = record.submitFields({
        type: WEINFUSE.ID,
        id,
        values,
        options: {
          enablesourcing: true,
          ignoreMandatoryFields: true
        }
      })
      return recordId
    } catch (err) {
      log.error('updateWeInfuseRecord | err:', { id, values, err })
      log.error('Error on updateWeInfuseRecord', err)
      return null
    }
  }

  /**
   * The function updates a record with new values and returns the updated record ID, while handling
   * errors with logging.
   * @param type - The type of record being updated (e.g. customer, sales order, invoice, etc.).
   * @param id - The `id` parameter in the `updateRecord` function is a unique identifier for the
   * record that needs to be updated. It is used to specify which record to update in the NetSuite
   * system.
   * @param values - The `values` parameter is an object that contains the field/value pairs to be
   * updated for the record identified by `id`. Each key in the object represents the internal ID of a
   * field on the record, and the corresponding value is the new value to be set for that field. For
   * example:
   * @returns The function `updateRecord` returns the `recordId` of the updated record if the update is
   * successful. If there is an error, it logs an error message and does not return anything.
   */
  const updateRecord = (type, id, values) => {
    try {
      const recordId = record.submitFields({ type, id, values })
      return recordId
    } catch (err) {
      log.error('Error on updateRecord', err)
    }
  }

  return { getInputData, map, reduce, summarize }
})
