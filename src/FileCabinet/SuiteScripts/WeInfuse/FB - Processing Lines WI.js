/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/config', 'N/log', 'N/record', 'N/runtime', 'N/search', './FB - Const Lib.js', './extras/moment.js'], /**
 * @param{config} config
 * @param{log} log
 * @param{record} record
 * @param{runtime} runtime
 * @param{search} search
 * @param{constLib} constLib
 * @param{moment} moment
 */ (config, log, record, runtime, search, constLib, moment) => {
  const { RECORDS, LIST, TRANSACTIONS, STATUS, ERRORS } = constLib
  const MAIN_CONFIG = {}
  const ALL_STATUS = []
  const ALL_ERRORS = []
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
    try {
      const { WEINFUSE } = RECORDS
      const removeColumns = [WEINFUSE.FIELDS.TRANID, WEINFUSE.FIELDS.STAGING]
      const columns = Object.values(WEINFUSE.FIELDS)
        .filter(column => !removeColumns.includes(column))
        .map(column => ({ name: column }))
      const objScript = runtime.getCurrentScript()
      const filters = [[WEINFUSE.FIELDS.STATUS_STAGE, search.Operator.ANYOF, 2], 'AND', [WEINFUSE.FIELDS.ISINACTIVE, search.Operator.IS, 'F']]
      const poNumber = objScript.getParameter({ name: 'custscript_po_number' })
      if (poNumber) {
        filters.push('AND')
        filters.push([
          `formulatext:{${WEINFUSE.FIELDS.PO_NUMBER}}`,
          search.Operator.CONTAINS,
          poNumber.indexOf(' ') !== -1 ? poNumber.replace(' ', '%') : poNumber
        ])
      }
      const objSearch = search.create({
        type: WEINFUSE.ID,
        filters,
        columns
      })
      const countResults = objSearch.runPaged().count
      log.debug('getInputData | countResults:', countResults)
      const data = []
      if (countResults) {
        if (countResults < 1000) {
          objSearch.run().each(function (result) {
            const line = {}
            columns.forEach(column => {
              line[column.name] = result.getValue(column)
            })
            data.push(line)
            return true
          })
        } else {
          const pagedData = objSearch.runPaged({ pageSize: 1000 })
          pagedData.pageRanges.forEach(pageRange => {
            const page = pagedData.fetch({ index: pageRange.index })
            page.data.forEach(result => {
              const line = {}
              columns.forEach(column => {
                line[column.name] = result.getValue(column)
              })
              data.push(line)
            })
          })
        }
      }
      return data
    } catch (err) {
      log.error('Error on getInputData', err)
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
      const { WEINFUSE } = RECORDS
      let { value } = mapContext
      value = JSON.parse(value)
      value[WEINFUSE.FIELDS.QUANTITY] = Number(value[WEINFUSE.FIELDS.QUANTITY])
      value[WEINFUSE.FIELDS.PRICE] = Number(value[WEINFUSE.FIELDS.PRICE])
      mapContext.write({
        key: value[WEINFUSE.FIELDS.PO_NUMBER],
        value
      })
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
      const { WEINFUSE } = RECORDS
      const { PURCHASE_ORDER, SALES_ORDER, TRANSFER_ORDER } = TRANSACTIONS
      getConfig()
      getStatusWeInfuse()
      getErrorsWeInfuse()
      const { key, values } = reduceContext
      log.debug('reduce | key:', key)
      log.debug('reduce | values:', values)
      const data = values.map(line => JSON.parse(line))
      const dataGrouped = groupedMap(data, WEINFUSE.FIELDS.NDC)
      const dataItems = summaryData(dataGrouped)
      const filtersTransaction = [
        {
          fieldId: 'custbody_tkio_wi_purchase_number',
          operator: search.Operator.IS,
          value: MAIN_CONFIG[RECORDS.CONFIG.FIELDS.PREFIX] + data[0][WEINFUSE.FIELDS.PO_NUMBER]
        },
        { fieldId: 'mainline', operator: search.Operator.IS, value: 'T' },
        { fieldId: 'type', operator: search.Operator.ANYOF, value: ['SalesOrd', 'PurchOrd', 'TrnfrOrd'] }
      ]
      const objTran = searchExistTransaction(filtersTransaction)
      const filtersWeInfuse = [[WEINFUSE.FIELDS.LINE_ID, search.Operator.IS, data[0][WEINFUSE.FIELDS.LINE_ID]]]
      const wiRecord = searchWeInfuseRecord(filtersWeInfuse)
      if (objTran.exist === false && wiRecord.count > 0) {
        const updateFields = {}
        const processedData = { recordId: '', type: '', success: false, errors: '' }
        if (data[0][WEINFUSE.FIELDS.VENDOR_NAME] === MAIN_CONFIG[RECORDS.CONFIG.FIELDS.VENDOR]) {
          // HEALIX the Vendor
          if (data[0][WEINFUSE.FIELDS.CUSTOMER_CATEGORY] !== MAIN_CONFIG[RECORDS.CONFIG.FIELDS.IS_CORE]) {
            // NO CORE
            const subsidiaryLocation = getSubsidiaryLocation(MAIN_CONFIG[RECORDS.CONFIG.FIELDS.SOURCE_LOCATION])
            log.debug('reduce ~ subsidiaryLocation:', subsidiaryLocation)
            const subsidiaryCustomer = getSubsidiaryCustomer(data[0][WEINFUSE.FIELDS.CUSTOMER])
            log.debug('reduce ~ subsidiaryCustomer:', subsidiaryCustomer)
            if (subsidiaryLocation.text && subsidiaryCustomer.text && subsidiaryLocation.text.toString() !== subsidiaryCustomer.text.toString()) {
              const main = {
                [TRANSFER_ORDER.FIELDS.MAIN.TO_SUBSIDIARY]: data[0][WEINFUSE.FIELDS.SUBSIDIARY],
                [TRANSFER_ORDER.FIELDS.MAIN.SUBSIDIARY]: subsidiaryLocation.id,
                [TRANSFER_ORDER.FIELDS.MAIN.TRANID]: data[0]?.[WEINFUSE.FIELDS.PO_NUMBER]
                  ? MAIN_CONFIG[RECORDS.CONFIG.FIELDS.PREFIX] + data[0]?.[WEINFUSE.FIELDS.PO_NUMBER]
                  : '',
                [TRANSFER_ORDER.FIELDS.MAIN.FROMLOCATION]: MAIN_CONFIG[RECORDS.CONFIG.FIELDS.SOURCE_LOCATION],
                [TRANSFER_ORDER.FIELDS.MAIN.TOLOCATION]: data[0]?.[WEINFUSE.FIELDS.LOCATION],

                [TRANSFER_ORDER.FIELDS.MAIN.TRANDATE]: formatDate(data[0][WEINFUSE.FIELDS.DATE]),
                [TRANSFER_ORDER.FIELDS.MAIN.PURCHASE_NUMBER]: data[0]?.[WEINFUSE.FIELDS.PO_NUMBER]
                  ? MAIN_CONFIG[RECORDS.CONFIG.FIELDS.PREFIX] + data[0]?.[WEINFUSE.FIELDS.PO_NUMBER]
                  : '',
                [TRANSFER_ORDER.FIELDS.MAIN.CUSTOMER]: data[0][WEINFUSE.FIELDS.CUSTOMER],
                [TRANSFER_ORDER.FIELDS.MAIN.INCOTERM]: MAIN_CONFIG[RECORDS.CONFIG.FIELDS.INCOTERM]
              }
              const items = dataItems.map(line => {
                /** Match to units type accord by quantity item */
                const unitType = assignUnitsType(
                  line[WEINFUSE.FIELDS.UNIT_TYPE],
                  line[WEINFUSE.FIELDS.PURCHASE_UNIT],
                  line[WEINFUSE.FIELDS.PURCHASE_UNIT_ID],
                  line[WEINFUSE.FIELDS.QUANTITY]
                )
                let units = ''
                let quantity = 0
                if (unitType.err) {
                  quantity = line[WEINFUSE.FIELDS.QUANTITY]
                } else {
                  units = unitType.data.id
                  quantity = unitType.data?.quantity || line[WEINFUSE.FIELDS.QUANTITY]
                }
                return {
                  [TRANSFER_ORDER.FIELDS.LIST.ITEM]: line[WEINFUSE.FIELDS.ITEM],
                  [TRANSFER_ORDER.FIELDS.LIST.QUANTITY]: quantity,
                  [TRANSFER_ORDER.FIELDS.LIST.AMOUNT]: line[WEINFUSE.FIELDS.PRICE],
                  [TRANSFER_ORDER.FIELDS.LIST.UNITS]: units
                }
              })
              processedData.recordId = createTransaction(record.Type.INTER_COMPANY_TRANSFER_ORDER, 'item', { main, items })
              processedData.type = record.Type.TRANSFER_ORDER
            } else {
              const main = {
                [TRANSFER_ORDER.FIELDS.MAIN.SUBSIDIARY]: data[0][WEINFUSE.FIELDS.SUBSIDIARY],
                [TRANSFER_ORDER.FIELDS.MAIN.TRANID]: data[0]?.[WEINFUSE.FIELDS.PO_NUMBER]
                  ? MAIN_CONFIG[RECORDS.CONFIG.FIELDS.PREFIX] + data[0][WEINFUSE.FIELDS.PO_NUMBER]
                  : '',
                [TRANSFER_ORDER.FIELDS.MAIN.FROMLOCATION]: MAIN_CONFIG[RECORDS.CONFIG.FIELDS.SOURCE_LOCATION],
                [TRANSFER_ORDER.FIELDS.MAIN.TOLOCATION]: data[0]?.[WEINFUSE.FIELDS.LOCATION],

                [TRANSFER_ORDER.FIELDS.MAIN.TRANDATE]: formatDate(data[0][WEINFUSE.FIELDS.DATE]),
                [TRANSFER_ORDER.FIELDS.MAIN.PURCHASE_NUMBER]: data[0]?.[WEINFUSE.FIELDS.PO_NUMBER]
                  ? MAIN_CONFIG[RECORDS.CONFIG.FIELDS.PREFIX] + data[0][WEINFUSE.FIELDS.PO_NUMBER]
                  : '',
                [TRANSFER_ORDER.FIELDS.MAIN.CUSTOMER]: data[0][WEINFUSE.FIELDS.CUSTOMER]
              }
              const items = dataItems.map(line => {
                /** Match to units type accord by quantity item */
                const unitType = assignUnitsType(
                  line[WEINFUSE.FIELDS.UNIT_TYPE],
                  line[WEINFUSE.FIELDS.PURCHASE_UNIT],
                  line[WEINFUSE.FIELDS.PURCHASE_UNIT_ID],
                  line[WEINFUSE.FIELDS.QUANTITY]
                )
                let units = ''
                let quantity = 0
                if (unitType.err) {
                  quantity = line[WEINFUSE.FIELDS.QUANTITY]
                } else {
                  units = unitType.data.id
                  quantity = unitType.data?.quantity || line[WEINFUSE.FIELDS.QUANTITY]
                }
                return {
                  [TRANSFER_ORDER.FIELDS.LIST.ITEM]: line[WEINFUSE.FIELDS.ITEM],
                  [TRANSFER_ORDER.FIELDS.LIST.QUANTITY]: quantity,
                  [TRANSFER_ORDER.FIELDS.LIST.AMOUNT]: line[WEINFUSE.FIELDS.PRICE],
                  [TRANSFER_ORDER.FIELDS.LIST.UNITS]: units,
                  [TRANSFER_ORDER.FIELDS.LIST.LINE_ID]: line[WEINFUSE.FIELDS.LINE_ID]
                }
              })
              processedData.recordId = createTransaction(record.Type.TRANSFER_ORDER, 'item', { main, items })
              processedData.type = record.Type.TRANSFER_ORDER
            }
            if (processedData.recordId) {
              processedData.success = true
              updateFields[WEINFUSE.FIELDS.TO] = processedData.recordId
            }
          } else {
            // IS CORE
            const main = {
              [SALES_ORDER.FIELDS.MAIN.TRANID]: data[0]?.[WEINFUSE.FIELDS.PO_NUMBER]
                ? MAIN_CONFIG[RECORDS.CONFIG.FIELDS.PREFIX] + data[0][WEINFUSE.FIELDS.PO_NUMBER]
                : '',
              [SALES_ORDER.FIELDS.MAIN.ENTITY]: data[0][WEINFUSE.FIELDS.CUSTOMER],
              [SALES_ORDER.FIELDS.MAIN.LOCATION]: data[0]?.[WEINFUSE.FIELDS.LOCATION],
              [SALES_ORDER.FIELDS.MAIN.TRANDATE]: formatDate(data[0][WEINFUSE.FIELDS.DATE]),
              [SALES_ORDER.FIELDS.MAIN.WI_PURCHASE_NUMBER]: data[0]?.[WEINFUSE.FIELDS.PO_NUMBER]
                ? MAIN_CONFIG[RECORDS.CONFIG.FIELDS.PREFIX] + data[0][WEINFUSE.FIELDS.PO_NUMBER]
                : '',
              [SALES_ORDER.FIELDS.MAIN.SHIPTOADRESSLIST]: data[0][WEINFUSE.FIELDS.SHIP_TO]
            }
            const items = dataItems.map(line => {
              /** Match to units type accord by quantity item */
              const unitType = assignUnitsType(
                line[WEINFUSE.FIELDS.UNIT_TYPE],
                line[WEINFUSE.FIELDS.PURCHASE_UNIT],
                line[WEINFUSE.FIELDS.PURCHASE_UNIT_ID],
                line[WEINFUSE.FIELDS.QUANTITY]
              )
              let units = ''
              let quantity = 0
              if (unitType.err) {
                quantity = line[WEINFUSE.FIELDS.QUANTITY]
              } else {
                units = unitType.data.id
                quantity = unitType.data?.quantity || line[WEINFUSE.FIELDS.QUANTITY]
              }
              return {
                [SALES_ORDER.FIELDS.LIST.ITEM]: line[WEINFUSE.FIELDS.ITEM],
                [SALES_ORDER.FIELDS.LIST.QUANTITY]: quantity,
                [SALES_ORDER.FIELDS.LIST.AMOUNT]: line[WEINFUSE.FIELDS.PRICE],
                [SALES_ORDER.FIELDS.LIST.UNITS]: units,
                [TRANSACTIONS.SALES_ORDER.FIELDS.LIST.CREATEDPO]: '',
                [TRANSACTIONS.SALES_ORDER.FIELDS.LIST.POVENDOR]: '',
                [SALES_ORDER.FIELDS.LIST.LINE_ID]: line[WEINFUSE.FIELDS.LINE_ID]
              }
            })
            processedData.recordId = createTransaction(record.Type.SALES_ORDER, 'item', { main, items })
            processedData.type = record.Type.SALES_ORDER
            if (processedData.recordId) {
              processedData.success = true
              updateFields[WEINFUSE.FIELDS.SO] = processedData.recordId
            }
          }
        } else {
          // NO VENDOR HEALIX
          // IS CORE
          if (data[0][WEINFUSE.FIELDS.CUSTOMER_CATEGORY] === MAIN_CONFIG[RECORDS.CONFIG.FIELDS.IS_CORE]) {
            const main = {
              ['vendor']: data[0][WEINFUSE.FIELDS.VENDOR],
              [SALES_ORDER.FIELDS.MAIN.TRANID]: data[0]?.[WEINFUSE.FIELDS.PO_NUMBER]
                ? MAIN_CONFIG[RECORDS.CONFIG.FIELDS.PREFIX] + data[0][WEINFUSE.FIELDS.PO_NUMBER]
                : '',
              [SALES_ORDER.FIELDS.MAIN.ENTITY]: data[0][WEINFUSE.FIELDS.CUSTOMER],
              [SALES_ORDER.FIELDS.MAIN.LOCATION]: data[0]?.[WEINFUSE.FIELDS.LOCATION],
              [SALES_ORDER.FIELDS.MAIN.TRANDATE]: formatDate(data[0][WEINFUSE.FIELDS.DATE]),
              [SALES_ORDER.FIELDS.MAIN.WI_PURCHASE_NUMBER]: data[0]?.[WEINFUSE.FIELDS.PO_NUMBER]
                ? MAIN_CONFIG[RECORDS.CONFIG.FIELDS.PREFIX] + data[0][WEINFUSE.FIELDS.PO_NUMBER]
                : '',
              [SALES_ORDER.FIELDS.MAIN.SHIPTOADRESSLIST]: data[0][WEINFUSE.FIELDS.SHIP_TO]
            }
            const items = dataItems.map(line => {
              /** Match to units type accord by quantity item */
              const unitType = assignUnitsType(
                line[WEINFUSE.FIELDS.UNIT_TYPE],
                line[WEINFUSE.FIELDS.PURCHASE_UNIT],
                line[WEINFUSE.FIELDS.PURCHASE_UNIT_ID],
                line[WEINFUSE.FIELDS.QUANTITY]
              )
              let units = ''
              let quantity = 0
              if (unitType.err) {
                quantity = line[WEINFUSE.FIELDS.QUANTITY]
              } else {
                units = unitType.data.id
                quantity = unitType.data?.quantity || line[WEINFUSE.FIELDS.QUANTITY]
              }
              return {
                [SALES_ORDER.FIELDS.LIST.ITEM]: line[WEINFUSE.FIELDS.ITEM],
                [SALES_ORDER.FIELDS.LIST.QUANTITY]: quantity,
                [SALES_ORDER.FIELDS.LIST.AMOUNT]: line[WEINFUSE.FIELDS.PRICE],
                [SALES_ORDER.FIELDS.LIST.UNITS]: units,
                [SALES_ORDER.FIELDS.LIST.LINE_ID]: line[WEINFUSE.FIELDS.LINE_ID]
              }
            })
            const getShipToAccount = getShipAccount(
              main[SALES_ORDER.FIELDS.MAIN.SHIPTOADRESSLIST],
              main[SALES_ORDER.FIELDS.MAIN.ENTITY],
              main['vendor']
            )
            if (getShipToAccount.success === true) {
              processedData.recordId = createTransaction(record.Type.SALES_ORDER, 'item', { main, items })
              processedData.type = record.Type.SALES_ORDER
              if (processedData.recordId) {
                processedData.success = true
                updateFields[WEINFUSE.FIELDS.SO] = processedData.recordId
              }
            } else {
              if (getShipToAccount.count === 0) {
                processedData.errors = assignErrors(ERRORS.SHIP_ACCOUNT_NOT_FOUND)
              } else {
                processedData.errors = assignErrors(ERRORS.SHIP_ACCOUNT_DUPLICATED)
              }
            }
          } else {
            // NO CORE
            const main = {
              [PURCHASE_ORDER.FIELDS.MAIN.TRANID]: data[0]?.[WEINFUSE.FIELDS.PO_NUMBER]
                ? MAIN_CONFIG[RECORDS.CONFIG.FIELDS.PREFIX] + data[0][WEINFUSE.FIELDS.PO_NUMBER]
                : '',
              [PURCHASE_ORDER.FIELDS.MAIN.ENTITY]: data[0][WEINFUSE.FIELDS.VENDOR],
              [PURCHASE_ORDER.FIELDS.MAIN.SHIPTO]: data[0][WEINFUSE.FIELDS.CUSTOMER],
              [PURCHASE_ORDER.FIELDS.MAIN.SUBSIDIARY]: data[0][WEINFUSE.FIELDS.SUBSIDIARY],
              [PURCHASE_ORDER.FIELDS.MAIN.TRANDATE]: formatDate(data[0][WEINFUSE.FIELDS.DATE]),
              [PURCHASE_ORDER.FIELDS.MAIN.WI_PURCHASE_NUMBER]: data[0]?.[WEINFUSE.FIELDS.PO_NUMBER]
                ? MAIN_CONFIG[RECORDS.CONFIG.FIELDS.PREFIX] + data[0][WEINFUSE.FIELDS.PO_NUMBER]
                : '',
              [PURCHASE_ORDER.FIELDS.MAIN.LOCATION]: data.find(x => x[WEINFUSE.FIELDS.LOCATION] !== '' && x[WEINFUSE.FIELDS.LOCATION] !== null)?.[
                WEINFUSE.FIELDS.LOCATION
              ],
              [PURCHASE_ORDER.FIELDS.MAIN.SHIPTOADRESSLIST]: data[0][WEINFUSE.FIELDS.SHIP_TO]
            }
            const items = dataItems.map(line => {
              /** Match to units type accord by quantity item */
              const unitType = assignUnitsType(
                line[WEINFUSE.FIELDS.UNIT_TYPE],
                line[WEINFUSE.FIELDS.PURCHASE_UNIT],
                line[WEINFUSE.FIELDS.PURCHASE_UNIT_ID],
                line[WEINFUSE.FIELDS.QUANTITY]
              )
              let units = ''
              let quantity = 0
              if (unitType.err) {
                quantity = line[WEINFUSE.FIELDS.QUANTITY]
              } else {
                units = unitType.data.id
                quantity = unitType.data?.quantity || line[WEINFUSE.FIELDS.QUANTITY]
              }
              return {
                [PURCHASE_ORDER.FIELDS.LIST.ITEM]: line[WEINFUSE.FIELDS.ITEM],
                [PURCHASE_ORDER.FIELDS.LIST.QUANTITY]: quantity,
                [PURCHASE_ORDER.FIELDS.LIST.AMOUNT]: line[WEINFUSE.FIELDS.PRICE],
                [PURCHASE_ORDER.FIELDS.LIST.UNITS]: units
                // [PURCHASE_ORDER.FIELDS.LIST.LINE_ID]: line[WEINFUSE.FIELDS.LINE_ID]
              }
            })
            // Consult the ship to account
            const getShipToAccount = getShipAccount(
              main[PURCHASE_ORDER.FIELDS.MAIN.SHIPTOADRESSLIST],
              main[PURCHASE_ORDER.FIELDS.MAIN.SHIPTO],
              main[PURCHASE_ORDER.FIELDS.MAIN.ENTITY]
            )
            log.debug({title: 'Data to account:', details: { SHIPTOADRESSLIST: main[PURCHASE_ORDER.FIELDS.MAIN.SHIPTOADRESSLIST],SHIPTO: main[PURCHASE_ORDER.FIELDS.MAIN.SHIPTO],ENTITY: main[PURCHASE_ORDER.FIELDS.MAIN.ENTITY]} });
            log.debug({title: 'getShipToAccount', details: getShipToAccount});
            if (getShipToAccount.success === true) {
              processedData.recordId = createTransaction(record.Type.PURCHASE_ORDER, 'item', { main, items })
              processedData.type = record.Type.PURCHASE_ORDER
              if (processedData.recordId) {
                processedData.success = true
                updateFields[WEINFUSE.FIELDS.PO] = processedData.recordId
              }
            } else {
              if (getShipToAccount.count === 0) {
                processedData.errors = assignErrors(ERRORS.SHIP_ACCOUNT_NOT_FOUND)
              } else {
                processedData.errors = assignErrors(ERRORS.SHIP_ACCOUNT_DUPLICATED)
              }
            }
          }
        }
        log.audit('reduce ~ processedData:', processedData)
        if (processedData.success && processedData.recordId) {
          updateFields[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.PROCESSED)
          reduceContext.write({ key, value: { success: true, id: processedData.recordId } })
        } else {
          if (processedData.errors) {
            reduceContext.write({ key, value: { success: false, id: '' } })
            updateFields[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.ERROR)
            updateFields[WEINFUSE.FIELDS.ERRORS] = processedData.errors
          } else {
            updateFields[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.ERROR)
            updateFields[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.ERROR)
          }
        }
        data.forEach(line => {
          if (updateFields[WEINFUSE.FIELDS.STATUS] === assignStatus(STATUS.PROCESSED)) {
            updateFields[WEINFUSE.FIELDS.ON_ERROR] = false
            updateFields[WEINFUSE.FIELDS.ERRORS] = ''
            if (line[WEINFUSE.FIELDS.DATE_RECEIVED] && line[WEINFUSE.FIELDS.LOT] && line[WEINFUSE.FIELDS.EXPIRATION]) {
              updateFields[WEINFUSE.FIELDS.STATUS_STAGE] = 3
            } else {
              updateFields[WEINFUSE.FIELDS.STATUS_STAGE] = 1
            }
          }
          updateWeInfuseRecord(line[WEINFUSE.FIELDS.INTERNALID], updateFields)
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
      let counterFails = 0
      output.iterator().each((key, value) => {
        const line = JSON.parse(value)
        if (line.success === true && line.id !== '') {
          counterCreates++
        } else {
          counterFails++
        }
        return true
      })
      log.audit('Results', `PO's resume, creates: ${counterCreates}, fails: ${counterFails}`)
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

  /**
   * The function `assignErrors` takes an event as input and returns the internal ID associated with
   * that event, or an empty string if no ID is found.
   * @returns The function `assignErrors` returns the value of `id[WEINFUSE_ERRORS.FIELDS.INTERNALID]`
   * if `id` is truthy and has a property `WEINFUSE_ERRORS.FIELDS.INTERNALID`. Otherwise, it returns an
   * empty string.
   */
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
   * The `groupedMap` function takes an initial array and a key, and returns an object where the keys
   * are unique values from the array's objects' `key` property, and the values are arrays of objects
   * with the same `key` value.
   * @param initialArray - The initialArray parameter is an array of objects that you want to group
   * based on a specific key. Each object in the array should have a property with the key you want to
   * group by.
   * @param key - The `key` parameter is a string that represents the property name in each object of
   * the `initialArray` that will be used to group the objects.
   * @returns The `groupedMap` function returns an object where the keys are the unique values of the
   * `key` property in the `initialArray`, and the values are arrays of objects from the `initialArray`
   * that have the same `key` value.
   */
  const groupedMap = (initialArray, key) => {
    try {
      return initialArray.reduce(
        (result, item) => ({
          ...result,
          [item[key]]: [...(result[item[key]] || []), item]
        }),
        {}
      )
    } catch (err) {
      log.error('Error on groupedMap', err)
    }
  }

  /**
   * The function `summaryData` takes in a data object and returns a new array of values, where
   * duplicate items are combined and their quantities are summed.
   * @returns The function `summaryData` returns an array of `newValues`.
   */
  const summaryData = data => {
    const newValues = []
    try {
      const { WEINFUSE } = RECORDS
      Object.values(data).forEach(items => {
        items.forEach(item => {
          /* const findItem = newValues.findIndex(
            i => i[WEINFUSE.FIELDS.ITEM] === item[WEINFUSE.FIELDS.ITEM] && i[WEINFUSE.FIELDS.LOT] === item[WEINFUSE.FIELDS.LOT]
          ) */
          const findItem = newValues.findIndex(i => i[WEINFUSE.FIELDS.ITEM] === item[WEINFUSE.FIELDS.ITEM])
          if (findItem !== -1) {
            newValues[findItem][WEINFUSE.FIELDS.QUANTITY] += Number(item[WEINFUSE.FIELDS.QUANTITY])
            newValues[findItem][WEINFUSE.FIELDS.PRICE] += Number(item[WEINFUSE.FIELDS.PRICE])
          } else {
            newValues.push(item)
          }
        })
      })
    } catch (err) {
      log.error('Error on summaryData', err)
    }
    return newValues
  }

  /**
   * The function `searchExistTransaction` searches for existing transactions based on the provided
   * filters and returns whether a transaction exists and its ID.
   * @returns The function `searchExistTransaction` returns an object with two properties: `exist` and
   * `id`. The `exist` property indicates whether a transaction exists based on the search filters, and
   * the `id` property contains the internal ID of the first matching transaction if it exists.
   */
  const searchExistTransaction = filters => {
    const response = { exist: false, id: '', type: '', status: '' }
    try {
      const customFilters = []
      filters.forEach(filter => {
        if (customFilters.length) {
          customFilters.push('AND')
        }

        customFilters.push([filter.fieldId, filter.operator, filter.value])
      })
      const objSearch = search.create({
        type: search.Type.TRANSACTION,
        filters: customFilters,
        columns: [
          search.createColumn({ name: 'datecreated', sort: search.Sort.DESC }),
          search.createColumn({ name: 'internalid' }),
          search.createColumn({ name: 'tranid' }),
          search.createColumn({ name: 'type' }),
          search.createColumn({ name: 'status' })
        ]
      })
      const result = objSearch.run().getRange({ start: 0, end: 1 })
      if (result.length > 0) {
        response.exist = true
        response.id = result[0].getValue({ name: 'internalid' })
        response.type = result[0].getValue({ name: 'type' })
        response.status = result[0].getValue({ name: 'status' }) || ''
      }
    } catch (err) {
      log.error('Error on searchExistTransaction', err)
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
   * The function `getSubsidiaryLocation` retrieves the subsidiary location associated with a given
   * location ID.
   * @param [location] - The `location` parameter is a string that represents the ID of a location
   * record in NetSuite.
   * @returns The function `getSubsidiaryLocation` returns the value of the subsidiary associated with
   * the given location. If the location is empty or not found, it returns an empty string.
   */
  const getSubsidiaryLocation = (location = '') => {
    const response = { id: '', text: '' }
    try {
      if (location === '') return response
      const { LOCATION } = RECORDS
      const objSearch = search.lookupFields({
        type: search.Type.LOCATION,
        id: location,
        columns: [LOCATION.FIELDS.SUBSIDIARY]
      })
      if (objSearch?.[LOCATION.FIELDS.SUBSIDIARY]) {
        response.text = objSearch[LOCATION.FIELDS.SUBSIDIARY]
      }
      if (response.text.indexOf(': ') !== -1) {
        response.text = response.text.split(': ')[1]
      }
      const subsidiarySearch = search.create({
        type: search.Type.SUBSIDIARY,
        filters: [['isinactive', search.Operator.IS, 'F'], 'AND', ['name', search.Operator.CONTAINS, response.text]],
        columns: [{ name: 'internalid' }]
      })
      const results = subsidiarySearch.run().getRange({ start: 0, end: 1 })
      if (results.length !== 0) {
        response.id = results[0].getValue({ name: 'internalid' })
      }
    } catch (err) {
      log.error('Error on getSubsidiaryLocation', err)
    }
    return response
  }

  /**
   * The function `getSubsidiaryCustomer` retrieves the subsidiary of a customer based on their ID.
   * @param [customer] - The `customer` parameter is a string that represents the ID of a customer
   * record.
   * @returns The function `getSubsidiaryCustomer` returns the value of the `response` variable, which
   * is initially an empty string. If the `customer` parameter is not provided or is falsy, the
   * function will return an empty string. Otherwise, it will perform a search for the subsidiary of
   * the customer using the `search.lookupFields` method. If a subsidiary is found, the function will
   */
  const getSubsidiaryCustomer = (customer = '') => {
    const response = { id: '', text: '' }
    try {
      if (!customer) return response
      const { CUSTOMER } = RECORDS
      const objSearch = search.lookupFields({
        type: search.Type.CUSTOMER,
        id: customer,
        columns: [CUSTOMER.FIELDS.SUBSIDIARY]
      })
      if (objSearch?.[CUSTOMER.FIELDS.SUBSIDIARY]) {
        response.text = objSearch[CUSTOMER.FIELDS.SUBSIDIARY][0].text
        response.id = objSearch[CUSTOMER.FIELDS.SUBSIDIARY][0].value
      }
      if (response.text.indexOf(': ') !== -1) {
        response.text = response.text.split(': ')[1]
      }
    } catch (err) {
      log.error('Error on getSubsidiaryCustomer', err)
    }
    return response
  }

  /**
   * The function `assignUnitsType` searches for a specific unit type based on unit ID, unit name, and
   * quantity, and returns the result along with any errors or messages.
   * @param unit - The `unit` parameter is the internal ID of the unit type you want to search for.
   * @param purchase - The `purchase` parameter is the name of the unit type that you want to search
   * for.
   * @param quantity - The `quantity` parameter represents the quantity of units you want to search
   * for.
   * @returns The function `searchUnitsType` returns an object with the following properties:
   */
  const assignUnitsType = (unit = '', purchase = '', purchaseunit, quantity) => {
    const response = { count: 0, data: {}, err: false, msg: '' }
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
        const results = objSearch.run().getRange({ start: 0, end: 1 })
        results.forEach(result => {
          const conversionrate = result.getValue({ name: UNIT_TYPE.FIELDS.CONVERSIONRATE })
          if (quantity % conversionrate === 0) {
            response.data.id = purchaseunit
            response.data.quantity = quantity / conversionrate
          } else {
            response.err = true
            response.msg = 'The division of the quantity returned a decimal number'
          }
        })
      } else {
        response.err = true
        response.msg = 'Unit type not found'
      }
    } catch (err) {
      log.error('Error on assignUnitsType', err)
      response.err = true
      response.msg = 'Error on assign unit type'
    }
    return response
  }

  /**
   * The function `getShipAccount` retrieves the Ship to Account number based on the provided address,
   * customer, and vendor.
   * @param address - The address parameter is the location of the shipment. It is used as a filter in
   * the search query to find the ship to account associated with that address.
   * @param customer - The `customer` parameter is used to filter the search results based on the
   * customer value.
   * @param vendor - The "vendor" parameter is used to filter the search results based on the vendor
   * field. It is a value that represents the vendor you want to search for in the "SHIPTO_ACCOUNT"
   * records.
   * @returns The function `getShipAccount` returns an object with the following properties:
   */
  const getShipAccount = (address, customer, vendor) => {
    log.debug({title: 'address, customer, vendor', details: {address, customer, vendor}});
    const response = { count: 0, account: '', success: false }
    try {
      if (!address || !customer || !vendor) return response
      const { SHIPTO_ACCOUNT } = RECORDS
      const filters = []
      // Create Filters, if the Address field is not empty
      if (address) {
        if (filters.length) {
          filters.push('AND')
        }
        filters.push([SHIPTO_ACCOUNT.FIELDS.LOCATION, search.Operator.IS, address])
      }

      // Create Filters, if the Customer field is not empty
      if (customer) {
        if (filters.length) {
          filters.push('AND')
        }
        filters.push([SHIPTO_ACCOUNT.FIELDS.CUSTOMER, search.Operator.IS, customer])
      }

      // Create Filters, if the Vendor field is not empty
      if (vendor) {
        if (filters.length) {
          filters.push('AND')
        }
        filters.push([SHIPTO_ACCOUNT.FIELDS.VENDOR, search.Operator.IS, vendor])
      }

      // Execute the saved search generated
      const objSearch = search.create({
        type: SHIPTO_ACCOUNT.ID,
        filters,
        columns: [search.createColumn({ name: SHIPTO_ACCOUNT.FIELDS.ACCOUNT })]
      })
      const searchResultCount = objSearch.runPaged().count
      response.count = searchResultCount
      const results = objSearch.run().getRange(0, 10)

      // Return the value of Ship to Account #
      if (results.length) {
        response.success = true
        response.account = results[0].getValue({ name: SHIPTO_ACCOUNT.FIELDS.ACCOUNT })
      }
    } catch (e) {
      log.error({ title: 'Error getShipAccountÑ', details: e })
      response.success = false
    }
    return response
  }

  /**
   * The function `createPurchaseOrder` creates a purchase order record in NetSuite using the provided
   * data.
   * @param {object} data
   * @param {object} data.main
   * @param {array} data.items
   * @returns The function `createPurchaseOrder` returns the `recordId` of the newly created purchase
   * order.
   */
  const createTransaction = (type, sublistId, data) => {
    try {
      log.debug('createTransaction ~ type:', type)
      const { main } = data
      log.debug('createTransaction ~ main:', main)
      let { items } = data
      items = items.map(item => {
        if (item.units === '') {
          delete item.units
        }
        return item
      })
      items.forEach(item => {
        log.debug('createTransaction ~ item:', item)
      })
      const objRecord = record.create({
        type,
        isDynamic: true
      })
      Object.entries(main).forEach(([fieldId, value]) => {
        switch (fieldId) {
          case 'vendor':
            break;
          default:
            objRecord.setValue({
              fieldId,
              value,
              ignoreFieldChange: false
            })
            break;
        }
      })
      items.forEach(item => {
        objRecord.selectNewLine({
          sublistId
        })
        Object.entries(item).forEach(([fieldId, value]) => {
          objRecord.setCurrentSublistValue({
            sublistId,
            fieldId,
            value,
            ignoreFieldChange: false
          })
        })
        objRecord.commitLine({
          sublistId,
          ignoreRecalc: false
        })
      })
      const recordId = objRecord.save({
        enableSourcing: true,
        ignoreMandatoryFields: true
      })
      return recordId
    } catch (err) {
      log.error('Error on createTransaction', err)
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

  return { getInputData, map, reduce, summarize }
})
