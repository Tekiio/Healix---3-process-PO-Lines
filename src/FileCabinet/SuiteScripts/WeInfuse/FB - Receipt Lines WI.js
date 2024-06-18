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
      const filters = [
        [WEINFUSE.FIELDS.STATUS_STAGE, search.Operator.ANYOF, 3],
        'AND',
        // [WEINFUSE.FIELDS.STATUS_STAGE, search.Operator.ANYOF, 1],
        // 'AND',
        [WEINFUSE.FIELDS.ISINACTIVE, search.Operator.IS, 'F'],
        'AND',
        // [
        [WEINFUSE.FIELDS.LOT, search.Operator.ISNOTEMPTY, '']
        // 'AND',
        // [WEINFUSE.FIELDS.EXPIRATION, search.Operator.ISNOTEMPTY, ''],
        // 'AND',
        // [WEINFUSE.FIELDS.DATE_RECEIVED, search.Operator.ISNOTEMPTY, '']
        // ]
      ]
      const poNumber = objScript.getParameter({ name: 'custscript_receipt_po_number' })
      if (poNumber) {
        filters.push('AND')
        filters.push([
          `formulatext:{${WEINFUSE.FIELDS.PO_NUMBER}}`,
          search.Operator.CONTAINS,
          poNumber.indexOf(' ') !== -1 ? poNumber.replace(' ', '%') : poNumber
        ])
      }
      log.debug('getInputData ~ filters:', filters)
      const objSearch = search.create({
        type: WEINFUSE.ID,
        filters,
        columns
      })
      const countResults = objSearch.runPaged().count
      log.debug('getInputData ~ countResults:', countResults)
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
      getConfig()
      getStatusWeInfuse()
      getErrorsWeInfuse()
      let { value } = mapContext
      value = JSON.parse(value)
      value[WEINFUSE.FIELDS.QUANTITY] = Number(value[WEINFUSE.FIELDS.QUANTITY])
      value[WEINFUSE.FIELDS.PRICE] = Number(value[WEINFUSE.FIELDS.PRICE])
      mapContext.write({ key: value[WEINFUSE.FIELDS.PO_NUMBER], value })
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
  var errorMap = null;
  const reduce = reduceContext => {
    try {
      // Preparing the information to process the data
      const { WEINFUSE } = RECORDS
      getConfig()
      getStatusWeInfuse()
      getErrorsWeInfuse()
      const { key, values } = reduceContext
      log.debug('reduce ~ key:', key)
      var data = values.map(line => JSON.parse(line));

      // Validation the data, to prevent the errors to generate the transaction
      const validateData = validationToData(data, WEINFUSE);
      const newPOLine = [...validateData.poLineToProcess];
      const errPOLine = [...validateData.poLineWithError];
      log.debug({title: 'newPOLine', details: {count: newPOLine.length, newPOLine}});
      log.debug({title: 'errPOLine', details: {count: errPOLine.length, errPOLine}});

      // obtain the information by transaction related 
      const filtersTransaction = [
        {
          fieldId: 'custbody_tkio_wi_purchase_number',
          operator: search.Operator.IS,
          value: MAIN_CONFIG[RECORDS.CONFIG.FIELDS.PREFIX] + data[0][WEINFUSE.FIELDS.PO_NUMBER]
        },
        { fieldId: 'mainline', operator: search.Operator.IS, value: 'T' },
        { fieldId: 'type', operator: search.Operator.ANYOF, value: ['SalesOrd', 'PurchOrd', 'TrnfrOrd'] },
        { fieldId: 'createdfrom', operator: search.Operator.ANYOF, value: '@NONE@' }
      ]
      const objTran = searchExistTransaction(filtersTransaction)
      const filtersWeInfuse = [[WEINFUSE.FIELDS.LINE_ID, search.Operator.IS, data[0][WEINFUSE.FIELDS.LINE_ID]]]
      const wiRecord = searchWeInfuseRecord(filtersWeInfuse)
      
      if (objTran.exist && wiRecord.count && newPOLine.length > 0) {
        const newPOLineToUpdate = [...newPOLine];
        // Summarize the data to process, additional the information is grouped by NDC.
        const dataGrouped = groupedMap(newPOLine, WEINFUSE.FIELDS.NDC);
        const dataItems = summaryData(dataGrouped);
        log.debug({title: 'dataItems', details: dataItems});
        const closeLines = getLinesClose(objTran.id)
        var idRecord = null;
        switch (objTran.type) {
          case 'PurchOrd':
            var groupsReceipt = groupedMap(newPOLine, WEINFUSE.FIELDS.LOT)
            Object.values(groupsReceipt).forEach(groupItems => {
              if(groupItems.length > 0){
                idRecord = createTransformRecord( record.Type.PURCHASE_ORDER, objTran.id, record.Type.ITEM_RECEIPT, groupItems, 'item', closeLines);
                groupItems.forEach((poLine)=>{
                  if (idRecord) {
                    var updateFieldsNewLine = {};
                    log.audit('reduce ~ idRecord:', idRecord)
                    updateFieldsNewLine[WEINFUSE.FIELDS.RECEIPT] = idRecord
                    updateFieldsNewLine[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.PROCESSED)
                  } else if(errorMap !== null) {
                    newPOLineToUpdate.forEach((poLine)=>{
                      var updateFieldsNewLine = {};
                      switch(errorMap){
                        case "MATCHING_SERIAL_NUM_REQD":
                          updateFieldsNewLine[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.LOT_MUST_HAVE_FULFILL)
                          break;
                        default:
                          updateFieldsNewLine[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.ERROR)
                          break;
                      }
                      updateFieldsNewLine[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.PENDING);
                      updateFieldsNewLine[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.ERROR);
                      errPOLine.push({ id: poLine[WEINFUSE.FIELDS.INTERNALID], updateFields: updateFieldsNewLine});
                    })
                  }
                  else {
                    newPOLineToUpdate.forEach((poLine)=>{
                      var updateFieldsNewLine = {};
                      updateFieldsNewLine[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.PENDING);
                      updateFieldsNewLine[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.ERROR);
                      errPOLine.push({ id: poLine[WEINFUSE.FIELDS.INTERNALID], updateFields: updateFieldsNewLine});
                    })
                  }                        
                  updateWeInfuseRecord(poLine[WEINFUSE.FIELDS.INTERNALID], updateFieldsNewLine);
                  data = data.filter((dataFilterPib)=> dataFilterPib[WEINFUSE.FIELDS.INTERNALID] !== poLine[WEINFUSE.FIELDS.INTERNALID])
                })
              }
            })
            break;
          case 'SalesOrd':
            var groupsReceipt = groupedMap(newPOLine, WEINFUSE.FIELDS.LOT)
            Object.values(groupsReceipt).forEach(groupItems => {
              if(groupItems.length > 0){
                idRecord = createTransformRecord( record.Type.PURCHASE_ORDER, objTran.id, record.Type.ITEM_RECEIPT, groupItems, 'item', closeLines);
                groupItems.forEach((poLine)=>{
                  if (idRecord) {
                    var updateFieldsNewLine = {};
                    log.audit('reduce ~ idRecord:', idRecord)
                    updateFieldsNewLine[WEINFUSE.FIELDS.RECEIPT] = idRecord
                    updateFieldsNewLine[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.PROCESSED)
                  } else if(errorMap !== null) {
                    newPOLineToUpdate.forEach((poLine)=>{
                      var updateFieldsNewLine = {};
                      switch(errorMap){
                        case "MATCHING_SERIAL_NUM_REQD":
                          updateFieldsNewLine[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.LOT_MUST_HAVE_FULFILL)
                          break;
                        default:
                          updateFieldsNewLine[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.ERROR)
                          break;
                      }
                      updateFieldsNewLine[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.PENDING);
                      updateFieldsNewLine[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.ERROR);
                      errPOLine.push({ id: poLine[WEINFUSE.FIELDS.INTERNALID], updateFields: updateFieldsNewLine});
                    })
                  }
                  else {
                    newPOLineToUpdate.forEach((poLine)=>{
                      var updateFieldsNewLine = {};
                      updateFieldsNewLine[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.PENDING);
                      updateFieldsNewLine[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.ERROR);
                      errPOLine.push({ id: poLine[WEINFUSE.FIELDS.INTERNALID], updateFields: updateFieldsNewLine});
                    })
                  }                        
                  updateWeInfuseRecord(poLine[WEINFUSE.FIELDS.INTERNALID], updateFieldsNewLine);
                  data = data.filter((dataFilterPib)=> dataFilterPib[WEINFUSE.FIELDS.INTERNALID] !== poLine[WEINFUSE.FIELDS.INTERNALID])
                })
              }
            })
            break;
          case 'TrnfrOrd': 
            idRecord = createTransformRecord(
              record.Type.TRANSFER_ORDER,
              objTran.id,
              record.Type.ITEM_RECEIPT,
              dataItems,
              'item',
              closeLines
            )
            if(idRecord !== null){
              newPOLineToUpdate.forEach((poLine)=>{
                var updateFieldsNewLine = {};
                updateFieldsNewLine[WEINFUSE.FIELDS.RECEIPT] = idRecord
                updateFieldsNewLine[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.PROCESSED);
                updateFieldsNewLine[WEINFUSE.FIELDS.ON_ERROR] = false
                updateFieldsNewLine[WEINFUSE.FIELDS.ERRORS] = ''
                updateFieldsNewLine[WEINFUSE.FIELDS.STATUS_STAGE] = 4
                updateWeInfuseRecord(poLine[WEINFUSE.FIELDS.INTERNALID], updateFieldsNewLine);
              })
            } else if(errorMap !== null) {
              newPOLineToUpdate.forEach((poLine)=>{
                var updateFieldsNewLine = {};
                switch(errorMap){
                  case "MATCHING_SERIAL_NUM_REQD":
                    updateFieldsNewLine[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.LOT_MUST_HAVE_FULFILL)
                    break;
                  default:
                    updateFieldsNewLine[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.ERROR)
                    break;
                }
                updateFieldsNewLine[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.PENDING);
                updateFieldsNewLine[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.ERROR);
                errPOLine.push({ id: poLine[WEINFUSE.FIELDS.INTERNALID], updateFields: updateFieldsNewLine});
              })
            }
            else {
              newPOLineToUpdate.forEach((poLine)=>{
                var updateFieldsNewLine = {};
                updateFieldsNewLine[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.PENDING);
                updateFieldsNewLine[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.ERROR);
                errPOLine.push({ id: poLine[WEINFUSE.FIELDS.INTERNALID], updateFields: updateFieldsNewLine});
              })
            }
            break;
        }
      }

      errPOLine.forEach((line)=>{
        if(line.updateFields[WEINFUSE.FIELDS.STATUS] === assignStatus(STATUS.PROCESSED)){
          line.updateFields[WEINFUSE.FIELDS.ON_ERROR] = false
          line.updateFields[WEINFUSE.FIELDS.ERRORS] = ''
          line.updateFields[WEINFUSE.FIELDS.STATUS_STAGE] = 4
        }
        updateWeInfuseRecord(line.id, line.updateFields);
      })
    } catch (err) {
      log.error('Error on reduce', err)
    }
  }
   const validationToData = (dataOld, WEINFUSE) =>{
    try {
      var dataIF = [], dataIR = [], poLineProcessed = [], poLineToProcess = [], poLineWithError = [], poLineToUpdate = [];
      // Obtain the transaction related by POLine
      const filtersTransaction = [
          {
            fieldId: 'custbody_tkio_wi_purchase_number',
            operator: search.Operator.IS,
            value: MAIN_CONFIG[RECORDS.CONFIG.FIELDS.PREFIX] + dataOld[0][WEINFUSE.FIELDS.PO_NUMBER]
          },
          { fieldId: 'mainline', operator: search.Operator.IS, value: 'T' },
          { fieldId: 'type', operator: search.Operator.ANYOF, value: ['SalesOrd', 'PurchOrd', 'TrnfrOrd'] },
          { fieldId: 'createdfrom', operator: search.Operator.ANYOF, value: '@NONE@' }
        ]
      const objTran = searchExistTransaction(filtersTransaction);
      const filtersWeInfuse = [[WEINFUSE.FIELDS.LINE_ID, search.Operator.IS, dataOld[0][WEINFUSE.FIELDS.LINE_ID]]]
      const wiRecord = searchWeInfuseRecord(filtersWeInfuse)
      log.debug({title: 'objTran', details: objTran});
      log.debug({title: 'wiRecord', details: wiRecord});

      if (objTran.exist && wiRecord.count) {
        // Get Data to IF, IR, PO Line
        switch (objTran.type) {
          case 'PurchOrd':{
              // Obtain data to NetSuite
              poLineProcessed = getPOLine(dataOld)
              log.debug({title: 'POLineProcessed', details: poLineProcessed[0]});
              dataIR = getIRLot(dataOld[0][WEINFUSE.FIELDS.PO]);
              var dataIRNew = [... dataIR]
              log.debug({title: 'dataIR', details: dataIR});
              dataIF = getIFLot(dataOld[0][WEINFUSE.FIELDS.PO])
              // log.debug({title: 'dataIF', details: dataIF});
  
              // Reduce the data to POLineProcessed
              dataIRNew = dataIRNew.map((dtirPib)=>{
                log.debug({title: 'dtirPib', details: dtirPib});
                var poLineFound = poLineProcessed.filter((poLinPib)=> poLinPib.item === dtirPib.item && poLinPib.lot=== dtirPib.lot)//|| [];
                log.debug({title: 'poLineFound', details: poLineFound});
                if(poLineFound.length > 0){
                  poLineFound.forEach((poLinePib)=>{
                    if(dtirPib.quantity > 0){
                      dtirPib.quantity -= poLinePib.quantity
                      poLineProcessed = poLineProcessed.filter((poLineFilter)=> poLineFilter.id !== poLinePib.id)
                    }
                  })
                }
                return dtirPib
              }).filter(dtif => dtif.quantity > 0);
  
              // Reduce the data to Item Fulfillment
              dataIF = dataIF.map((dtif)=>{
                var itemFulfillment = dataIR.filter((dtir)=> dtir.item === dtif.item && dtir.lot === dtif.lot)|| [];
                if(itemFulfillment.length > 0){
                  itemFulfillment.forEach((ir)=>{
                    dtif.quantity -= ir.quantity
                  })
                }
                return dtif
              }).filter(dtif => dtif.quantity > 0)
  
              log.debug({title: 'Reduce data to Itemful:', details: {count:dataIF.length, dataIF}});
              log.debug({title: 'Reduce data to IR PO Line:', details: {count: dataIRNew.length, dataIRNew}});
            }
            break;
          case 'TrnfrOrd': {
            // Obtain data to NetSuite
            poLineProcessed = getPOLine(dataOld)
            log.debug({title: 'POLineProcessed', details: poLineProcessed[0]});
            dataIR = getIRLot(dataOld[0][WEINFUSE.FIELDS.TO]);
            var dataIRNew = [... dataIR]
            log.debug({title: 'dataIR', details: dataIR});
            dataIF = getIFLot(dataOld[0][WEINFUSE.FIELDS.TO])
            // log.debug({title: 'dataIF', details: dataIF});

            // Reduce the data to POLineProcessed
            dataIRNew = dataIRNew.map((dtirPib)=>{
              log.debug({title: 'dtirPib', details: dtirPib});
              var poLineFound = poLineProcessed.filter((poLinPib)=> poLinPib.item === dtirPib.item && poLinPib.lot=== dtirPib.lot)//|| [];
              log.debug({title: 'poLineFound', details: poLineFound});
              if(poLineFound.length > 0){
                poLineFound.forEach((poLinePib)=>{
                  if(dtirPib.quantity > 0){
                    dtirPib.quantity -= poLinePib.quantity
                    poLineProcessed = poLineProcessed.filter((poLineFilter)=> poLineFilter.id !== poLinePib.id)
                  }
                })
              }
              return dtirPib
            }).filter(dtif => dtif.quantity > 0);

            // Reduce the data to Item Fulfillment
            dataIF = dataIF.map((dtif)=>{
              var itemFulfillment = dataIR.filter((dtir)=> dtir.item === dtif.item && dtir.lot === dtif.lot)|| [];
              if(itemFulfillment.length > 0){
                itemFulfillment.forEach((ir)=>{
                  dtif.quantity -= ir.quantity
                })
              }
              return dtif
            }).filter(dtif => dtif.quantity > 0)

            log.debug({title: 'Reduce data to Itemful:', details: {count:dataIF.length, dataIF}});
            log.debug({title: 'Reduce data to IR PO Line:', details: {count: dataIRNew.length, dataIRNew}});
          }
          break;
        }

        // Initialyze the validation to PO Lines
        for(let line=0; line < dataOld.length; line++){
          const updateFields = {}
          const poLinePib = dataOld[line];
          // Validation general to transaction related
          if (objTran.status !== 'fullyBilled' && objTran.status !== 'pendingBilling' && objTran.status !== 'closed' && objTran.status !== 'received') {
            // Validate if not have the Date Received
            if((poLinePib[WEINFUSE.FIELDS.DATE_RECEIVED] === '' || poLinePib[WEINFUSE.FIELDS.DATE_RECEIVED] === null)){
              updateFields[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.PENDING);
              updateFields[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.NOT_DATE_RECEIVE);
              poLineWithError.push({ id: poLinePib[WEINFUSE.FIELDS.INTERNALID], updateFields: updateFields});
              continue;
            }
            // Validate if not have the Lot Number
            if(poLinePib[WEINFUSE.FIELDS.LOT] === '' && poLinePib[WEINFUSE.FIELDS.LOT] === null){
              updateFields[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.PENDING);
              updateFields[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.LOT_NOT_FOUND);
              poLineWithError.push({ id: poLinePib[WEINFUSE.FIELDS.INTERNALID], updateFields: updateFields});
              continue;
            }
            // Validate the status of the transaction, if the status is incorrect, the lines cannot be processe
            switch (objTran.type) {
              // Case to Purchase Order
              case 'PurchOrd': {
                if (objTran.status !== 'pendingSupApproval') {
                  var dataIFFound = dataIF.findIndex((dtif)=> dtif.quantity > 0 && dtif.item == poLinePib[WEINFUSE.FIELDS.ITEM] && dtif.lot == poLinePib[WEINFUSE.FIELDS.LOT]);
                  if(dataIFFound !== -1){
                    dataIF[dataIFFound].quantity -= poLinePib[WEINFUSE.FIELDS.QUANTITY]
                    poLineToProcess.push(poLinePib)
                    continue;
                  } else {
                    updateFields[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.PENDING);
                    updateFields[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.LOT_MUST_HAVE_FULFILL);
                    poLineWithError.push({ id: poLinePib[WEINFUSE.FIELDS.INTERNALID], updateFields: updateFields});
                    continue;
                  }
                }
                else {
                  updateFields[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.NOT_APPROVED)
                  updateFields[WEINFUSE.FIELDS.ERRORS] = ''
                  poLineWithError.push({ id: poLinePib[WEINFUSE.FIELDS.INTERNALID], updateFields: updateFields});
                  continue;
                }
                break
              }
              case 'SalesOrd': {
                if (poLinePib[WEINFUSE.FIELDS.VENDOR_NAME] !== MAIN_CONFIG[RECORDS.CONFIG.FIELDS.VENDOR]) {
                  var dataIFFound = dataIF.findIndex((dtif)=> dtif.quantity > 0 && dtif.item == poLinePib[WEINFUSE.FIELDS.ITEM] && dtif.lot == poLinePib[WEINFUSE.FIELDS.LOT]);
                  if(dataIFFound !== -1){
                    dataIF[dataIFFound].quantity -= poLinePib[WEINFUSE.FIELDS.QUANTITY]
                    poLineToProcess.push(poLinePib)
                    continue;
                  } else {
                    updateFields[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.PENDING);
                    updateFields[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.LOT_MUST_HAVE_FULFILL);
                    poLineWithError.push({ id: poLinePib[WEINFUSE.FIELDS.INTERNALID], updateFields: updateFields});
                    continue;
                  }
                }
                else {
                  updateFields[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.PROCESSED)
                  poLineWithError.push({ id: poLinePib[WEINFUSE.FIELDS.INTERNALID], updateFields: updateFields});
                  continue;
                }
                break
              }
              case 'TrnfrOrd': {
                // Validate the transfer order
                if ( objTran.status === 'pendingReceipt' || objTran.status === 'pendingReceiptPartFulfilled' || objTran.status === 'partiallyFulfilled' ) {
                  // TODO: 
                  // Comparar las cantidades de todos aquellos que ya fueron procesados pero que no estan en esta ejecucion, restando cantidades a los IF obtenidos
                  var dataIFFound = dataIF.findIndex((dtif)=> dtif.quantity > 0 && dtif.item == poLinePib[WEINFUSE.FIELDS.ITEM] && dtif.lot == poLinePib[WEINFUSE.FIELDS.LOT]);
                  if(dataIFFound !== -1){
                    dataIF[dataIFFound].quantity -= poLinePib[WEINFUSE.FIELDS.QUANTITY]
                    poLineToProcess.push(poLinePib)
                    continue;
                  } else {
                    updateFields[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.PENDING);
                    updateFields[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.LOT_MUST_HAVE_FULFILL);
                    poLineWithError.push({ id: poLinePib[WEINFUSE.FIELDS.INTERNALID], updateFields: updateFields});
                    continue;
                  }
                  // TODO: 
                  // Comparar las cantidades de todos aquellos que ya fueron recibidos pero que no estan en esta ejecucion, restando cantidades a los recibidos

                  


                  // for(let lotIndex = 0; lotIndex < dataIF.length; lotIndex++){
                  //   const dataAv = dataIF[lotIndex];

                  //   // Decrement the quantity to original
                  //   if((poLinePib[WEINFUSE.FIELDS.QUANTITY] -dataAv.quantity) >= 0 ){
                  //     dataAv.quantity -= poLinePib[WEINFUSE.FIELDS.QUANTITY];
                  //     continue;
                  //   } else {
                  //     updateFields[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.PENDING);
                  //     updateFields[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.LOT_MUST_HAVE_FULFILL);
                  //     poLineWithError.push({ id: poLinePib[WEINFUSE.FIELDS.INTERNALID], updateFields: updateFields});
                  //     continue;
                  //   }
                  // }
                  // const objTO = getFulfillData(poLinePib[WEINFUSE.FIELDS.TO])
                  // log.debug({title: 'objTO', details: objTO});
                  // let matchDates = 0;
                  // const arrIF = [];
                  // objTO.forEach(fulfill => {
                  //   if (moment(data[0][WEINFUSE.FIELDS.DATE_RECEIVED]).format('MM/DD/YYYY') < moment(fulfill.date).format('MM/DD/YYYY')) {
                  //     matchDates++;
                  //     arrIF.push(fulfill.id);
                  //   }
                  // })
                  // if (matchDates) {
                  //   findLots = getLotsOnFulfill(data[0][WEINFUSE.FIELDS.TO], dataItems);
                  //   var dataFilter = getIFLot(data[0][WEINFUSE.FIELDS.TO], dataNew);
                  //   if (findLots.length === 0) {

                  //     // Cicle t remove the lines not processing correctly
                  //     log.debug({title: 'dataItems.length', details: dataItems.length});
                  //     dataItems.map((dataPib)=>{
                  //       // Remove the lines that not fulfill complete
                  //       const dataFind = dataFilter.filter((iIFPib)=>iIFPib.item == dataPib[WEINFUSE.FIELDS.ITEM] && iIFPib.lot == dataPib[WEINFUSE.FIELDS.LOT]) || [];
                  //       log.debug({title: 'dataFind', details: dataFind});
                  //       if(dataFind.length > 0){
                  //         // Not item fulfillment lot
                  //         dataFind.map((dataFindPib)=>{
                  //           dataPib[WEINFUSE.FIELDS.QUANTITY] -= dataFindPib.quantity
                  //           log.debug({title: 'New quantity', details: dataPib});
                  //           const updateFields1 = {};
                  //           updateFields1[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.LOT_MUST_HAVE_FULFILL)          
                  //           updateFields1[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.ERROR)
                  //           updateWeInfuseRecord(dataFindPib.internalId, updateFields1)
                  //           data = data.filter((dataFilterPib)=> dataFilterPib[WEINFUSE.FIELDS.INTERNALID] !== dataFindPib.internalId)
                  //         })
                  //       }
                  //       return dataPib;
                  //     })
                  //     log.debug({title: 'dataItems.lentgh New', details: data.length});
                  //     const recordId = createTransformRecord(
                  //       record.Type.TRANSFER_ORDER,
                  //       objTran.id,
                  //       record.Type.ITEM_RECEIPT,
                  //       dataItems,
                  //       'item',
                  //       closeLines
                  //     )
                  //     if (recordId) {
                  //       log.audit('reduce ~ recordId:', recordId)
                  //       updateFields[WEINFUSE.FIELDS.RECEIPT] = recordId
                  //       updateFields[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.PROCESSED)
                  //     } else if(errorMap !== null){
                  //       log.debug({title: 'errorMap', details: errorMap});
                  //       switch(errorMap){
                  //         case "MATCHING_SERIAL_NUM_REQD":
                  //             updateFields[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.LOT_MUST_HAVE_FULFILL)
                  //           break;
                  //         default:
                  //             updateFields[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.ERROR)
                  //           break;
                  //       }
                  //       updateFields[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.ERROR)
                  //     } else {
                  //       updateFields[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.ERROR)
                  //       updateFields[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.ERROR)
                  //     }
                  //   } else {
                  //     updateFields[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.ERROR)
                  //     updateFields[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.LOT_MISMATCH)
                  //   }
                  // } else {
                  //   updateFields[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.ERROR)
                  //   updateFields[WEINFUSE.FIELDS.ERRORS] = assignErrors(ERRORS.FULFILL_DATE_HIGHER)
                  // }
                } else {
                  updateFields[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.NOT_PENDING)
                  updateFields[WEINFUSE.FIELDS.ERRORS] = ''
                  poLineWithError.push({ id: poLinePib[WEINFUSE.FIELDS.INTERNALID], updateFields: updateFields});
                  continue;
                }
                break
              }
            }
          } else {
            // updateFields[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.CLOSED);
            updateFields[WEINFUSE.FIELDS.STATUS] = assignStatus(STATUS.PROCESSED);
            updateFields[WEINFUSE.FIELDS.ERRORS] = '';
            poLineWithError.push({ id: poLinePib[WEINFUSE.FIELDS.INTERNALID], updateFields: updateFields});
            continue;
          }
        }
      }
      return {poLineWithError, poLineToProcess};
    } catch (e) {
        log.error({title: 'Error validationToData:', details: e});
        
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
  const summarize = summaryContext => {}

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
          const findItem = newValues.findIndex(
            i => i[WEINFUSE.FIELDS.ITEM] === item[WEINFUSE.FIELDS.ITEM] && i[WEINFUSE.FIELDS.LOT] === item[WEINFUSE.FIELDS.LOT]
          )
          if (findItem !== -1) {
            newValues[findItem][WEINFUSE.FIELDS.QUANTITY] += Number(item[WEINFUSE.FIELDS.QUANTITY])
            newValues[findItem][WEINFUSE.FIELDS.PRICE] += Number(item[WEINFUSE.FIELDS.PRICE])
            // newValues[findItem][WEINFUSE.FIELDS.LINE_ID] += `|${Number(item[WEINFUSE.FIELDS.LINE_ID])}`
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

  const getLinesClose = id => {
    const lines = []
    try {
      const objSearch = search.create({
        type: search.Type.TRANSACTION,
        filters: [
          ['mainline', search.Operator.IS, 'F'],
          'AND',
          ['cogs', search.Operator.IS, 'F'],
          'AND',
          ['closed', search.Operator.IS, 'T'],
          'AND',
          ['internalid', search.Operator.IS, id]
        ],
        columns: [{ name: 'item' }, { name: 'closed' }, { name: 'quantity' }]
      })
      const countResults = objSearch.runPaged().count
      if (countResults) {
        objSearch.run().each(result => {
          const line = {
            item: result.getValue({ name: 'item' }),
            quantity: Number(result.getValue({ name: 'quantity' })),
            closed: result.getValue({ name: 'closed' })
          }
          lines.push(line)
          return true
        })
      }
    } catch (err) {
      log.error('Error on getLinesClose', err)
    }
    return lines
  }

  const getFulfillData = id => {
    const response = []
    try {
      const objSearch = search.create({
        type: search.Type.ITEM_FULFILLMENT,
        filters: [['mainline', search.Operator.IS, 'T'], 'AND', ['createdfrom', search.Operator.IS, id]],
        columns: [{ name: 'trandate' }, { name: 'internalid' }]
      })
      const countResults = objSearch.runPaged().count
      if (countResults) {
        objSearch.run().each(result => {
          const line = {}
          line.id = result.getValue({ name: 'internalid' })
          line.date = result.getValue({ name: 'trandate' })
          response.push(line)
          return true
        })
      }
    } catch (err) {
      log.error('Error on getFulfillData', err)
    }
    return response
  }

  const getLotsOnFulfill = (id, items) => {
    const itemMismatch = []
    try {
      const { WEINFUSE } = RECORDS
      const objSearch = search.create({
        type: search.Type.ITEM_FULFILLMENT,
        filters: [['mainline', search.Operator.ANY, ''], 'AND', ['createdfrom', search.Operator.ANYOF, id], 'AND', ['cogs', search.Operator.IS, 'F']],
        columns: [
          search.createColumn({ name: 'item' }),
          search.createColumn({ name: 'quantity' }),
          search.createColumn({ name: 'inventorynumber', join: 'inventoryDetail' })
        ]
      })
      const countResults = objSearch.runPaged().count
      if (countResults) {
        const itemFulfill = []
        objSearch.run().each(result => {
          itemFulfill.push({
            item: result.getValue({ name: 'item' }),
            quantity: result.getValue({ name: 'quantity' }),
            lot: result.getText({ name: 'inventorynumber', join: 'inventoryDetail' })
          })
          return true
        })
        items.forEach(item => {
          if (
            itemFulfill.findIndex(itemFulfill => itemFulfill.item === item[WEINFUSE.FIELDS.ITEM] && itemFulfill.lot === item[WEINFUSE.FIELDS.LOT]) ===
            -1
          ) {
            itemMismatch.push(item)
          }
        })
      }
    } catch (err) {
      log.error('Error on getLotsOnFulfill', err)
    }
    return itemMismatch
  }
  
  const getIFLot = (id) => {
    var itemFulfill = []
    try {
      const objSearch = search.create({
        type: search.Type.ITEM_FULFILLMENT,
        filters: [['mainline', search.Operator.ANY, ''], 'AND', ['createdfrom', search.Operator.ANYOF, id], 'AND', ['cogs', search.Operator.IS, 'F']],
        columns: [
          search.createColumn({ name: 'item', summary: search.Summary.GROUP }),
          search.createColumn({ name: 'quantity', join: 'inventoryDetail', summary: search.Summary.SUM}),
          search.createColumn({ name: 'inventorynumber', join: 'inventoryDetail', summary: search.Summary.GROUP })
        ]
      })
      const countResults = objSearch.runPaged().count
      if (countResults) {
        objSearch.run().each(result => {
          itemFulfill.push({
            item: result.getValue({ name: 'item', summary: search.Summary.GROUP }),
            quantity: Number(result.getValue({ name: 'quantity', join: 'inventoryDetail', summary: search.Summary.SUM })),
            lot: result.getText({ name: 'inventorynumber', join: 'inventoryDetail', summary: search.Summary.GROUP })
          })
          return true
        })
      }
      
    } catch (err) {
      log.error('Error on getIFLot', err)
    }
    return itemFulfill
  }

  const getIRLot = (id) => {
    var itemReceipt = []
    try {
      // Obtain the item receipt that have the lot number by item
      const objSearch = search.create({
        type: search.Type.ITEM_RECEIPT,
        filters: [['mainline', search.Operator.ANY, ''], 'AND', ['createdfrom', search.Operator.ANYOF, id], 'AND', ['cogs', search.Operator.IS, 'F'], "AND", ["type",search.Operator.ANYOF,"ItemRcpt"]],
        columns: [
          search.createColumn({ name: 'item'}),
          search.createColumn({ name: 'quantity', join: 'inventoryDetail'}),
          search.createColumn({ name: 'inventorynumber', join: 'inventoryDetail'})
        ]
      })
      const countResults = objSearch.runPaged().count
      if (countResults) {
        objSearch.run().each(result => {
          const objAux = {
            id: result.id,
            item: result.getValue({ name: 'item'}),
            quantity: Number(result.getValue({ name: 'quantity', join: 'inventoryDetail'})),
            lot: result.getText({ name: 'inventorynumber', join: 'inventoryDetail'})
          }
          if(objAux.item !== '' && objAux.quantity !== 0 && objAux.lot !== ''){
            itemReceipt.push(objAux)
          }
          return true
        })
      }
      
    } catch (err) {
      log.error('Error on getIRLot', err)
    }
    return itemReceipt;
  }
  
  const getPOLine = (items) => {
    var POLineProcessed = [];
    try {
      const { WEINFUSE } = RECORDS
      log.debug({title: 'items', details: items});
      var ids = items.reduce((acc, item) => {
        acc.push(item[WEINFUSE.FIELDS.INTERNALID]);
        return acc;
    }, []);
    
      log.debug({title: 'ids', details: ids});
      const objSearch = search.create({
        type: WEINFUSE.ID,
        filters: [
          [WEINFUSE.FIELDS.TO, search.Operator.ANYOF, items[0][WEINFUSE.FIELDS.PO]], 
          'AND', 
          [WEINFUSE.FIELDS.INTERNALID, search.Operator.NONEOF, ids],
          // 'AND', 
          // [WEINFUSE.FIELDS.STATUS_STAGE, search.Operator.ANYOF, 4]
        ],
        columns: [
          search.createColumn({ name: WEINFUSE.FIELDS.ITEM }),
          search.createColumn({ name: WEINFUSE.FIELDS.QUANTITY }),
          search.createColumn({ name: WEINFUSE.FIELDS.LOT })
        ]
      })
      const countResults = objSearch.runPaged().count
      log.debug({title: 'countResults', details: countResults});
      if (countResults) {
        objSearch.run().each(result => {
          POLineProcessed.push({
            id: result.id,
            item: result.getValue({ name: WEINFUSE.FIELDS.ITEM }),
            quantity: Number(result.getValue({ name: WEINFUSE.FIELDS.QUANTITY})),
            lot: result.getValue({ name: WEINFUSE.FIELDS.LOT })
          })
          return true
        })
      }
    } catch (err) {
      log.error('Error on getPOLine', err)
    }
    return POLineProcessed
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
   * The function `createTransformRecord` creates a transformation record in JavaScript.
   * @param fromType - The `fromType` parameter represents the record type of the source record that
   * you want to transform. It could be a string value such as "customer", "salesorder", "invoice",
   * etc.
   * @param fromId - The `fromId` parameter represents the internal ID of the record you want to
   * transform. It is used to identify the specific record that you want to transform from one type to
   * another.
   * @param toType - The `toType` parameter represents the type of record that you want to transform
   * the current record into. It specifies the record type that you want to convert the current record
   * to.
   * @returns the recordId of the saved object record.
   */
  const createTransformRecord = (fromType, fromId, toType, items, sublistId, closeLines = []) => {
    try {
      const { WEINFUSE } = RECORDS
      const { ITEM_FULFILLMENT, ITEM_RECEIPT } = TRANSACTIONS
      if (closeLines.length && items.length) {
        closeLines.forEach(lineClose => {
          const indexClose = items.findIndex(altItem => altItem[WEINFUSE.FIELDS.ITEM] === lineClose.item)
          if (indexClose !== -1) {
            const unitType = assignUnitsType(
              items[indexClose][WEINFUSE.FIELDS.UNIT_TYPE],
              items[indexClose][WEINFUSE.FIELDS.PURCHASE_UNIT],
              items[indexClose][WEINFUSE.FIELDS.PURCHASE_UNIT_ID],
              items[indexClose][WEINFUSE.FIELDS.QUANTITY]
            )
            log.debug({title: 'unitType', details: unitType});
            if (unitType.data.quantity === lineClose.quantity) {
              items.splice(indexClose, 1)
            }
          }
        })
      }
      log.debug('createTransformRecord ~ items:', items)
      log.debug('createTransformRecord ~ items.length:', items.length)
      if (items.length) {
        const objRecord = record.transform({
          fromType,
          fromId,
          toType,
          isDynamic: true
        })
        switch (fromType) {
          case record.Type.PURCHASE_ORDER:
          case record.Type.TRANSFER_ORDER: {
            
            // Agrupando datos para elaborar las lineas del detalle de inventario
            const groupingItems = {};
            items.forEach((dataItem)=>{
              let internalId = dataItem[WEINFUSE.FIELDS.ITEM];
              if(!groupingItems[internalId]){
                groupingItems[internalId] = []
              }
              groupingItems[internalId].push(dataItem);
            })
            log.debug({title: 'groupedMap', details: groupingItems});

            //Colocando los datos a nivel cabecera
            const mainLocation = objRecord.getValue({ fieldId: ITEM_RECEIPT.MAIN.LOCATION })
            objRecord.setValue({ fieldId: ITEM_RECEIPT.MAIN.DATE, value: formatDate(items[0][WEINFUSE.FIELDS.DATE_RECEIVED]) })
            
            // Obtencion y declariacion de item transaction
            const numLines = objRecord.getLineCount({ sublistId })
            const itemsTransaction = {}

            // Se colocan las lineas recibidas como falso esto con el objetivo que sean dinamicas y poder manipular el objeto para su respectiva entrada de inventario
            for (let i = 0; i < numLines; i++) {
              const itemID = objRecord.getSublistValue({ sublistId, fieldId: ITEM_RECEIPT.LIST.ITEM, line: i })
              const name = objRecord.getSublistValue({ sublistId, fieldId: 'description', line: i })
              const receive = objRecord.getSublistValue({ sublistId, fieldId: ITEM_RECEIPT.LIST.ITEMRECEIVE, line: i })
              const quantity = objRecord.getSublistValue({ sublistId, fieldId: ITEM_RECEIPT.LIST.QUANTITY, line: i })
              itemsTransaction[i] = { itemID, name, receive, quantity }
              objRecord.selectLine({ sublistId, line: i })
              objRecord.setCurrentSublistValue({ sublistId, fieldId: ITEM_RECEIPT.LIST.ITEMRECEIVE, value: false, ignoreFieldChange: false })
              objRecord.commitLine({ sublistId })
            }

            // Recorrido de las lineas para la colocar los diferentes datos a nivel linea
            for (let i = 0; i < numLines; i++) {
              log.debug({title: 'i', details: i});
              objRecord.selectLine({ sublistId, line: i })
              const item = objRecord.getCurrentSublistValue({ sublistId, fieldId: ITEM_RECEIPT.LIST.ITEM })
              const indexItem = items.findIndex(altItem => altItem[WEINFUSE.FIELDS.ITEM] === item)
              
              const dataItem = { ...items[indexItem] }
              if (indexItem !== -1) {
                items.splice(indexItem, 1)
              }
              if (!dataItem || !Object.keys(dataItem).length) {
                continue
              }
              // log.debug({title: 'dataItem', details: groupingItems[item]});
              log.debug({title: 'dataItem length', details: groupingItems[item].length});
              var quantityTotal = 0;
              groupingItems[item].forEach( (currentValue) => {
                log.debug({title: 'currentValue', details: currentValue});
                quantityTotal += currentValue[WEINFUSE.FIELDS.QUANTITY]
              });
              
              log.debug({title: 'quantityTotal', details: quantityTotal});
              /** Match to units type accord by quantity item */
              const unitType = assignUnitsType(
                dataItem[WEINFUSE.FIELDS.UNIT_TYPE],
                dataItem[WEINFUSE.FIELDS.PURCHASE_UNIT],
                dataItem[WEINFUSE.FIELDS.PURCHASE_UNIT_ID],
                quantityTotal// dataItem[WEINFUSE.FIELDS.QUANTITY]
              )
              log.debug({title: 'unitType', details: unitType});
              objRecord.setCurrentSublistValue({ sublistId, fieldId: ITEM_RECEIPT.LIST.ITEMRECEIVE, value: true, ignoreFieldChange: false })
              objRecord.setCurrentSublistValue({ sublistId, fieldId: ITEM_RECEIPT.LIST.QUANTITY, value: unitType.data.quantity || dataItem[WEINFUSE.FIELDS.QUANTITY], ignoreFieldChange: false })
              objRecord.setCurrentSublistValue({ sublistId, fieldId: ITEM_RECEIPT.LIST.LOCATION, value: mainLocation, ignoreFieldChange: false })
              /**  ------------------------- Assignment Inventory Detail ------------------------------------ */
              const invDetail = objRecord.getCurrentSublistSubrecord({ sublistId, fieldId: ITEM_RECEIPT.LIST.INVENTORYDETAIL })
              const linesInvDet = invDetail.getLineCount({ sublistId: ITEM_RECEIPT.LIST.INVENTORYASSIGNMENT })
              log.debug('createTransformRecord ~ linesInvDet:', linesInvDet)
              if (linesInvDet > 0) {
                for (let j = linesInvDet - 1; j >= 0; j--) {
                  invDetail.removeLine({ sublistId: ITEM_RECEIPT.LIST.INVENTORYASSIGNMENT, line: j, ignoreRecalc: false })
                }
              }
              
              groupingItems[item].forEach( (currentValue) => {
                log.debug({title: 'currentValue', details: currentValue});
                invDetail.selectNewLine({ sublistId: ITEM_RECEIPT.LIST.INVENTORYASSIGNMENT })
                invDetail.setCurrentSublistValue({
                  sublistId: ITEM_RECEIPT.LIST.INVENTORYASSIGNMENT,
                  fieldId: ITEM_RECEIPT.LIST.RECEIPTINVENTORYNUMBER,
                  value: currentValue[WEINFUSE.FIELDS.LOT],
                  ignoreFieldChange: false
                })
                var quantityavailable = invDetail.getCurrentSublistValue({
                  sublistId: ITEM_RECEIPT.LIST.INVENTORYASSIGNMENT,
                  fieldId: ITEM_RECEIPT.LIST.QUANTITY_AVAILABLE,
                  ignoreFieldChange: false
                })
                log.debug({title: 'quantityavailable', details: quantityavailable});
                log.debug({title: 'currentValue[WEINFUSE.FIELDS.QUANTITY]', details: currentValue[WEINFUSE.FIELDS.QUANTITY]});
                invDetail.setCurrentSublistValue({
                  sublistId: ITEM_RECEIPT.LIST.INVENTORYASSIGNMENT,
                  fieldId: ITEM_RECEIPT.LIST.EXPIRATIONDATE,
                  value: formatDate(currentValue[WEINFUSE.FIELDS.EXPIRATION]),
                  ignoreFieldChange: false
                })
                invDetail.setCurrentSublistValue({
                  sublistId: ITEM_RECEIPT.LIST.INVENTORYASSIGNMENT,
                  fieldId: ITEM_RECEIPT.LIST.QUANTITY,
                  value: currentValue[WEINFUSE.FIELDS.QUANTITY],
                  ignoreFieldChange: false
                })
                invDetail.commitLine({ sublistId: ITEM_RECEIPT.LIST.INVENTORYASSIGNMENT })
              });
              /** ----------------------------------------------------------------------- */
              objRecord.commitLine({ sublistId })
            }
            break
          }
          case record.Type.SALES_ORDER: {
            objRecord.setValue({ fieldId: ITEM_FULFILLMENT.MAIN.DATE, value: formatDate(items[0][WEINFUSE.FIELDS.DATE_RECEIVED]) })
            const numLines = objRecord.getLineCount({ sublistId })
            for (let i = 0; i < numLines; i++) {
              objRecord.selectLine({
                sublistId,
                line: i
              })
              objRecord.setCurrentSublistValue({
                sublistId,
                fieldId: ITEM_RECEIPT.LIST.ITEMRECEIVE,
                value: false,
                ignoreFieldChange: false
              })
              objRecord.commitLine({ sublistId })
            }

            for (let i = 0; i < numLines; i++) {
              objRecord.selectLine({
                sublistId,
                line: i
              })
              const item = objRecord.getCurrentSublistValue({ sublistId, fieldId: ITEM_RECEIPT.LIST.ITEM })
              const indexItem = items.findIndex(altItem => altItem[WEINFUSE.FIELDS.ITEM] === item)

              const dataItem = { ...items[indexItem] }
              if (indexItem !== -1) {
                items.splice(indexItem, 1)
              }
              if (!dataItem || !Object.keys(dataItem).length) {
                continue
              }
              /** Match to units type accord by quantity item */
              const unitType = assignUnitsType(
                dataItem[WEINFUSE.FIELDS.UNIT_TYPE],
                dataItem[WEINFUSE.FIELDS.PURCHASE_UNIT],
                dataItem[WEINFUSE.FIELDS.PURCHASE_UNIT_ID],
                dataItem[WEINFUSE.FIELDS.QUANTITY]
              )
              objRecord.setCurrentSublistValue({
                sublistId,
                fieldId: ITEM_RECEIPT.LIST.ITEMRECEIVE,
                value: true,
                ignoreFieldChange: false
              })
              objRecord.setCurrentSublistValue({
                sublistId,
                fieldId: ITEM_RECEIPT.LIST.QUANTITY,
                value: unitType.data.quantity || dataItem[WEINFUSE.FIELDS.QUANTITY],
                ignoreFieldChange: false
              })
              /**  ------------------------- Assignment Inventory Detail ------------------------------------ */
              const invDetail = objRecord.getCurrentSublistSubrecord({
                sublistId,
                fieldId: ITEM_RECEIPT.LIST.INVENTORYDETAIL
              })
              invDetail.selectNewLine({ sublistId: ITEM_RECEIPT.LIST.INVENTORYASSIGNMENT })
              invDetail.setCurrentSublistValue({
                sublistId: ITEM_RECEIPT.LIST.INVENTORYASSIGNMENT,
                fieldId: ITEM_RECEIPT.LIST.RECEIPTINVENTORYNUMBER,
                value: dataItem[WEINFUSE.FIELDS.LOT]
              })
              invDetail.setCurrentSublistValue({
                sublistId: ITEM_RECEIPT.LIST.INVENTORYASSIGNMENT,
                fieldId: ITEM_RECEIPT.LIST.EXPIRATIONDATE,
                value: formatDate(dataItem[WEINFUSE.FIELDS.EXPIRATION])
              })
              invDetail.setCurrentSublistValue({
                sublistId: ITEM_RECEIPT.LIST.INVENTORYASSIGNMENT,
                fieldId: ITEM_RECEIPT.LIST.QUANTITY,
                value: Number(unitType.data.quantity) || dataItem[WEINFUSE.FIELDS.QUANTITY]
              })
              invDetail.commitLine({ sublistId: ITEM_RECEIPT.LIST.INVENTORYASSIGNMENT })
              /** ----------------------------------------------------------------------- */
              objRecord.commitLine({ sublistId })
            }
            break
          }
        }
        const recordId = objRecord.save({
          enableSourcing: true,
          ignoreMandatoryFields: true
        })
        log.debug('RECEIPT RECORDID:', recordId)
        return recordId
      } else {
        return ''
      }
    } catch (err) {
      log.error('Error on createTransformRecord', err)
      errorMap = err.name
      return null
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
