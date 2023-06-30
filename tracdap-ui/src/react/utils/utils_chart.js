import {Sizes} from "../../config/config_general";
import {
    convertNumberFormatToArray,
    getUniqueVariableTypes,
    schemaIsAllNumbers,
    schemaIsMissingNumberFormatCode,
    applyNumberFormat,
    schemaIsAllDatesOrDatetimes,
    getUniqueNumberFormatCodes,
    getUniqueDatetimeFormatCodes,
    getUniqueFormatCodes,
    convertUtcSecondsToDateObject,
    formatDateObject,
    getUniqueRowsOfArrayOfObjects, getAllDateAndDatetimeVariables, getThemeColour
} from "./utils";
import {GeneralChartConfig} from "../../config/config_charts";
import parseISO from "date-fns/parseISO";
import Highcharts from "highcharts";
import {convertKeyToText} from "./utils_string";
import {convertArrayToOptions} from "./utils_arrays";

export const setHighchartsStyles = (Highcharts) => {

    // Set the unit labels for thousands, millions and also the thousands separator
    return Highcharts.setOptions({
        chart: {
            style: {
                // This should match the AppTsx.css fonts
                fontFamily: "ui-sans-serif,  system-ui, -system-ui, -apple-system, BlinkMacSystemFont, Roboto, Helvetica, Arial, sans-serif"
            }
        }, lang: {
            numericSymbols: GeneralChartConfig.numericSymbols, thousandsSep: GeneralChartConfig.thousandsSeparator
        }
    })
}




export const getHighChartType = (chartType) => {

    return ["circleDoughnut", "semiDoughnut"].includes(chartType) ? "pie" : ["lineWithError"].includes(chartType) ? "line" : ["columnWithError"].includes(chartType) ? "column" : chartType
}

/**
 * A function that runs when the chart mounts, it sets the initial x and y-axis variables to show on the chart. It deals
 * with two cases, one where the chart component has default values for the variables to select and one where is
 * doesn't so we just pick the first n series.
 * @param variableOptions - The TRAC schema for the variables that can  be selected as a Y variable.
 * @param defaultVariables - An array of variable names to use as the selected variables.
 * @param defaultMaximumNumberOfSeries - An integer number of variables to pick if defaultYVariables is
 * not set.
 * @param maxVariablesPerAxis - The maximum number of variables that can be selected per axis.
 */
export const setInitialSelectedVariables = (variableOptions, defaultVariables, defaultMaximumNumberOfSeries = 1, maxVariablesPerAxis = 1) => {

    // If there are no default variables defined then we choose some for the user up to defaultMaximumNumberOfSeries
    if (defaultVariables === "auto") {

        return variableOptions.slice(0, Math.min(defaultMaximumNumberOfSeries, maxVariablesPerAxis))

    } else {

        // Set the initial Y1 variables to be the first n in the options
        return variableOptions.filter(variable => defaultVariables.includes(variable.fieldName)).slice(0, maxVariablesPerAxis)
    }
}

/**
 * A function that either finds the categorical variables in a dataset and uses these as the segment options or uses a user
 * defined list.
 * @param schema - The TRAC schema for the dataset.
 * @param defaultSegmentationVariables - The user defined list of segmentation variables to use.
 * @param hiddenColumns - The hidden columns that should be excluded as segmentation variables.
 * @param defaultXVariables - The list of x-axis variables.
 * @param defaultY1Variables - The list of y1-axis variables.
 * @param defaultY2Variables - The list of y2-axis variables.
 */
export const getSegmentationVariables = (schema, defaultSegmentationVariables, hiddenColumns, defaultXVariables, defaultY1Variables, defaultY2Variables) => {

    const categoricalVariables = getCategoricalVariables(schema).map(variable => variable.fieldName)

    // Ensure that all segmentation variables are categorical, if the user does not set a
    // segmentation variable to use then we set it to be all categorical variables
    let segmentationVariables = (defaultSegmentationVariables.length === 0 ? categoricalVariables : defaultSegmentationVariables)

    let excludedColumns = [...hiddenColumns]
    // By now if the defaults are a string it means that they are "auto"
    if (Array.isArray(defaultXVariables)) excludedColumns = [...excludedColumns, ...defaultXVariables]
    if (Array.isArray(defaultY1Variables)) excludedColumns = [...excludedColumns, ...defaultY1Variables]
    if (Array.isArray(defaultY2Variables)) excludedColumns = [...excludedColumns, ...defaultY2Variables]

    return segmentationVariables.filter(variable => !excludedColumns.includes(variable))
}



/**
 * A function that converts the values found in the data for the segmentation variables into
 * a set of options for the SelectOption component.
 * @param segmentationVariables - A list of the segmentation variables.
 * @param uniqueSegmentationCombinations - The rows from the dataset that have unique combinations
 * of the segmentation variables.
 * @param allowAllSegmentsToBePlotted - Whether to add an "All segments" option.
 */
export const getSegmentationOptions = (segmentationVariables, uniqueSegmentationCombinations, allowAllSegmentsToBePlotted) => {

    // Get the options to pass to the SelectOptions component
    let segmentationOptions = {}

    // Create a property for each variable
    segmentationVariables.forEach(key => {

        segmentationOptions[key] = []
    })

    // Go through the rows and add the options
    uniqueSegmentationCombinations.forEach(row => {

        segmentationVariables.forEach(key => {

            segmentationOptions[key].push(row[key])
        })
    })

    // Dedupe the lists, sort and convert to a set of options
    segmentationVariables.forEach(key => {

        // TODO these will have null and undefined or "" values in it so we might want to add a label for this case?
        segmentationOptions[key] = convertArrayToOptions([...new Set(segmentationOptions[key])].sort(), false, false)

        // Add an all option at the start
        if (allowAllSegmentsToBePlotted) segmentationOptions[key].unshift({value: "all", label: "All segments", isAll: true})
    })

    return segmentationOptions
}

/**
 * A function that sets the initial selections for the segmentation options. This takes the first unique combination of
 * segmentation variables found and looks up the corresponding option and returns that as the default.
 * @param segmentationVariables - A list of the segmentation variables.
 * @param uniqueSegmentationCombinations - The rows from the dataset that have unique combinations.
 * @param segmentationOptions -
 * @param allowAllSegmentsToBePlotted - Whether to allow "All segments" to be plotted, if so this is the default
 * option selected.

 */
export const setInitialSelectedSegmentationOptions = (segmentationVariables, uniqueSegmentationCombinations, segmentationOptions, allowAllSegmentsToBePlotted) => {

    let initialSelectedSegmentationOptions = {}

    // Create a property for each variable
    segmentationVariables.forEach(key => {

        initialSelectedSegmentationOptions[key] = null
    })

    if (uniqueSegmentationCombinations.length === 0) return initialSelectedSegmentationOptions

    // Pick the first set of segmentation variables as the default selection
    const uniqueSegmentationCombination = uniqueSegmentationCombinations[0]

    // Find the corresponding option
    segmentationVariables.forEach(key => {

        if (allowAllSegmentsToBePlotted) {

            initialSelectedSegmentationOptions[key] = {value: "all", label: "All segments", isAll: true}

        } else {

            initialSelectedSegmentationOptions[key] = segmentationOptions[key].find(option => option.value === uniqueSegmentationCombination[key])
        }

    })

    return initialSelectedSegmentationOptions
}

/**
 * A function that sets the minimum chart interval on the axis for date and datetime variables. This is the smallest
 * division that will be seen when zoomed in.
 * @param variables - The TRAC schema items for the axis variables.
 * @returns {null|number}
 */
export const setAxisMinInterval = (variables) => {

    // There are some cases where we have to bin out
    if (!variables || variables.length === 0) return undefined

    let finalXAxisVariables = getUniqueDatetimeFormatCodes(variables)

    // We only set the interval for date and datetime values
    if (finalXAxisVariables.length === 0) return undefined

    // TODO check if this actually works because the periods are not exact
    // TODO datetime needs to resolve finer than a day
    const intervalLookup = {
        "DAY": 1000 * 3600 * 24,
        "WEEK": 1000 * 3600 * 24 * 7,
        "MONTH": 1000 * 3600 * 24 * 364.25 / 12,
        "QUARTER": 1000 * 3600 * 24 * 364.25 / 4,
        "HALF-YEAR": 1000 * 3600 * 24 * 364.25 / 2,
        "YEAR": 1000 * 3600 * 24 * 364.25,
    }

    return Math.min(...finalXAxisVariables.map(formatCode => intervalLookup[formatCode]).filter(interval => interval !== undefined))
}

/**
 * A function that takes a row of a dataset and returns true if the row is contains data for
 * a user selected segmentation to chart.
 * @param row {{}}  The row of a dataset to check.
 * @param selectedSegmentationOptions {{}} An object of the selected segmentation values, keyed by the variable name.
 * @returns -
 */
export const filterRowForSegmentation = (row, selectedSegmentationOptions) => {

    let newRow = {...row}

    // If the use has selected the all option then we always pass the row through (true means it has no properties outside the chosen segmentation)
    const notInSegmentation = Object.entries(selectedSegmentationOptions).some(([key, value]) => {

        // Need to confirm how this works with undefined and null values
        return !(value.hasOwnProperty("isAll") && value.isAll === true) && newRow.hasOwnProperty(key) && value.value !== row[key && value.isAll]
    })

    return !notInSegmentation
}

/**
 * A function that takes a row of a dataset and converts date and datetime variables into the
 * right format required for HighCharts.
 * @param row {{}} The row of a dataset to convert the date and datetime variables in.
 * @param dateAndDatetimeVariables - The TRAC schema items for the date and datetime variables in the row.
 * @returns {{}}
 */
export const convertDateTimeVariables = (row, dateAndDatetimeVariables) => {

    let newRow = {...row}

    dateAndDatetimeVariables.forEach(variable => {

        const d = parseISO(newRow[variable.fieldName])
        newRow[variable.fieldName] = ![null, undefined].includes(newRow[variable.fieldName]) ? convertDateObjectToUtcSeconds(d) : null
    })

    return newRow
}

/**
 * A function that sets the series array for a line (line, scatter, area) HighChart..
 * @param chartType - The chart type.
 * @param xVariables - The TRAC schema items for the x axis variables.
 * @param y1Variables - The TRAC schema items for the y1 axis variables.
 * @param y2Variables - The TRAC schema items for the y2 axis variables.
 * @param useLineStyles - Whether the lines should be solid or dashed for greater differentiation.
 * @param variableLabels {-} An object containing the labels for the variables in the legend. These
 * include any user set overrides.
 * @returns {{yAxis: number, fieldName: *, data: [], dashStyle: *|string, formatCodeY: *, formatCodeX: *|undefined, name: *, type: *, fieldTypeY: *, fieldTypeX: *|undefined}[]}
 */
export const setSeriesForWaterfallChart = (chartType, xVariables, y1Variables, y2Variables, zVariables, useLineStyles, variableLabels) => {

    const numberOfY1Variables = y1Variables.length

    return [...y1Variables, ...y2Variables].map((variable, i) => (

        {
            type: getHighChartType(chartType),
            data: [],
            yAxis: i < numberOfY1Variables ? 0 : 1,
            fieldNameY: variable.fieldName,
            formatCodeY: variable.formatCode,
            fieldTypeY: variable.fieldType,
            formatCodeX: xVariables.length > 0 ? xVariables[0].formatCode : undefined,
            fieldTypeX: xVariables.length > 0 ? xVariables[0].fieldType : undefined,
            fieldLabelZ: zVariables.length > 0 ? variableLabels[zVariables[0].fieldName] : undefined,
            formatCodeZ: zVariables.length > 0 ? zVariables[0].formatCode : undefined,
            fieldTypeZ: zVariables.length > 0 ? zVariables[0].fieldType : undefined,
            dashStyle: "dot",
            name: variableLabels[variable.fieldName],
        }
    ))
}

export const setSeriesForBubbleChart = (chartType, xVariables, y1Variables, zVariables, useLineStyles, variableLabels) => {

    return [...y1Variables].map((variable, i) => (

        {
            type: "bubble",
            data: [],
            yAxis: 0,
            fieldLabelY: variableLabels[variable.fieldName],
            fieldNameY: variable.fieldName,
            formatCodeY: variable.formatCode,
            fieldTypeY: variable.fieldType,
            fieldLabelX: xVariables.length > 0 ? variableLabels[xVariables[0].fieldName] : undefined,
            formatCodeX: xVariables.length > 0 ? xVariables[0].formatCode : undefined,
            fieldTypeX: xVariables.length > 0 ? xVariables[0].fieldType : undefined,
            fieldLabelZ: zVariables.length > 0 ? variableLabels[zVariables[0].fieldName] : undefined,
            formatCodeZ: zVariables.length > 0 ? zVariables[0].formatCode : undefined,
            fieldTypeZ: zVariables.length > 0 ? zVariables[0].fieldType : undefined,
            name: variableLabels[variable.fieldName]
        }
    ))
}

export const setSeriesForOrganisationChart = () => {

    return (

        [{
            type: "dependencywheel",
            data: [],
            nodes: [],
            name: "Flow",
            // colorByPoint: false,
            // color: 'var(--info)',
            // borderColor: 'white',
            // nodeWidth: 100,
            // nodeHeight: 200,
            allowPointSelect: true,
            curveFactor:1,
            nodePadding: 20
        }]
    )

}


export const setSeriesForChartWithError = (chartType, xVariables, y1Variables, e1Variables, e2Variables, useLineStyles, variableLabels) => {

    const {formatCode: xFormatCode, fieldType: xFieldType} = xVariables[0]
    const {fieldName, formatCode, fieldType} = y1Variables[0]

    return [
        {
            type: getHighChartType(chartType),
            data: [],
            yAxis: 0,
            fieldNameY: fieldName,
            formatCodeY: formatCode,
            fieldTypeY: fieldType,
            formatCodeX: xFormatCode,
            fieldTypeX: xFieldType,
            dashStyle: "solid",
            name: variableLabels[fieldName]
        },
        {
            type: "errorbar",
            data: [],
            yAxis: 0,
            fieldNameY: fieldName,
            formatCodeY: formatCode,
            fieldTypeY: fieldType,
            formatCodeX: xFormatCode,
            fieldTypeX: xFieldType,
            name: `${variableLabels[fieldName]} error`
        }
    ]
}

/**
 * A function that sets the series array for an arearange HighChart. There is one series on the y1-axis, the y2 and z axis
 * variables are used to set the range.
 * @param chartType - The chart type.
 * @param xVariables - The TRAC schema items for the x axis variables.
 * @param y1Variables - The TRAC schema items for the y1 axis variables.
 * @param y2Variables - The TRAC schema items for the lower range variable.
 * @param zVariables - The TRAC schema items for the upper range variable.
 * @param useLineStyles - Whether the lines should be solid or dashed for greater differentiation.
 * @param variableLabels {-} An object containing the labels for the variables in the legend. These
 * include any user set overrides.
 * @returns {{yAxis: number, fieldName: *, data: [], dashStyle: *|string, formatCodeY: *, formatCodeX: *|undefined, name: *, type: *, fieldTypeY: *, fieldTypeX: *|undefined}[]}
 */
export const setSeriesForAreaRangeChart = (chartType, xVariables, y1Variables, useLineStyles, variableLabels) => {

    // TODO add in upper and lower formats and types
    return [...y1Variables].map((variable, i) => (

        {
            type: "arearange",
            data: [],
            yAxis: 0,
            fieldNameY: variable.fieldName,
            formatCodeY: variable.formatCode,
            fieldTypeY: variable.fieldType,
            formatCodeX: xVariables.length > 0 ? xVariables[0].formatCode : undefined,
            fieldTypeX: xVariables.length > 0 ? xVariables[0].fieldType : undefined,
            dashStyle: useLineStyles ? Charts.lineStyles[i % Charts.lineStyles.length] : "solid",
            name: variableLabels[variable.fieldName],
        }
    ))
}

export const setSeriesForHistogramChart = (chartType, y1Variables) => {

    return [

        {
            type: chartType,
            xAxis: 1,
            yAxis: 1,
            baseSeries: y1Variables[0].fieldName,
            name: "Histogram",
            formatCodeX: y1Variables[0].formatCode,
            fieldTypeX: y1Variables[0].fieldType,
        }, {
            type: "scatter",
            id: y1Variables[0].fieldName,
            data: [],
            fieldNameY: y1Variables[0].fieldName,
            formatCodeY: y1Variables[0].formatCode,
            fieldTypeY: y1Variables[0].fieldType,
            name: "Data",
            visible: false,
            marker: {
                radius: 1.5
            }
        }
    ]
}

export const setSeriesForStructureChart = (chartType, xVariables, y1Variables, zVariables, useLineStyles, variableLabels) => {

    return [...y1Variables].map((variable, i) => (

        {
            type: chartType,
            data: [],
            yAxis: 0,
            fieldLabelY: variableLabels[variable.fieldName],
            fieldNameY: variable.fieldName,
            formatCodeY: variable.formatCode,
            fieldTypeY: variable.fieldType,
            fieldLabelX: xVariables.length > 0 ? variableLabels[xVariables[0].fieldName] : undefined,
            formatCodeX: xVariables.length > 0 ? xVariables[0].formatCode : undefined,
            fieldTypeX: xVariables.length > 0 ? xVariables[0].fieldType : undefined,
            fieldLabelZ: zVariables.length > 0 ? variableLabels[zVariables[0].fieldName] : undefined,
            formatCodeZ: zVariables.length > 0 ? zVariables[0].formatCode : undefined,
            fieldTypeZ: zVariables.length > 0 ? zVariables[0].fieldType : undefined,
            name: variableLabels[variable.fieldName]
        }
    ))
}

export const addStructureSeriesData = (data, xVariables, y1Variables, zVariables, series, selectedSegmentationOptions) => {

    // First we need to create some lookups before we start to iterate through the dataset
    // These are all keyed by the variable fieldName
    const {multipliers} = getSeriesLookups(series, xVariables, y1Variables, [], zVariables)

    const yVariables = getUniqueRowsOfArrayOfObjects(y1Variables, ["fieldName", "formatCode", "fieldType"])

    // Get the datetime variables in the chart data
    const datetimeVariables = getUniqueRowsOfArrayOfObjects(getAllDateAndDatetimeVariables([...yVariables, ...xVariables, ...zVariables]), ["fieldName", "formatCode"])

    let newRow

    data.forEach((row, i) => {

        // Is the row in one of the user selected segments to plot
        const keepRow = filterRowForSegmentation(row, selectedSegmentationOptions)

        // If the row is not in a segment we need then skip to the next row
        if (!keepRow) return

        // Convert the date and datetime columns to the format understood by High Charts
        newRow = convertDateTimeVariables(row, datetimeVariables)

        yVariables.forEach(variable => {

            const {fieldName} = variable

            // If the datapoint is null we have to pass null back to HighCharts. However null * 1 = 0 in
            // Javascript so we need set these as null still after the multiplier
            const dataValue = processSeriesRow(fieldName, newRow, multipliers)

            const {fieldName: xFieldName} = xVariables.length > 0 ? xVariables[0] : null
            const {fieldName: zFieldName} = zVariables.length > 0 ? zVariables[0] : null

            // Get the other values
            const xDataValue = processSeriesRow(xFieldName, newRow, multipliers)
            const zDataValue = processSeriesRow(zFieldName, newRow, multipliers)

            series[0].data.push({from: xDataValue, to: dataValue, weight: zDataValue})
        })
    })

    return series

}

export const getSeriesLookups = (series, xVariables = [], y1Variables = [], y2Variables = [], zVariables = []) => {

    // First we need to create some lookups before we start to iterate through the dataset
    let indices = {}
    let formats = {}
    let multipliers = {}

    // All y-axis variables
    const yVariables = [...y1Variables, ...y2Variables]

    yVariables.forEach(variable => {

        const {fieldName, formatCode} = variable

        // We use filter because the same variable can be on the y1 and y2 axis in which case there are two series to update
        indices[fieldName] = series.map((item, i) => ({
            index: i,
            fieldName: item.fieldNameY
        })).filter(item => item.fieldName === fieldName).map(item => item.index)

        formats[fieldName] = convertNumberFormatToArray(formatCode)
        multipliers[fieldName] = formats[fieldName] && Charts.symbolConversion.hasOwnProperty(formats[fieldName][4].toLowerCase()) ? Charts.symbolConversion[formats[fieldName][4].toLowerCase()] : 1
    })

    xVariables.forEach(variable => {
        const {fieldName, formatCode} = variable

        formats[fieldName] = convertNumberFormatToArray(formatCode)
        multipliers[fieldName] = formats[fieldName] && Charts.symbolConversion.hasOwnProperty(formats[fieldName][4].toLowerCase()) ? Charts.symbolConversion[formats[fieldName][4].toLowerCase()] : 1
    })

    zVariables.forEach(variable => {

        const {fieldName, formatCode} = variable
        formats[fieldName] = convertNumberFormatToArray(formatCode)
        multipliers[fieldName] = formats[fieldName] && Charts.symbolConversion.hasOwnProperty(formats[fieldName][4].toLowerCase()) ? Charts.symbolConversion[formats[fieldName][4].toLowerCase()] : 1
    })

    return {indices, multipliers}
}

export const processSeriesRow = (fieldName, row, multipliers) => {

    // If the datapoint is null we have to pass null back to HighCharts. However null * 1 = 0 in
    // Javascript so we need set these as null still after the multiplier
    return [null, undefined].includes(row[fieldName]) ? null : typeof row[fieldName] === 'number' ? row[fieldName] * multipliers[fieldName] : row[fieldName]
}

export const checkForIsolatedPoint = (fieldName, newRow, newRowMinus1, newRowMinus2) => {

    // If the series is a line series then if it has no markers and a data point has
    // a null before it and after it then it won't show. Now because we do filtering
    // in this function to do the check we can only see if a datapoint is null after
    // we have processed the next row. This is why we have newRowMinus1 & newRowMinus2.
    // TODO Deal with single datasets, first and the last data point
    const nullBefore = newRowMinus2 !== null && newRowMinus2[fieldName] === null
    const notNullNow = newRowMinus1 !== null && newRowMinus1[fieldName] !== null
    const nullAfter = newRow !== null && newRow[fieldName] === null

    // This is for the previous point added to the series, to cope with it being surrounded by missing values
    // which means a line won't show these points
    return notNullNow && nullBefore && nullAfter
}

export const makePoint = (fieldName, indices, series) => {

    indices[fieldName].forEach(index => {

        const oldDataPoint = series[index].data[series[index].data.length - 1]

        // Set an object as the value for the point in the series if it has null data before and after. This allows
        // us to specify a separate style for these points - different to the general styles for the series
        series[index].data[series[index].data.length - 1] = {
            x: oldDataPoint[0],
            y: oldDataPoint[1]
        }
    })

    return {series}
}


// TODO this can be optimised by passing in the existing series object and working out what changed
export const addSeriesData = (data, xVariables, y1Variables, y2Variables, series, selectedSegmentationOptions) => {

    // First we need to create some lookups before we start to iterate through the dataset
    // These are all keyed by the variable fieldName
    const {indices, multipliers} = getSeriesLookups(series, xVariables, y1Variables, y2Variables)

    const yVariables = getUniqueRowsOfArrayOfObjects([...y1Variables, ...y2Variables], ["fieldName", "formatCode", "fieldType"])

    // Get the datetime variables in the chart data
    const datetimeVariables = getUniqueRowsOfArrayOfObjects(getAllDateAndDatetimeVariables([...yVariables, ...xVariables]), ["fieldName", "formatCode"])

    // Track some information across the rows
    let newRow = null, newRowMinus1 = null, newRowMinus2 = null

    const xFieldName = xVariables && xVariables.length > 0 ? xVariables[0].fieldName : null
    // TODO dont think we need xFieldTYpe

    data.forEach((row, i) => {

        // Is the row in one of the user selected segments to plot
        const keepRow = filterRowForSegmentation(row, selectedSegmentationOptions)

        // If the row is not in a segment we need then skip to the next row
        if (!keepRow) return

        // Convert the date and datetime columns to the format understood by High Charts
        newRow = convertDateTimeVariables(row, datetimeVariables)

        // The single first x-axis variable is used as the x value for the data points. We already converted
        // it if it is a date but now we need to handle the case that its a number. Specifically we need to
        // unwind the scaling due to the numericSymbols e.g. "bn"
        const xValue = processSeriesRow(xFieldName, newRow, multipliers)

        // Keep track of the last two rows added to the series
        if (i > 1) newRowMinus2 = newRowMinus1
        if (i > 0) newRowMinus1 = newRow

        yVariables.forEach(variable => {

            const {fieldName} = variable

            // If the datapoint is null we have to pass null back to HighCharts. However null * 1 = 0 in
            // Javascript so we need set these as null still after the multiplier
            const dataValue = processSeriesRow(fieldName, newRow, multipliers)

            // Check if the previous point in the time series is isolated, we can only do this when we
            // are evaluating the the point after the row after the isolated point is processed
            const previousPointIsIsolated = checkForIsolatedPoint(fieldName, newRow, newRowMinus1, newRowMinus2)

            if (previousPointIsIsolated) {

                // Convert the isolated point to a point
                series = makePoint(fieldName, indices, series)
            }

            // We use forEach because the same variable can be on the y1 and y2 axis in which case there are two series to update
            indices[fieldName].forEach(index => {
                series[index].data.push([xValue, dataValue])
            })
        })
    })

    return series
}


// TODO this can be optimised by passing in the existing series object and working out what changed
export const addLineSeriesDataWithError = (data, xVariables, y1Variables, e1Variables, e2Variables, series, selectedSegmentationOptions) => {

    // First we need to create some lookups before we start to iterate through the dataset
    // These are all keyed by the variable fieldName
    const {indices, multipliers} = getSeriesLookups(series, xVariables, y1Variables, [...e1Variables, ...e2Variables])

    // Get the datetime variables in the chart data
    const datetimeVariables = getUniqueRowsOfArrayOfObjects(getAllDateAndDatetimeVariables([...y1Variables, ...xVariables, ...e1Variables, ...e2Variables]), ["fieldName", "formatCode"])

    // Track some information across the rows
    let newRow = null, newRowMinus1 = null, newRowMinus2 = null

    const xFieldName = xVariables && xVariables.length > 0 ? xVariables[0].fieldName : null

    data.forEach((row, i) => {

        // Is the row in one of the user selected segments to plot
        const keepRow = filterRowForSegmentation(row, selectedSegmentationOptions)

        // If the row is not in a segment we need then skip to the next row
        if (!keepRow) return

        // Convert the date and datetime columns to the format understood by High Charts
        newRow = convertDateTimeVariables(row, datetimeVariables)

        // The single first x-axis variable is used as the x value for the data points. We already converted
        // it if it is a date but now we need to handle the case that its a number. Specifically we need to
        // unwind the scaling due to the numericSymbols e.g. "bn"
        const xValue = processSeriesRow(xFieldName, newRow, multipliers)

        // Keep track of the last two rows added to the series
        if (i > 1) newRowMinus2 = newRowMinus1
        if (i > 0) newRowMinus1 = newRow

        y1Variables.forEach(variable => {

            const {fieldName} = variable

            // If the datapoint is null we have to pass null back to HighCharts. However null * 1 = 0 in
            // Javascript so we need set these as null still after the multiplier
            const dataValue = processSeriesRow(fieldName, newRow, multipliers)

            // Check if the previous point in the time series is isolated, we can only do this when we
            // are evaluating the the point after the row after the isolated point is processed
            const previousPointIsIsolated = checkForIsolatedPoint(fieldName, newRow, newRowMinus1, newRowMinus2)

            if (previousPointIsIsolated) {

                // Convert the isolated point to a point
                series = makePoint(fieldName, indices, series)
            }

            const {fieldName: e1FieldName} = e1Variables.length > 0 ? e1Variables[0] : null
            const {fieldName: e2FieldName} = e2Variables.length > 0 ? e2Variables[0] : null

            // If the error bars are set get the values
            // TODO let this be set as a difference variable
            const error1DataValue = processSeriesRow(e1FieldName, newRow, multipliers)
            const error2DataValue = processSeriesRow(e2FieldName, newRow, multipliers)

            // Add the data to the first y axis
            series[0].data.push([xValue, dataValue])
            // And the errors as two elements in the second axis
            series[1].data.push([xValue, error1DataValue, error2DataValue])
        })
    })

    return series
}

// TODO this can be optimised by passing in the existing series object and working out what changed
export const addColumnSeriesData = (data, xVariables, y1Variables, y2Variables, series, selectedSegmentationOptions) => {

    // First we need to create some lookups before we start to iterate through the dataset
    // These are all keyed by the variable fieldName
    const {indices, multipliers} = getSeriesLookups(series, xVariables, y1Variables, y2Variables)

    const yVariables = getUniqueRowsOfArrayOfObjects([...y1Variables, ...y2Variables], ["fieldName", "formatCode", "fieldType"])

    // Get the datetime variables in the chart data
    const datetimeVariables = getUniqueRowsOfArrayOfObjects(getAllDateAndDatetimeVariables([...yVariables, ...xVariables]), ["fieldName", "formatCode"])

    let newRow = null

    const xFieldName = xVariables && xVariables.length > 0 ? xVariables[0].fieldName : null

    // In column charts we need to create an array of the categories for the x axis
    let categories = []
    let minimumValueY1 = null
    let minimumValueY2 = null

    data.forEach(row => {

        // Is the row in one of the user selected segments to plot
        const keepRow = filterRowForSegmentation(row, selectedSegmentationOptions)

        // If the row is not in a segment we need then skip to the next row
        if (!keepRow) return

        // Convert the date and datetime columns to the format understood by High Charts
        newRow = convertDateTimeVariables(row, datetimeVariables)

        // The single first x-axis variable is used as the x value for the data points. We already converted
        // it if it is a date but now we need to handle the case that its a number. Specifically we need to
        // unwind the scaling due to the numericSymbols e.g. "bn"
        const xValue = processSeriesRow(xFieldName, newRow, multipliers)

        categories.push(typeof xValue === "string" && xValue.split(" ").length - 1 === 1 ? xValue.replace(" ", " ") : xValue)

        yVariables.forEach(variable => {

            const {fieldName} = variable

            // If the datapoint is null we have to pass null back to HighCharts. However null * 1 = 0 in
            // Javascript so we need set these as null still after the multiplier
            const dataValue = processSeriesRow(fieldName, newRow, multipliers)

            // We use forEach because the same variable can be on the y1 and y2 axis in which case there are two series to update
            indices[fieldName].forEach(index => {
                series[index].data.push(dataValue)
                if (series[index].yAxis === 0 && (!minimumValueY1 || dataValue < minimumValueY1)) {
                    minimumValueY1 = dataValue
                } else if (series[index].yAxis === 1 && (!minimumValueY2 || dataValue < minimumValueY2)) {
                    minimumValueY2 = dataValue
                }
            })
        })
    })

    return {series, categories, minimumValueY1, minimumValueY2}
}

// TODO this can be optimised by passing in the existing series object and working out what changed
export const addWaterfallSeriesData = (data, xVariables, y1Variables, y2Variables, zVariables, series, selectedSegmentationOptions) => {

    // First we need to create some lookups before we start to iterate through the dataset
    // These are all keyed by the variable fieldName
    const {indices, multipliers} = getSeriesLookups(series, xVariables, y1Variables, y2Variables)

    const yVariables = getUniqueRowsOfArrayOfObjects([...y1Variables, ...y2Variables], ["fieldName", "formatCode", "fieldType"])

    // Get the datetime variables in the chart data
    const datetimeVariables = getUniqueRowsOfArrayOfObjects(getAllDateAndDatetimeVariables([...yVariables, ...xVariables, ...zVariables]), ["fieldName", "formatCode"])

    let newRow = null

    const xFieldName = xVariables && xVariables.length > 0 ? xVariables[0].fieldName : null

    // In waterfall charts we need to create an array of the categories for the x axis
    let categories = []
    let minimumValueY1 = null
    let minimumValueY2 = null

    data.forEach((row,i) => {

        // Is the row in one of the user selected segments to plot
        const keepRow = filterRowForSegmentation(row, selectedSegmentationOptions)

        // If the row is not in a segment we need then skip to the next row
        if (!keepRow) return

        // Convert the date and datetime columns to the format understood by High Charts
        newRow = convertDateTimeVariables(row, datetimeVariables)

        // The single first x-axis variable is used as the x value for the data points. We already converted
        // it if it is a date but now we need to handle the case that its a number. Specifically we need to
        // unwind the scaling due to the numericSymbols e.g. "bn"
        const xValue = processSeriesRow(xFieldName, newRow, multipliers)

        // Put category labels onto two lines if they are simple two word labels
        categories.push((xValue.split(" ").length - 1) === 1 ? xValue.replace(" ", " ") : xValue)


        const {fieldName: zFieldName} = zVariables.length > 0 ? zVariables[0] : {}

        yVariables.forEach(variable => {

            const {fieldName} = variable

            // If the datapoint is null we have to pass null back to HighCharts. However null * 1 = 0 in
            // Javascript so we need set these as null still after the multiplier
            const dataValue = processSeriesRow(fieldName, newRow, multipliers)

            const zDataValue = processSeriesRow(zFieldName, newRow, multipliers)

            // We use forEach because the same variable can be on the y1 and y2 axis in which case there are two series to update
            indices[fieldName].forEach(index => {

                if (zVariables.length > 0) {

                    if (i === 0) {
                        // To make the start look like a total we have to colour the first bar as a total
                        // but tell HighCharts it's a change amount. Otherwise HighCharts will sum the first point as 0
                        series[index].data.push({y: dataValue, isIntermediateSum:  false,  color: getThemeColour("--primary-text")})

                    } else {

                        series[index].data.push({y: zDataValue === true ? undefined : dataValue,isIntermediateSum:  zDataValue,  color: zDataValue === true ? getThemeColour("--primary-text") : undefined})
                    }

                } else {
                    series[index].data.push(dataValue)
                }

                if (series[index].yAxis === 0 && (!minimumValueY1 || dataValue < minimumValueY1)) {
                    minimumValueY1 = dataValue
                } else if (series[index].yAxis === 1 && (!minimumValueY2 || dataValue < minimumValueY2)) {
                    minimumValueY2 = dataValue
                }
            })
        })
    })

    if ( zVariables.length === 0) {
         yVariables.forEach(variable => {

        const {fieldName} = variable

         indices[fieldName].forEach(index => {
                series[index].data.push({isIntermediateSum: true, color: getThemeColour("--primary-text")})
            })

    })

    categories.push("Total")
    }


    console.log(series)

    return {series, categories, minimumValueY1, minimumValueY2}
}

// TODO this can be optimised by passing in the existing series object and working out what changed
export const addColumnSeriesDataWithError = (data, xVariables, y1Variables, e1Variables, e2Variables, series, selectedSegmentationOptions) => {

    // First we need to create some lookups before we start to iterate through the dataset
    // These are all keyed by the variable fieldName
    const {indices, multipliers} = getSeriesLookups(series, xVariables, y1Variables, [...e1Variables, ...e2Variables])

    // Get the datetime variables in the chart data
    const datetimeVariables = getUniqueRowsOfArrayOfObjects(getAllDateAndDatetimeVariables([...y1Variables, ...xVariables, ...e1Variables, ...e2Variables]), ["fieldName", "formatCode"])

    let newRow = null

    const xFieldName = xVariables && xVariables.length > 0 ? xVariables[0].fieldName : null

    // In column charts we need to create an array of the categories for the x axis
    let categories = []
    let minimumValueY1 = null
    let minimumValueY2 = null

    data.forEach(row => {

        // Is the row in one of the user selected segments to plot
        const keepRow = filterRowForSegmentation(row, selectedSegmentationOptions)

        // If the row is not in a segment we need then skip to the next row
        if (!keepRow) return

        // Convert the date and datetime columns to the format understood by High Charts
        newRow = convertDateTimeVariables(row, datetimeVariables)

        // The single first x-axis variable is used as the x value for the data points. We already converted
        // it if it is a date but now we need to handle the case that its a number. Specifically we need to
        // unwind the scaling due to the numericSymbols e.g. "bn"
        const xValue = processSeriesRow(xFieldName, newRow, multipliers)

        categories.push(xValue)

        y1Variables.forEach(variable => {

            const {fieldName} = variable

            // If the datapoint is null we have to pass null back to HighCharts. However null * 1 = 0 in
            // Javascript so we need set these as null still after the multiplier
            const dataValue = processSeriesRow(fieldName, newRow, multipliers)

            const {fieldName: e1FieldName} = e1Variables.length > 0 ? e1Variables[0] : null
            const {fieldName: e2FieldName} = e2Variables.length > 0 ? e2Variables[0] : null

            // If the error bars are set get the values
            // TODO let this be set as a difference variable
            const error1DataValue = processSeriesRow(e1FieldName, newRow, multipliers)
            const error2DataValue = processSeriesRow(e2FieldName, newRow, multipliers)

            // Add the data to the first y axis
            series[0].data.push(dataValue)
            // And the errors as two elements in the second axis
            series[1].data.push([error1DataValue, error2DataValue])

            if (series[0].yAxis === 0 && (!minimumValueY1 || dataValue < minimumValueY1)) {
                minimumValueY1 = dataValue
            }
        })
    })

    return {series, categories, minimumValueY1, minimumValueY2}
}

export const addAreaRangeSeriesData = (data, xVariables, y1Variables, y2Variables, series, selectedSegmentationOptions) => {

    // First we need to create some lookups before we start to iterate through the dataset
    // These are all keyed by the variable fieldName
    const {indices, multipliers} = getSeriesLookups(series, xVariables, y1Variables, y2Variables)

    const yVariables = [...y1Variables]

    // Get the datetime variables in the chart data
    const datetimeVariables = getUniqueRowsOfArrayOfObjects(getAllDateAndDatetimeVariables([...yVariables, ...xVariables, ...y2Variables]), ["fieldName", "formatCode"])

    let newRow = null

    const xFieldName = xVariables && xVariables.length > 0 ? xVariables[0].fieldName : null
    // TODO dont think we need xFieldTYpe

    data.forEach(row => {

        // Is the row in one of the user selected segments to plot
        const keepRow = filterRowForSegmentation(row, selectedSegmentationOptions)

        // If the row is not in a segment we need then skip to the next row
        if (!keepRow) return

        // Convert the date and datetime columns to the format understood by High Charts
        newRow = convertDateTimeVariables(row, datetimeVariables)

        // The single first x-axis variable is used as the x value for the data points. We already converted
        // it if it is a date but now we need to handle the case that its a number. Specifically we need to
        // unwind the scaling due to the numericSymbols e.g. "bn"
        const xValue = processSeriesRow(xFieldName, newRow, multipliers)

        yVariables.forEach(variable => {

            const {fieldName: upperFieldName} = variable

            // If the datapoint is null we have to pass null back to HighCharts. However null * 1 = 0 in
            // Javascript so we need set these as null still after the multiplier
            const upperDataValue = processSeriesRow(upperFieldName, newRow, multipliers)

            const {fieldName: lowerFieldName} = y2Variables.length > 0 ? y2Variables[0] : null

            // If the datapoint is null we have to pass null back to HighCharts. However null * 1 = 0 in
            // Javascript so we need set these as null still after the multiplier
            const lowerDataValue = processSeriesRow(lowerFieldName, newRow, multipliers)

            // There will only be one index for arearange charts but we use the same logic adding
            // an upper and lower series. Note how the series is tagged to the upper series name which
            // is on the y1-axis
            indices[upperFieldName].forEach(index => {
                series[index].data.push([xValue, lowerDataValue, upperDataValue])
            })
        })
    })

    return series
}

export const addPieSeriesData = (data, xVariables, y1Variables, series, selectedSegmentationOptions) => {

    // First we need to create some lookups before we start to iterate through the dataset
    // These are all keyed by the variable fieldName
    const {indices, multipliers} = getSeriesLookups(series, xVariables, y1Variables)

    const yVariables = getUniqueRowsOfArrayOfObjects(y1Variables, ["fieldName", "formatCode", "fieldType"])

    // Get the datetime variables in the chart data
    const datetimeVariables = getUniqueRowsOfArrayOfObjects(getAllDateAndDatetimeVariables([...yVariables, ...xVariables]), ["fieldName", "formatCode"])

    let newRow = null

    const xFieldName = xVariables && xVariables.length > 0 ? xVariables[0].fieldName : null

    data.forEach(row => {

        // Is the row in one of the user selected segments to plot
        const keepRow = filterRowForSegmentation(row, selectedSegmentationOptions)

        // If the row is not in a segment we need then skip to the next row
        if (!keepRow) return

        // Convert the date and datetime columns to the format understood by High Charts
        newRow = convertDateTimeVariables(row, datetimeVariables)

        // The single first x-axis variable is used as the x value for the data points. We already converted
        // it if it is a date but now we need to handle the case that its a number. Specifically we need to
        // unwind the scaling due to the numericSymbols e.g. "bn"
        const xValue = processSeriesRow(xFieldName, newRow, multipliers)

        yVariables.forEach(variable => {

            const {fieldName} = variable

            // If the datapoint is null we have to pass null back to HighCharts. However null * 1 = 0 in
            // Javascript so we need set these as null still after the multiplier
            const dataValue = processSeriesRow(fieldName, newRow, multipliers)

            // We use forEach because the same variable can be on the y1 and y2 axis in which case there are two series to update
            indices[fieldName].forEach(index => {
                series[index].data.push({
                    name: xValue,
                    y: dataValue
                })
            })
        })
    })

    return series
}

// TODO this can be optimised by passing in the existing series object and working out what changed
export const addHistogramSeriesData = (data, y1Variables, series, selectedSegmentationOptions) => {

    // First we need to create some lookups before we start to iterate through the dataset
    // These are all keyed by the variable fieldName
    const {multipliers} = getSeriesLookups(series, [], y1Variables, [])

    const yVariables = getUniqueRowsOfArrayOfObjects(y1Variables, ["fieldName", "formatCode", "fieldType"])

    // Get the datetime variables in the chart data
    const datetimeVariables = getUniqueRowsOfArrayOfObjects(getAllDateAndDatetimeVariables(y1Variables), ["fieldName", "formatCode"])

    let newRow

    data.forEach((row, i) => {

        // Is the row in one of the user selected segments to plot
        const keepRow = filterRowForSegmentation(row, selectedSegmentationOptions)

        // If the row is not in a segment we need then skip to the next row
        if (!keepRow) return

        // Convert the date and datetime columns to the format understood by High Charts
        newRow = convertDateTimeVariables(row, datetimeVariables)

        yVariables.forEach(variable => {

            const {fieldName} = variable

            // If the datapoint is null we have to pass null back to HighCharts. However null * 1 = 0 in
            // Javascript so we need set these as null still after the multiplier
            const dataValue = processSeriesRow(fieldName, newRow, multipliers)

            series[1].data.push(dataValue)
        })
    })

    return series
}

export const addOrganisationSeriesData = (data, xVariables, y1Variables, series, selectedSegmentationOptions) => {

    let nodes = {}

    data.forEach(row => {

        // Is the row in one of the user selected segments to plot
        const keepRow = filterRowForSegmentation(row, selectedSegmentationOptions)

        // If the row is not in a segment we need then skip to the next row
        if (!keepRow) return

        y1Variables.forEach(variable => {

            const {fieldName} = variable

            // If the datapoint is null we have to pass null back to HighCharts. However null * 1 = 0 in
            // Javascript so we need set these as null still after the multiplier
            const yDataValue = row[fieldName]

            const {fieldName: xFieldName} = xVariables.length > 0 ? xVariables[0] : null

            // Get the other values
            const xDataValue = row[xFieldName]

            series[0].data.push({from: xDataValue, to: yDataValue})

            // Add the details of the nodes in the data to the nodes objects
            if (!nodes.hasOwnProperty(xDataValue)) {

                nodes[xDataValue] = {id: xDataValue, name: xDataValue, title: null}
            }

            // Add the details of the nodes in the data to the nodes objects
            if (!nodes.hasOwnProperty(yDataValue)) {

                nodes[yDataValue] = {id: yDataValue, name: yDataValue, title: null}
            }
        })
    })

    const nodes2 = Object.keys(nodes).map(key => nodes[key])
    series[0].nodes = nodes2

    return series

}

export const addBubbleSeriesData = (data, xVariables, y1Variables, zVariables, series, selectedSegmentationOptions) => {

    // First we need to create some lookups before we start to iterate through the dataset
    // These are all keyed by the variable fieldName
    const {multipliers} = getSeriesLookups(series, xVariables, y1Variables, [], zVariables)

    const yVariables = getUniqueRowsOfArrayOfObjects(y1Variables, ["fieldName", "formatCode", "fieldType"])

    // Get the datetime variables in the chart data
    const datetimeVariables = getUniqueRowsOfArrayOfObjects(getAllDateAndDatetimeVariables([...yVariables, ...xVariables, ...zVariables]), ["fieldName", "formatCode"])

    let newRow

    data.forEach((row, i) => {

        // Is the row in one of the user selected segments to plot
        const keepRow = filterRowForSegmentation(row, selectedSegmentationOptions)

        // If the row is not in a segment we need then skip to the next row
        if (!keepRow) return

        // Convert the date and datetime columns to the format understood by High Charts
        newRow = convertDateTimeVariables(row, datetimeVariables)

        yVariables.forEach(variable => {

            const {fieldName} = variable

            // If the datapoint is null we have to pass null back to HighCharts. However null * 1 = 0 in
            // Javascript so we need set these as null still after the multiplier
            const dataValue = processSeriesRow(fieldName, newRow, multipliers)

            const {fieldName: xFieldName} = xVariables.length > 0 ? xVariables[0] : null
            const {fieldName: zFieldName} = zVariables.length > 0 ? zVariables[0] : null

            // Get the other values
            const xDataValue = processSeriesRow(xFieldName, newRow, multipliers)
            const zDataValue = processSeriesRow(zFieldName, newRow, multipliers)

            series[0].data.push({x: xDataValue, y: dataValue, z: zDataValue})
        })
    })

    return series
}

/**
 * A function that calculates the string to use as a label for a axis on the chart.
 * @param variables - The TRAC schema of the variables plotted on the axis.
 * @param userSetAxisLabels {string|null} The user set label for the axis.
 * @returns {string|null}
 */
export const setAxisLabel = (variables, userSetAxisLabels) => {

    if (userSetAxisLabels !== null) return userSetAxisLabels

    // If the user sets the label to be blank we take this as to have no label
    if (userSetAxisLabels === "") return null

    // With no variable set we add a default
    if (variables.length === 0) return "Not set"

    const uniqueVariableTypes = getUniqueVariableTypes(variables)
    const isAllNumbers = schemaIsAllNumbers(variables)

    // Eek its all a bit too complicated, we have multiple variable types and they are not all numbers
    // so we remove the label - the user can still set one
    if (uniqueVariableTypes.length > 1 && !isAllNumbers) return null

    // Some variables we omit a label
    // The split is in case we join datasets before creating a chart and we have
    // duplicate names (which we append with the dataset ID)
    if (uniqueVariableTypes.length === 1 && Charts.labelLessSeries.includes(variables[0].fieldName.split("||")[0])) return null

    // A single variable that is not a number
    if (uniqueVariableTypes.length === 1 && !isAllNumbers) {

        // The split is in case we join datasets before creating a chart and we have
        // duplicate names (which we append with the dataset ID)
        const fieldName = variables[0].fieldName.split("||")[0]
        return variables[0].fieldLabel || convertKeyToText(fieldName)
    }

    // A single variable that is a number - no label as the units will be on the axis
    // TODO check this - as when there are multiple numbers we have a label
    if (uniqueVariableTypes.length === 1 && isAllNumbers) return null

    // Now we have multiple variable types and they are all numbers. We can still set a label
    // but we need to use the format codes.
    const isMissingFormatCode = schemaIsMissingNumberFormatCode(variables)

    if (isMissingFormatCode) return "Unknown units"

    // Get the size labels from the config
    const sizes = Sizes.map(size => size.value.toUpperCase())

    // Get a list of the unique formats, this assumes that they are all the string versions
    const uniqueFormats = getUniqueNumberFormatCodes(variables)

    // If the format has a a numeric symbol such as "BN" then we don't include a label as
    // HighCharts will add that into the axis. However anything else like a "%" is added
    // as a label
    const uniqueUnits = [...new Set(uniqueFormats.map(format => sizes.includes(format[4].toUpperCase()) ? format[3] : format[3] + format[4]))]

    if (uniqueUnits.length > 1) return "Multiple units"

    if (uniqueUnits.length === 1 && uniqueUnits[0] !== "") return uniqueUnits[0]

    // No uniqueUnits and single unique unit but its a ""
    return null
}

/**
 * A function that calculates the string to use to either prepend or append the axis labels.
 * @param variables - The TRAC schema of the variables plotted on the axis.
 * @returns {{}}
 */
export const setAxisLabelFormatParts = (variables) => {

    let uniqueUnits = {pre: "", post: ""}

    const isAllNumbers = schemaIsAllNumbers(variables)

    // If we don't have all numbers on the axis then we don't set the axis format
    if (!isAllNumbers) return uniqueUnits

    // Now we have all number variables. We can still set a format, but we need to use the format codes.
    const isMissingFormatCode = schemaIsMissingNumberFormatCode(variables)

    if (isMissingFormatCode) return uniqueUnits

    // Get the size labels from the config
    const sizes = TracClassification.sizes.map(size => size.value.toUpperCase())

    // Get a list of the unique formats, this assumes that they are all the string versions
    const uniqueFormats = getUniqueFormatCodes(variables)

    const uniqueUnitsPre = [...new Set(uniqueFormats.map(format => format[3]))]
    // If the format has a a numeric symbol such as "BN" then we don't include a label as
    // HighCharts will add that into the axis. However anything else like a "%" is added
    // as a label
    const uniqueUnitsPost = [...new Set(uniqueFormats.map(format => sizes.includes(format[4].toUpperCase()) ? "" : format[4]))]

    if (uniqueUnitsPre.length === 1) {
        uniqueUnits.pre = uniqueUnitsPre[0]
    }

    if (uniqueUnitsPost.length === 1) {
        uniqueUnits.post = uniqueUnitsPost[0]
    }

    return uniqueUnits
}

/**
 * A function that returns a function for HighCharts to use to set an axis label formats. The actual
 * label format is set by HughCharts and this just adds the pre and post unit values for number
 * variables. For Date and datetimes it completely overwrites the label to be able to use the
 * formats in the config.
 * @param variables - The TRAC schema of the variables plotted on the axis.
 * @returns {function(): string}
 */
export const setAxisNumberFormatter = (chartType, variables) => {

    return function () {

        // Get the HighCharts format function
        const {defaultLabelFormatter} = this.axis

        // If the label is a datetime then overwrite the whole label
        // noinspection JSUnresolvedVariable
        if (this.dateTimeLabelFormat && variables.length > 0) {

            const formattedDate = convertUtcSecondsToDateObject(this.value)
            return formatDateObject(formattedDate, variables[0].formatCode)
        }

        // The default label for none date and datetimes
        const label = defaultLabelFormatter.call(this)

        // In a column chart is being plotted then the label will just
        // be the number - but we need to add a format that HighCharts does not do to
        // these types by default.
        if (["column", "bar", "columnWithError"].includes(chartType) && this.axis.isXAxis) {

            const {value} = this
            const formatCodeX = this.chart.series[0].userOptions.formatCodeX
            const formatArray = convertNumberFormatToArray(formatCodeX)
            const multiplier = formatArray && Charts.symbolConversion.hasOwnProperty(formatArray[4].toLowerCase()) ? Charts.symbolConversion[formatArray[4].toLowerCase()] : 1

            return applyNumberFormat(value / multiplier, formatCodeX)
        }

        // Work out what the pre and post text should be to show the units
        const formatParts = setAxisLabelFormatParts(variables)

        return `${formatParts.pre}${label}${formatParts.post}`
    }
}

export const formatPoint = (myValue, fieldType, formatCode) => {

    let formattedValue = myValue
    let format = undefined

    if (["INTEGER", "FLOAT", "DECIMAL"].includes(fieldType)) {

        format = convertNumberFormatToArray(formatCode)

        if (format) {

            const conversionFactor = GeneralChartConfig.symbolConversion[format[4].toLowerCase()]

            // If the format has numeric symbols then we need to account for this,
            // we will have refactored the number to plot it so we need to undo that
            if (conversionFactor) {
                formattedValue = formattedValue / conversionFactor
            }

            formattedValue = applyNumberFormat(formattedValue, format)
        }
    }

    if (["DATE", "DATETIME"].includes(fieldType)) {

        format = formatCode

        if (format) {
            formattedValue = convertUtcSecondsToDateObject(formattedValue)
            formattedValue = formatDateObject(formattedValue, format)
        }
    }

    return formattedValue
}

/**
 * A function that returns a function for HighCharts to use to set a tooltip format.
 * @returns {function(): string}
 */
export const setTooltipFormatter = () => {

    return function () {

        // We stored a custom properties to help with this part
        // TODO add write up
        let body, header

        if (this.hasOwnProperty("points")) {

            header = this.points.length === 0 ? "" : this.points[0]
            const {x, series: {userOptions: {fieldTypeX, formatCodeX}}} = header
            header = `<span style="font-size:0.85rem;">${formatPoint(x, fieldTypeX, formatCodeX)}</span><br/>`

            body = this.points.map((point) => {

                const {y, color, point: {high, low}, series: {userOptions: {fieldTypeY, formatCodeY}}} = point

                if (high && low) {
                    return `<span style="padding-right:0.5rem;font-size:0.875rem; color: ${color}">&#9632;</span><span>${formatPoint(low, fieldTypeY, formatCodeY)} - ${formatPoint(high, fieldTypeY, formatCodeY)}</span>`
                } else {
                    return `<span style="padding-right:0.5rem;font-size:0.875rem; color: ${color}">&#9632;</span><span>${formatPoint(y, fieldTypeY, formatCodeY)}</span><br/>`
                }

            }).join("")

        } else {

            // high and low are for ranges
            // x2 is used in histograms
            // z is used by bubble
            const {
                point: {percentage, category, name, x, y, x2, z, high, low, sum, from, to, weight},
                color,
                series: {
                    userOptions: {
                        fieldTypeX,
                        formatCodeX,
                        fieldTypeY,
                        formatCodeY,
                        fieldTypeZ,
                        formatCodeZ,
                        fieldLabelX,
                        fieldLabelY,
                        fieldLabelZ
                    },
                    color: seriesColor
                }
            } = this

            console.log(this)

            // Column and bar charts need to use the category property for the x-axis label
            // name is used by pie charts
            // sankey uses from and to and weight (weight is used when hovering over the link in a sankey)
            if (from && to) {
                header = `<span style="font-size:0.85rem;">${formatPoint(from, fieldTypeX, formatCodeX)} &#8594; ${formatPoint(to, fieldTypeY, formatCodeY)}</span><br/>`
            } else if (z) {
                header = `<span style="font-size:0.85rem;">${fieldLabelX}: ${formatPoint(x, fieldTypeX, formatCodeX)}</span><br/>`
            } else if (x2) {
                header = `<span style="font-size:0.85rem;">${formatPoint(x, fieldTypeX, formatCodeX)} - ${formatPoint(x2, fieldTypeX, formatCodeX)}}</span><br/>`
            } else if (category) {
                header = `<span style="font-size:0.85rem;">${formatPoint(category, fieldTypeX, formatCodeX, color)}</span><br/>`
            } else if (x2) {
                header = `<span style="font-size:0.85rem;">${formatPoint(x, fieldTypeX, formatCodeX)} - ${formatPoint(x2, fieldTypeX, formatCodeX)}}</span><br/>`
            } else if (name) {
                header = `<span style="font-size:0.85rem;">${formatPoint(name, fieldTypeX, formatCodeX, color)}</span><br/>`
            } else {
                header = `<span style="font-size:0.85rem;">${formatPoint(x, fieldTypeX, formatCodeX, color)}</span><br/>`
            }
            // arearange charts use high and low for the range
            // bubbles use z
            // sankey use sum (on nodes) and weight (on links)
            if (sum) {
                body = `<span style="font-size:0.85rem;">${formatPoint(sum, fieldTypeZ, formatCodeZ)}</span>`
            } else if (weight) {
                body = `<span style="font-size:0.85rem;">${formatPoint(weight, fieldTypeZ, formatCodeZ)}</span>`
            } else if (z) {
                body = `<span style="font-size:0.85rem;">${fieldLabelY}: ${formatPoint(y, fieldTypeY, formatCodeY)}</span><br/>`
                body = body + `<span style="font-size:0.85rem;">${fieldLabelZ}: ${formatPoint(z, fieldTypeZ, formatCodeZ)}</span>`
            } else if (high && low) {
                // SeriesColor is needed by waterfalls as the up and down colours are not passed through separately
                body = `<span style="padding-right:0.5rem;font-size:0.875rem; color: ${color || seriesColor}">&#9632;</span><span>${formatPoint(Math.min(low, high), fieldTypeY, formatCodeY)} - ${formatPoint(Math.max(low, high), fieldTypeY, formatCodeY)}</span>`
            } else {
                body = `<span style="padding-right:0.5rem;font-size:0.875rem; color: ${color || seriesColor}">&#9632;</span><span>${formatPoint(y, fieldTypeY, formatCodeY)}${percentage ? ` (${percentage.toFixed(1)}%)` : ""}</span>`
            }
        }

        return `${header}${body}`
    }
}

/**
 * A function that returns a function for HighCharts to use to set a Legend format. It
 * basically adds "left" or "right" to the label when there are dual axes.
 * @returns {function(): string}
 */
export const setLegendFormatter = (chartType, y1Variables, y2Variables) => {

    return function () {

        const {yAxis: {opposite}, name, userOptions: {type}} = this

        // If there are no y1 or y2 axis variables then we dont do anything
        // different to the default
        if (!y1Variables || y1Variables.length === 0 || !y2Variables || y2Variables.length === 0) return name

        if (opposite === false) {

            return `${name} (${type === "bar" ? "bottom" : "left"})`

        } else if (opposite === true) {

            return `${name} (${type === "bar" ? "top" : "right"})`
        }
    }
}

export const setDataLabelFormatter = () => {

    return function () {
        const {y, series: {userOptions: {fieldTypeY, formatCodeY}}} = this
        return formatPoint(y, fieldTypeY, formatCodeY)
    }
}

export const setDataLabelFormatterFromKey = () => {

    return function () {

        const {key, series: {userOptions: {fieldTypeX, formatCodeX}}} = this
        return formatPoint(key, fieldTypeX, formatCodeX)
    }
}

/**
 * A function that sets the x-axis type in HighCharts. When set to datetime
 * HighCharts will use the datetime formats for the axis.
 * @param variables - The TRAC schema of the variables plotted on the axis.
 * @returns -
 */
export const getAxisType = (variables) => {

    if (schemaIsAllDatesOrDatetimes(variables)) return "datetime"

    return "linear"
}

export const handleLegendSeriesClick = (chartType, chartComponent) => {

    return function () {

        // "this" is the chart
        const series = this.chart.series, allSeriesVis = series.filter(series => series.userOptions.type !== "errorbar").map(series => series.visible)
        let isSeriesVis;

        // The click will turn the visibility of the chosen series to its opposite if we
        // return true from this function, so dummy that off
        allSeriesVis[this.index] = !allSeriesVis[this.index];
        // Will there be a visible series after the click
        isSeriesVis = allSeriesVis.some(vis => vis);

        // In histograms we have two x and two y axes. One for the underlying data
        // and one for the histogram. If you hide one series then the series is made
        // invisible but the label still shows which is confusing. So we update the
        // chart to hide the label
        if (isSeriesVis && chartType === "histogram") {

            // If the user clicked on the histogram series
            if (this.index === 0) {
                // Hide the first axis index which corresponds to the data
                chartComponent.current.chart.xAxis[1].update({visible: !series[this.index].visible})
                chartComponent.current.chart.yAxis[1].update({visible: !series[this.index].visible})

            } else if (this.index === 1) {
                // Else they clicked on the data series
                // Hide the second axis index which corresponds to the histogram
                chartComponent.current.chart.xAxis[0].update({visible: !series[this.index].visible})
                chartComponent.current.chart.yAxis[0].update({visible: !series[this.index].visible})
            }
        }

        if (!isSeriesVis) return false;
    }
}