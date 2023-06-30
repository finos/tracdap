// import React, {useRef, useState, useMemo} from "react";
// import PropTypes from "prop-types";
// import {GeneralChartConfig, ChartDefinitions} from "../../../config/config_charts";
// import Highcharts from "highcharts/highcharts";
// import HighchartsReact from "highcharts-react-official";
// // Enable chart exporting
// import exporting from "highcharts/modules/exporting";
// import offlineExporting from "highcharts/modules/offline-exporting";
// // Enable sankey chart type
// import sankey from "highcharts/modules/sankey";
// // Enable arearange chart
// import highChartsMore from "highcharts/highcharts-more";
// // Enable histogram
// import histogramBellCurve from "highcharts/modules/histogram-bellcurve"
// // Enable dependencywheel
// import dependencyWheel from "highcharts/modules/dependency-wheel"
//
// import {
//     convertStringToArray,
//     getUniqueRowsOfArrayOfObjects,
//     sortBy,
//     schemaIsAllIntegers,
//     setDownloadName, getThemeColour
// } from "../../utils/utils";
//
// import {
//     setInitialSelectedVariables,
//     getVariableOptions,
//     getSegmentationVariables,
//     setVariableLabels,
//     getSegmentationOptions,
//     setInitialSelectedSegmentationOptions,
//     setAxisMinInterval,
//     setSeriesForLineChart,
//     addSeriesData,
//     setTooltipFormatter,
//     setLegendFormatter,
//     setAxisLabel,
//     setAxisNumberFormatter,
//     getAxisType,
//     setDataLabelFormatterFromKey,
//     setDataLabelFormatter,
//     handleLegendSeriesClick,
//     setSeriesForAreaRangeChart,
//     addAreaRangeSeriesData,
//     addColumnSeriesData,
//     setSeriesForHistogramChart,
//     addPieSeriesData,
//     addHistogramSeriesData,
//     setSeriesForChartWithError,
//     addLineSeriesDataWithError,
//     addColumnSeriesDataWithError,
//     setSeriesForBubbleChart,
//     addBubbleSeriesData,
//     getHighChartType,
//     setSeriesForStructureChart,
//     addStructureSeriesData,
//     setHighchartsStyles,
//     setSeriesForWaterfallChart,
//     addWaterfallSeriesData
// } from "../../utils/utils_chart";
//
// import Col from "react-bootstrap/Col";
// import Row from "react-bootstrap/Row";
// import {SelectOption} from "../SelectOption";
// import DropdownMenu from "./DropdownMenu"
// import {useSelector} from "react-redux";
// import {HeaderTitle} from "../HeaderTitle";
//
// // Add the imported tools to the Highcharts API object we use
// exporting(Highcharts)
// offlineExporting(Highcharts)
//
// // Needed for arearange and other charts
// highChartsMore(Highcharts)
//
// // Needed for histogram charts
// histogramBellCurve(Highcharts)
//
// // Needed for sankey charts
// sankey(Highcharts)
//
// // Needed for dependencyWheel charts
// dependencyWheel(Highcharts)
//
// /**
//  * A component that shows a line chart.
//  *
//  * @returns {JSX.Element}
//  */
// const Chart = props => {
//
//     const {
//         data,
//         schema,
//         chartType,
//         userSetVariableLabels,
//         useLineStyles,
//         showMarkers,
//         markerSize,
//         userSetAxisLabels,
//         showDataLabels,
//         useClientColours,
//         chartZoom,
//         inverted,
//         chartHeight,
//         chartHeightIsPercent,
//         margin,
//         spacing,
//         canExport,
//         tooltipIsShared,
//         legendIsEnabled,
//         allowAllSegmentsToBePlotted,
//         hideAxes,
//         lineWidth,
//         fontSize,
//         showTooltip,
//         header,
//         pointPadding,
//         circleDoughnutOptions,
//         pieOptions,
//         semiDoughnutOptions,
//         title
//     } = props
//
//     // Get what we need from the store
//     const {login: {userId}} = useSelector(state => state["applicationStore"])
//
//     // Add in font styles and numeric separators
//     setHighchartsStyles(Highcharts)
//
//     // Get the chart options from the config
//     const {[chartType]: {defaultNumberOfSeriesSelected, maxSeriesPerAxis}} = ChartDefinitions
//
//     const {colours} = GeneralChartConfig
//
//     // Overwrite the default function to show the Reset zoom button when the user zooms. This is so we can use our own button.
//     // See https://stackoverflow.com/questions/66536206/styling-highcharts-reset-zoom-button-with-material-ui-button
//     Highcharts.Chart.prototype.showResetZoom = function () {
//     };
//
//     // Set the legend symbol drawing function for line charts to the function that runs for column charts
//     // This allows us to set square symbols. If the user is using line styles dont do this so that the legend contains the styles
//     // See https://stackoverflow.com/questions/27510810/highcharts-make-the-legend-symbol-a-square-or-rectangle
//     //if (!useLineStyles) {
//     Highcharts["seriesTypes"].line.prototype.drawLegendSymbol = Highcharts["seriesTypes"].column.prototype.drawLegendSymbol;
//     //}
//
//     // A reference to the chart in the UI (again zoom related as we moved the buttons to outside the chart)
//     const chartComponent = useRef(null);
//
//     // Get the hidden columns, this can be a string or an array of column names
//     const hiddenColumns = convertStringToArray(props.hiddenColumns)
//
//     // Get the list of variables to show by default when the chart loads
//     const defaultXVariables = props.defaultXVariables !== "auto" ? convertStringToArray(props.defaultXVariables) : "auto"
//     const defaultY1Variables = props.defaultY1Variables !== "auto" ? convertStringToArray(props.defaultY1Variables) : "auto"
//     const defaultY2Variables = props.defaultY2Variables !== "auto" ? convertStringToArray(props.defaultY2Variables) : "auto"
//     const defaultZVariables = props.defaultZVariables !== "auto" ? convertStringToArray(props.defaultZVariables) : "auto"
//     const defaultE1Variables = props.defaultE1Variables !== "auto" ? convertStringToArray(props.defaultE1Variables) : "auto"
//     const defaultE2Variables = props.defaultE2Variables !== "auto" ? convertStringToArray(props.defaultE2Variables) : "auto"
//     const defaultSegmentationVariables = props.defaultSegmentationVariables !== "auto" ? convertStringToArray(props.defaultSegmentationVariables) : "auto"
//
//     // Get a list of the Y variable options to put in a select. x-axis variables are not eligible y-axis variables and vice vesa
//     const xVariableOptions = getVariableOptions(chartType, schema, hiddenColumns, "x")
//     const yVariableOptions = getVariableOptions(chartType, schema, hiddenColumns, "y")
//     const zVariableOptions = getVariableOptions(chartType, schema, hiddenColumns, "z")
//     const eVariableOptions = getVariableOptions(chartType, schema, hiddenColumns, "e")
//
//     // Get the variables to use a segmentation options
//     const segmentationVariables = getSegmentationVariables(schema, defaultSegmentationVariables, hiddenColumns, defaultXVariables, defaultY1Variables, defaultY2Variables)
//
//     // Get the unique combinations of segment variables - knowing this allows us to present a set of segment options
//     // that always have available data
//     const uniqueSegmentationCombinations = getUniqueRowsOfArrayOfObjects(data, segmentationVariables)
//
//     // Convert the segmentation variable dataset values into a set of options
//     // TODO the labels in the options should be updated to reflect the formats
//     const segmentationOptions = getSegmentationOptions(segmentationVariables, uniqueSegmentationCombinations, allowAllSegmentsToBePlotted)
//
//     // Save the variable selections into the store
//     const [selectedXVariables, setSelectedXVariables] = useState(setInitialSelectedVariables(xVariableOptions, defaultXVariables, defaultNumberOfSeriesSelected.x, maxSeriesPerAxis.x))
//     const [selectedY1Variables, setSelectedY1Variables] = useState(setInitialSelectedVariables(yVariableOptions, defaultY1Variables, defaultNumberOfSeriesSelected.y1, maxSeriesPerAxis.y1))
//     const [selectedY2Variables, setSelectedY2Variables] = useState(setInitialSelectedVariables(yVariableOptions, defaultY2Variables, defaultNumberOfSeriesSelected.y2, maxSeriesPerAxis.y2))
//     const [selectedZVariables, setSelectedZVariables] = useState(setInitialSelectedVariables(zVariableOptions, defaultZVariables, defaultNumberOfSeriesSelected.z, maxSeriesPerAxis.z))
//     const [selectedE1Variables, setSelectedE1Variables] = useState(setInitialSelectedVariables(eVariableOptions, defaultE1Variables, defaultNumberOfSeriesSelected.e1, maxSeriesPerAxis.e1))
//     const [selectedE2Variables, setSelectedE2Variables] = useState(setInitialSelectedVariables(eVariableOptions, defaultE2Variables, defaultNumberOfSeriesSelected.e1, maxSeriesPerAxis.e1))
//     const [selectedSegmentationOptions, setSelectedSegmentationOptions] = useState(setInitialSelectedSegmentationOptions(segmentationVariables, uniqueSegmentationCombinations, segmentationOptions, allowAllSegmentsToBePlotted))
//     const [isZoomed, setIsZoomed] = useState(false)
//     const [showMenu, setShowMenu] = useState(false);
//
//     /**
//      * A function that runs when the user changes a segmentation variable option.
//      * @param payload - The value returned by the SelectOption component
//      */
//     const handleSegmentVariableChange = (payload) => setSelectedSegmentationOptions((state) => ({...state, [payload.id]: payload.value}))
//
//     /**
//      * A function that runs when the user changes a y1-axis variable option.
//      * @param payload - The value returned by the SelectOption component
//      */
//     const handleY1VariableChange = (payload) => setSelectedY1Variables(payload.value)
//
//     /**
//      * A function that runs when the user changes a y2-axis variable option.
//      * @param payload - The value returned by the SelectOption component
//      */
//     const handleY2VariableChange = (payload) => setSelectedY2Variables(payload.value)
//
//     /**
//      * A function that runs when the user changes an x-axis variable option.
//      * @param payload - The value returned by the SelectOption component
//      */
//     const handleXVariableChange = (payload) => setSelectedXVariables(payload.value)
//
//     /**
//      * A function that runs when the user changes a z-axis variable option.
//      * @param payload - The value returned by the SelectOption component
//      */
//     const handleZVariableChange = (payload) => setSelectedZVariables(payload.value)
//
//     /**
//      * A function that runs when the user changes a e1-axis variable option.
//      * @param payload - The value returned by the SelectOption component
//      */
//     const handleE1VariableChange = (payload) => setSelectedE1Variables(payload.value)
//
//     /**
//      * A function that runs when the user changes a e2-axis variable option.
//      * @param payload - The value returned by the SelectOption component
//      */
//     const handleE2VariableChange = (payload) => setSelectedE2Variables(payload.value)
//
//     const config = useMemo(() => {
//
//         // Get the labels to use for the segmentation variables
//         const variableLabels = setVariableLabels(schema, userSetVariableLabels)
//
//         // Sort the data by the x axis variable unless if a waterfall as the order matters
//         let newData = selectedXVariables.length > 0 && chartType !== "waterfall" ? sortBy(data, selectedXVariables[0].fieldName) : data
//
//         let series, categories, minimumValueY1, minimumValueY2
//
//         //         "area",//
//         // x "arearange",//
//         // "areaspline",//
//         // x"bar",//
//         // x"bubble",//
//         // x"bullet",
//         // x"column",//
//         // "columnrange",
//         // "x dependencywheel",//
//         // "columnWithError",//
//         // x"histogram",//
//         // "line",//
//         // "lineWithError",//
//         // "pie",//
//         // "circleDoughnut",//
//         // "semiDoughnut",//
//         // x "pie",//
//         // x"sankey",//
//         // "scatter",//
//         // x"solidgauge",//
//         // "spline",//
//         // x"waterfall"
//
//
//         if (getHighChartType(chartType) === "pie") {
//
//             // Initialise the definitions of the series
//             series = setSeriesForLineChart(chartType, selectedXVariables, selectedY1Variables, selectedY2Variables, useLineStyles, variableLabels)
//             // Add the series data, this also filters for the segmentation selections, converts the date & datetime variables and checks for isolated data points
//             series = addPieSeriesData(newData, selectedXVariables, selectedY1Variables, series, selectedSegmentationOptions)
//
//         } else if (chartType === "arearange") {
//
//             // Initialise the definitions of the series
//             series = setSeriesForAreaRangeChart(chartType, selectedXVariables, selectedY1Variables, useLineStyles, variableLabels)
//             // Add the series data, this also filters for the segmentation selections, converts the date & datetime variables and checks for isolated data points
//             series = addAreaRangeSeriesData(newData, selectedXVariables, selectedY1Variables, selectedY2Variables, series, selectedSegmentationOptions)
//
//         } else if (["column", "bar"].includes(chartType)) {
//
//             // Initialise the definitions of the series
//             series = setSeriesForLineChart(chartType, selectedXVariables, selectedY1Variables, selectedY2Variables, useLineStyles, variableLabels)
//             // Add the series data, this also filters for the segmentation selections, converts the date & datetime variables and checks for isolated data points
//             const results = addColumnSeriesData(newData, selectedXVariables, selectedY1Variables, selectedY2Variables, series, selectedSegmentationOptions)
//
//             series = results.series
//             categories = results.categories
//             minimumValueY1 = results.minimumValueY1
//             minimumValueY2 = results.minimumValueY2
//
//         } else if (["histogram"].includes(chartType)) {
//
//             // Initialise the definitions of the series
//             series = setSeriesForHistogramChart(chartType, selectedY1Variables)
//             // Add the series data, this also filters for the segmentation selections, converts the date & datetime variables and checks for isolated data points
//             series = addHistogramSeriesData(newData, selectedY1Variables, series, selectedSegmentationOptions)
//
//
//         } else if (["lineWithError"].includes(chartType)) {
//
//             // Initialise the definitions of the series
//             series = setSeriesForChartWithError(chartType, selectedXVariables, selectedY1Variables, selectedE1Variables, selectedE2Variables, useLineStyles, variableLabels)
//             // Add the series data, this also filters for the segmentation selections, converts the date & datetime variables and checks for isolated data points
//             series = addLineSeriesDataWithError(newData, selectedXVariables, selectedY1Variables, selectedE1Variables, selectedE2Variables, series, selectedSegmentationOptions)
//
//         } else if (["columnWithError"].includes(chartType)) {
//
//             // Initialise the definitions of the series
//             series = setSeriesForChartWithError(chartType, selectedXVariables, selectedY1Variables, selectedE1Variables, selectedE2Variables, useLineStyles, variableLabels)
//             // Add the series data, this also filters for the segmentation selections, converts the date & datetime variables and checks for isolated data points
//             const results = addColumnSeriesDataWithError(newData, selectedXVariables, selectedY1Variables, selectedE1Variables, selectedE2Variables, series, selectedSegmentationOptions)
//
//             series = results.series
//             categories = results.categories
//             minimumValueY1 = results.minimumValueY1
//             minimumValueY2 = results.minimumValueY2
//
//         } else if (["bubble"].includes(chartType)) {
//
//             // Initialise the definitions of the series
//             series = setSeriesForBubbleChart(chartType, selectedXVariables, selectedY1Variables, selectedZVariables, useLineStyles, variableLabels)
//             // Add the series data, this also filters for the segmentation selections, converts the date & datetime variables and checks for isolated data points
//             series = addBubbleSeriesData(newData, selectedXVariables, selectedY1Variables, selectedZVariables, series, selectedSegmentationOptions)
//
//         } else if (["sankey", "dependencywheel"].includes(chartType)) {
//
//             // Initialise the definitions of the series
//             series = setSeriesForStructureChart(chartType, selectedXVariables, selectedY1Variables, selectedZVariables, useLineStyles, variableLabels)
//             // Add the series data, this also filters for the segmentation selections, converts the date & datetime variables and checks for isolated data points
//             series = addStructureSeriesData(newData, selectedXVariables, selectedY1Variables, selectedZVariables, series, selectedSegmentationOptions)
//
//         } else if (["waterfall"].includes(chartType)) {
//
//             // Initialise the definitions of the series
//             series = setSeriesForWaterfallChart(chartType, selectedXVariables, selectedY1Variables, selectedY2Variables, selectedZVariables, true, variableLabels)
//             // Add the series data, this also filters for the segmentation selections, converts the date & datetime variables and checks for isolated data points
//             const results = addWaterfallSeriesData(newData, selectedXVariables, selectedY1Variables, selectedY2Variables, selectedZVariables, series, selectedSegmentationOptions)
//
//             series = results.series
//             categories = results.categories
//             minimumValueY1 = results.minimumValueY1
//             minimumValueY2 = results.minimumValueY2
//
//         } else {
//
//             // Initialise the definitions of the series
//             series = setSeriesForLineChart(chartType, selectedXVariables, selectedY1Variables, selectedY2Variables, useLineStyles, variableLabels)
//             // Add the series data, this also filters for the segmentation selections, converts the date & datetime variables and checks for isolated data points
//             series = addSeriesData(newData, selectedXVariables, selectedY1Variables, selectedY2Variables, series, selectedSegmentationOptions)
//         }
//
//         let config = {
//
//             // By default we do not allow titles, the table can be displayed with a separate title
//             title: {text: null},
//             // TODO add shade option
//             colors: useClientColours ? colours.client : colours.google20,
//             chart: {
//                 type: getHighChartType(chartType),
//                 zoomType: typeof chartZoom === "string" ? chartZoom : undefined,
//                 events: {
//                     // A hook that allows us to move the zoom buttons outside the HighChart component
//                     selection: function (e) {
//                         if (e["resetSelection"]) {
//                             setIsZoomed(false)
//                         } else {
//                             setIsZoomed(true)
//                         }
//                     },
//                     load: function () {
//
//                         console.log(this)
//
//                         const {plotHeight, chartWidth, marginBottom, xAxis, options: {chart: {type}}} = this
//
//                         const oppositeYAxisIndex = this.yAxis.findIndex(axis => axis.opposite === true)
//
//                         if (oppositeYAxisIndex > -1) {
//
//                             let newRightMargin = 0
//
//                             Object.entries(this.yAxis[oppositeYAxisIndex].ticks).forEach(([_, tick]) => {
//
//                                 if (tick.label.attr('x') + tick.label.getBBox().width - chartWidth > newRightMargin) {
//
//                                     newRightMargin = tick.label.attr('x') + tick.label.getBBox().width - this.chartWidth
//                                 }
//                             })
//
//                             if (newRightMargin > 0 && this.margin[1]) newRightMargin = newRightMargin + this.margin[1]
//                             if (newRightMargin > 0) this.update({chart:{marginRight: newRightMargin}})
//                         }
//
//                         const top = this.axes[0].chart.axes[0].top
//                         const bottom = this.axes[0].chart.axes[0].bottom
//
//                         if (plotHeight !== chartHeight) {
//
//                             this.setSize(chartWidth, chartHeight + (chartHeight - plotHeight) - marginBottom + (bottom - top))
//                         }
//
//                         console.log(type)
//
//                         if (["waterfall"].includes(type) && xAxis[0].labelRotation === -45) {
//
//                             //xAxis[0].update({labels: {rotation: -90}})
//
//                         }
//                     }
//                 },
//                 inverted: inverted,
//                 height: chartHeight ? `${chartHeight}${chartHeightIsPercent ? "%" : ""}` : undefined,
//                 marginTop: (["bar"].includes(chartType) && selectedY2Variables.length > 0) || ["histogram"].includes(chartType) ? undefined : margin.top,
//                 marginBottom: selectedXVariables.length > 0 || !margin.bottom ? undefined : Math.max(markerSize, margin.bottom),
//                 marginLeft: selectedY1Variables.length > 0 || !margin.left ? undefined : Math.max(markerSize, margin.left),
//                 // Ensure that markers are not clipped
//                 marginRight: (!["bar"].includes(chartType) && selectedY2Variables.length > 0) || !margin.right || ["histogram"].includes(chartType) ? undefined : Math.max(markerSize, margin.right),
//                 spacingTop: spacing.top,
//                 spacingBottom: spacing.bottom,
//                 spacingLeft: spacing.left,
//                 // Ensure that markers are not clipped,
//                 spacingRight: Math.max(markerSize, spacing.right)
//             },
//             // Remove the HighCharts credit text
//             credits: {
//                 enabled: false
//             },
//             // Define export formats
//             exporting: {
//                 buttons: {
//                     contextButton: {
//                         enabled: false,
//                         menuItems: ["downloadPNG"]
//                     }
//                 },
//                 filename: setDownloadName(header, "", true, userId),
//                 sourceWidth: 640,
//                 sourceHeight: 320
//             },
//             // Hide the burger as we use a custom one
//             navigation: {
//                 buttonOptions: {
//                     enabled: true
//                 }
//             },
//             tooltip: {
//                 enabled: showTooltip,
//                 formatter: setTooltipFormatter(),
//                 shared: tooltipIsShared,
//                 backgroundColor: "var(--tertiary-background)",
//                 borderWidth: 0,
//                 borderRadius: 0,
//                 shadow: false,
//                 useHTML: true,
//                 style: {opacity: "75%"}
//             },
//             legend: {
//                 enabled: legendIsEnabled && !hideAxes && !["bubble", "waterfall"].includes(chartType),
//                 labelFormatter: setLegendFormatter(chartType, selectedY1Variables, selectedY2Variables),
//                 color: getThemeColour("--primary-text"),
//                 backgroundColor: getThemeColour("--secondary-background"),
//                 itemStyle: {fontSize: fontSize, fontWeight: "normal"},
//                 // TODO when using dashes this needs to be modified
//                 symbolHeight: 12,
//                 symbolWidth: 12,
//                 symbolRadius: 6,
//                 squareSymbol: false,
//                 //TODO
//                 //symbolLineWidth: 0
//             },
//             plotOptions: {
//                 area: {
//                     fillOpacity: 0.5,
//                     marker: {enabled: showMarkers, radius: markerSize, lineWidth: 2, lineColor: "#fff"},
//                     lineWidth: 0.5
//                 },
//                 arearange: {
//                     fillOpacity: 0.5,
//                     marker: {enabled: false},
//                     lineWidth: 0.5
//                 },
//                 areaspline: {
//                     fillOpacity: 0.5,
//                     lineWidth: 0.5
//                 },
//                 areasplinerange: {},
//                 bar: {
//                     pointPadding: pointPadding,
//                     borderWidth: 0
//                 },
//                 bubble: {clip: isZoomed},
//                 column: {
//                     pointPadding: pointPadding,
//                     borderWidth: 0
//                 },
//                 columnrange: {},
//                 dependencywheel: {
//                     dataLabels: {
//                         enabled: true,
//                         formatter: setDataLabelFormatterFromKey()
//                     }
//                 },
//                 errorbar: {},
//                 histogram: {
//                     pointPadding: pointPadding,
//                     borderWidth: 0
//                 },
//                 line: {
//                     lineWidth: lineWidth,
//                     marker: {enabled: showMarkers, radius: markerSize, lineWidth: 2, lineColor: "#fff"}
//                 },
//                 pie: {
//                     allowPointSelect: false,
//                     startAngle: chartType === "pie" ? pieOptions.startAngle : chartType === "circleDoughnut" ? circleDoughnutOptions.startAngle : semiDoughnutOptions.startAngle,
//                     endAngle: chartType === "pie" ? pieOptions.endAngle : chartType === "circleDoughnut" ? circleDoughnutOptions.endAngle : semiDoughnutOptions.endAngle,
//                     center: chartType === "pie" ? pieOptions.center : chartType === "circleDoughnut" ? circleDoughnutOptions.center : semiDoughnutOptions.center,
//                     size: chartType === "pie" ? pieOptions.size : chartType === "circleDoughnut" ? circleDoughnutOptions.size : semiDoughnutOptions.size,
//                     innerSize: chartType === "pie" ? pieOptions.innerSize : chartType === "circleDoughnut" ? circleDoughnutOptions.innerSize : semiDoughnutOptions.innerSize,
//                     dataLabels: {
//                         enabled: true,
//                         softConnector: false,
//                         distance: 10,
//                         formatter: setDataLabelFormatterFromKey()
//                     },
//                     states: {
//                         hover: {
//                             enabled: true,
//                             halo: {
//                                 size: 0
//                             }
//                         }
//                     }
//                 },
//                 sankey: {
//                     dataLabels: {
//                         enabled: true,
//                         formatter: setDataLabelFormatterFromKey()
//                     }
//                 },
//                 scatter: {marker: {radius: markerSize}},
//                 waterfall: {
//                     upColor: getThemeColour("--success"),
//                     color: getThemeColour("--danger"),
//                     borderWidth: 0
//                 },
//                 series: {
//                     dataLabels: {
//                         enabled: showDataLabels, formatter: setDataLabelFormatter(), style: {
//                             fontSize: fontSize,
//                             fontWeight: "normal",
//                             color: "var(--secondary-text)"
//                         }
//                     },
//                     events: {
//                         "legendItemClick": handleLegendSeriesClick(chartType, chartComponent),
//                         // Put the filtering here
//                         click: function () {
//                             alert("Click!");
//                         }
//                     },
//                     crisp: false,
//                     cursor: "pointer"
//                 }
//             },
//             xAxis: [{
//                 title: {
//                     text: ["histogram"].includes(chartType) ? "Index of data point" : setAxisLabel(selectedXVariables, userSetAxisLabels)["x"],
//                     style: {
//                         color: "var(--tertiary-text)",
//                         fontSize: fontSize
//                     }
//                 },
//                 minTickInterval: ["column", "bar", "histogram", "columnWithError", "waterfall"].includes(chartType) ? undefined : setAxisMinInterval(selectedXVariables),
//                 type: getAxisType(selectedXVariables),
//                 allowDecimals: !schemaIsAllIntegers(selectedXVariables),
//                 labels: {
//                     formatter: setAxisNumberFormatter(chartType, selectedXVariables),
//                     style: {
//                         color: getThemeColour("--tertiary-text"),
//                         fontSize: fontSize,
//                         //textOverflow: "none"
//                     },
//                     y: ["bar", "histogram"].includes(chartType) ? undefined : ["column", "waterfall"].includes(chartType) ? 16 : 30,
//                 },
//                 startOnTick: ["bar"].includes(chartType) ? false : false,
//                 endOnTick: ["bar"].includes(chartType) ? false : false,
//                 visible: ["histogram"].includes(chartType) ? false : !hideAxes,
//                 opposite: ["histogram"].includes(chartType),
//                 alignTicks: false
//             }],
//             yAxis: [{
//                 title: {
//                     text: setAxisLabel(selectedY1Variables, userSetAxisLabels)["y1"],
//                     style: {
//                         color: "var(--tertiary-text)",
//                         fontSize: fontSize
//                     }
//                 },
//                 minTickInterval: setAxisMinInterval(selectedY1Variables),
//                 type: getAxisType(selectedY1Variables),
//                 allowDecimals: !schemaIsAllIntegers(selectedY1Variables),
//                 labels: {
//                     formatter: setAxisNumberFormatter(chartType, selectedY1Variables),
//                     style: {
//                         color: "var(--tertiary-text)",
//                         fontSize: fontSize,
//                         //textOverflow: "none"
//                     }
//                 },
//                 startOnTick: ["bar"].includes(chartType) ? false : true,
//                 endOnTick: ["bar"].includes(chartType) ? false : true,
//                 opposite: ["histogram"].includes(chartType),
//                 visible: ["histogram"].includes(chartType) ? false : !hideAxes,
//                 // If you make he minimum value for the axis the minimum value in the data then the bar will not show for one point
//                 // So we make it one less than that
//                 // Todo add function?
//                 min: ["column", "bar", "columnWithError", "waterfall"].includes(chartType) && getAxisType(selectedY1Variables) === "datetime" ? minimumValueY1 - setAxisMinInterval(selectedY1Variables) : undefined
//             }],
//             series: series
//         }
//
//         // Add the second y axis config if there are any variables set
//         if (selectedY2Variables.length > 0) {
//
//             config.yAxis.push({
//                 title: {
//                     text: setAxisLabel(selectedY2Variables, userSetAxisLabels)[["y2"]],
//                     style: {
//                         color: "var(--tertiary-text)",
//                         fontSize: fontSize
//                     }
//                 },
//                 minTickInterval: setAxisMinInterval(selectedY2Variables),
//                 type: getAxisType(selectedY2Variables),
//                 allowDecimals: !schemaIsAllIntegers(selectedY2Variables),
//                 // Override the default formats for datetime variables
//                 labels: {
//                     formatter: setAxisNumberFormatter(chartType, selectedY2Variables),
//                     style: {
//                         color: "var(--tertiary-text)",
//                         fontSize: fontSize,
//                         //textOverflow: "none"
//                     }
//                 },
//                 startOnTick: ["bar"].includes(chartType) ? false : true,
//                 endOnTick: ["bar"].includes(chartType) ? false : true,
//                 opposite: true,
//                 visible: !hideAxes,
//                 min: ["column", "bar", "columnWithError", "waterfall"].includes(chartType) && getAxisType(selectedY2Variables) === "datetime" ? minimumValueY1 - setAxisMinInterval(selectedY2Variables) : undefined
//             })
//         }
//
//         if (["column", "bar", "columnWithError", "waterfall"].includes(chartType)) {
//
//             config.xAxis[0].categories = categories
//             // TODO add the specialisms here
//         }
//
//         if (["histogram"].includes(chartType)) {
//
//             config.xAxis.push({
//                 title: {
//                     text: 'Bin', style: {
//                         color: "var(--tertiary-text)",
//                         fontSize: fontSize
//                     }
//                 },
//                 alignTicks: false,
//                 opposite: false,
//                 labels: {
//                     style: {
//                         color: getThemeColour("--tertiary-text"),
//                         fontSize: fontSize,
//
//                     }
//                 }
//             })
//
//             config.yAxis.push({
//                 title: {
//                     text: 'Frequency', style: {
//                         color: "var(--tertiary-text)",
//                         fontSize: fontSize
//                     }
//                 },
//                 opposite: false,
//                 labels: {
//                     style: {
//                         color: getThemeColour("--tertiary-text"),
//                         fontSize: fontSize,
//
//                     }
//                 }
//             })
//
//         }
//
//         return config
//
//     }, [chartHeight, chartHeightIsPercent, chartType, chartZoom, circleDoughnutOptions.center, circleDoughnutOptions.endAngle, circleDoughnutOptions.innerSize, circleDoughnutOptions.size, circleDoughnutOptions.startAngle, colours.client, colours.google20, data, fontSize, header, hideAxes, inverted, legendIsEnabled, lineWidth, margin.bottom, margin.left, margin.right, margin.top, markerSize, pieOptions.center, pieOptions.endAngle, pieOptions.innerSize, pieOptions.size, pieOptions.startAngle, pointPadding, schema, selectedSegmentationOptions, selectedXVariables, selectedY1Variables, selectedY2Variables, semiDoughnutOptions.center, semiDoughnutOptions.endAngle, semiDoughnutOptions.innerSize, semiDoughnutOptions.size, semiDoughnutOptions.startAngle, showDataLabels, showMarkers, showTooltip, spacing.bottom, spacing.left, spacing.right, spacing.top, tooltipIsShared, useClientColours, useLineStyles, userId, userSetAxisLabels, userSetVariableLabels])
//
//     console.log(config)
//
//     return (
//         <React.Fragment>
//
//             {showMenu &&
//             <React.Fragment>
//                 <Row className={"mb-2"}>
//                     <Col xs={12}>
//                         <SelectOption options={yVariableOptions}
//                                       onChange={handleY1VariableChange}
//                                       value={selectedY1Variables}
//                                       isMulti={Boolean(maxSeriesPerAxis.y1 > 1)}
//                                       isDispatched={false}
//                                       maximumSelectionsBeforeMessageOverride={5}
//                                       labelText={"Select left hand y-axis"}
//                         />
//                     </Col>
//                     <Col xs={12}>
//                         <SelectOption options={yVariableOptions}
//                                       onChange={handleY2VariableChange}
//                                       value={selectedY2Variables}
//                                       isMulti={Boolean(maxSeriesPerAxis.y2 > 1)}
//                                       isDispatched={false}
//                                       maximumSelectionsBeforeMessageOverride={5}
//                                       labelText={"Select secondary y-axis"}
//                         />
//                     </Col>
//                     <Col xs={12}>
//                         <SelectOption options={xVariableOptions}
//                                       onChange={handleXVariableChange}
//                                       value={selectedXVariables}
//                                       isMulti={Boolean(maxSeriesPerAxis.x > 1)}
//                                       isDispatched={false}
//                                       maximumSelectionsBeforeMessageOverride={5}
//                                       labelText={"Select x-axis"}
//                         />
//                     </Col>
//
//                     <Col xs={12}>
//                         <SelectOption options={zVariableOptions}
//                                       onChange={handleZVariableChange}
//                                       value={selectedXVariables}
//                                       isMulti={Boolean(maxSeriesPerAxis.z > 1)}
//                                       isDispatched={false}
//                                       maximumSelectionsBeforeMessageOverride={5}
//                                       labelText={"Select z-axis"}
//                         />
//                     </Col>
//
//                 </Row>
//                 <Row>
//                     <Col xs={12}>
//                         Chart segment options:
//                     </Col>
//                     {Object.entries(segmentationOptions).map(([key, options]) => {
//
//                         return (
//
//                             <Col lg={6} key={key}>
//                                 <SelectOption options={options}
//                                               onChange={handleSegmentVariableChange}
//                                               value={selectedSegmentationOptions}
//                                               isMulti={false}
//                                               isDispatched={false}
//                                 />
//                             </Col>
//                         )
//                     })}
//                     <Col>
//
//                     </Col>
//                 </Row></React.Fragment>
//             }
//
//             {title &&
//             <HeaderTitle text={title} type={"h6"} className={"text-center mb-2 mih-55"}/>
//             }
//
//             <DropdownMenu canExport={canExport}
//                           chartZoom={chartZoom}
//                           isZoomed={isZoomed}
//                           chartComponent={chartComponent}
//             />
//
//             {/*If the chart needs to redrawn because of some external change e.g. the width of its parent*/}
//             {/*then add the external parameter as a key to the HighChart. Doing this forces a full redraw.*/}
//
//             {
//                 // TODO needs work
//                 selectedXVariables.length === 0 && (selectedY1Variables.length === 0 && selectedY2Variables.length === 0) &&
//                 <Row style={{color: "var(--tertiary-text)", height: 0.8 * chartHeight}}>
//                     <Col className={"my-auto align-items-center w-100 text-center"}>
//                         No variables selected
//                     </Col></Row>
//
//             }
//
//             {
//                 !(selectedXVariables.length === 0 && selectedY1Variables.length === 0 && selectedY2Variables.length === 0) &&
//                 <HighchartsReact ref={chartComponent}
//                                  highcharts={Highcharts}
//                                  constructorType={"chart"}
//                                  options={config}
//                 />
//             }
//
//         </React.Fragment>
//     )
// };
//
// Chart.propTypes = {
//
//     chartType: PropTypes.oneOf([
//         "area",//
//         "arearange",//
//         "areaspline",//
//         "bar",//
//         "bubble",//
//         "bullet",
//         "column",//
//         "columnrange",
//         "dependencywheel",//
//         "columnWithError",//
//         "histogram",//
//         "line",//
//         "lineWithError",//
//         "pie",//
//         "circleDoughnut",//
//         "semiDoughnut",//
//         "pie",//
//         "sankey",//
//         "scatter",//
//         "solidgauge",//
//         "spline",//
//         "waterfall"
//     ]).isRequired,
//     schema: PropTypes.arrayOf(
//         PropTypes.shape({
//             fieldOrder: PropTypes.number,
//             fieldName: PropTypes.string.isRequired,
//             fieldType: PropTypes.string.isRequired,
//             formatCode: PropTypes.string,
//             categorical: PropTypes.bool,
//             businessKey: PropTypes.object
//         })
//     ).isRequired,
//     data: PropTypes.array.isRequired,
//     showXVariableSelector: PropTypes.bool,
//     showY1VariableSelector: PropTypes.bool,
//     showY2VariableSelector: PropTypes.bool,
//     /**
//      * An array of the fieldNames of variables to use as the initial segmentation variables. Or a string that
//      * either is "auto" which lets this component set the variables, or is a pipe delimited string of the variable
//      * names to use as the segmentation variables.
//      */
//     defaultSegmentationVariables: PropTypes.oneOfType([PropTypes.string, PropTypes.arrayOf(PropTypes.string)]),
//     hiddenColumns: PropTypes.oneOfType([PropTypes.string, PropTypes.arrayOf(PropTypes.string)]),
//     defaultXVariables: PropTypes.oneOfType([PropTypes.string, PropTypes.arrayOf(PropTypes.string)]),
//     defaultY1Variables: PropTypes.oneOfType([PropTypes.string, PropTypes.arrayOf(PropTypes.string)]),
//     defaultY2Variables: PropTypes.oneOfType([PropTypes.string, PropTypes.arrayOf(PropTypes.string)]),
//     defaultZVariables: PropTypes.oneOfType([PropTypes.string, PropTypes.arrayOf(PropTypes.string)]),
//     defaultE1Variables: PropTypes.oneOfType([PropTypes.string, PropTypes.arrayOf(PropTypes.string)]),
//     defaultE2Variables: PropTypes.oneOfType([PropTypes.string, PropTypes.arrayOf(PropTypes.string)]),
//     /**
//      * Whether to differentiate line charts by the line style
//      */
//     useLineStyles: PropTypes.bool,
//     /**
//      * Whether to show lines with markers,
//      */
//     showMarkers: PropTypes.bool,
//     /**
//      * The marker size for line charts.
//      */
//     markerSize: PropTypes.number,
//     /**
//      * If the user has set a label for a particular variable then this is passed via userSetLegendLabels. If a label
//      * if not set by the user then the default label of the variable in the schema is used.
//      */
//     userSetVariableLabels: PropTypes.objectOf(PropTypes.string),
//     /**
//      * If the user has set a label for a particular axis then this is passed via userSetAxisLabels. If a label
//      * if not set by the user then the default label of the axis is used.
//      */
//     userSetAxisLabels: PropTypes.objectOf(PropTypes.string),
//     /**
//      * Whether to show data labels for the points on the chart.
//      */
//     showDataLabels: PropTypes.bool,
//     /**
//      * Whether to use client colours instead of the google20.
//      */
//     useClientColours: PropTypes.bool,
//     /**
//      * The zoom axes.
//      */
//     chartZoom: PropTypes.oneOf(["x", "xy", "y"]),
//     /**
//      * Whether the chart should be inverted so the x-axis is on the left and the y-axis is on the bottom.
//      */
//     inverted: PropTypes.bool,
//     /**
//      * The height if the chart in pixels, the width will always fll the parent.
//      */
//     chartHeight: PropTypes.number,
//     /**
//      * Whether the height number is actually a % setting, the height should be x% of the width.
//      */
//     chartHeightIsPercent: PropTypes.bool,
//     /**
//      * A margins around the chart.
//      */
//     margin: PropTypes.shape({
//         top: PropTypes.number,
//         bottom: PropTypes.number,
//         left: PropTypes.number,
//         right: PropTypes.number
//     }),
//     /**
//      * Whether the chart can be exported.
//      */
//     canExport: PropTypes.bool,
//     /**
//      * Whether the tooltip shows the values of all series when hovered.
//      */
//     tooltipIsShared: PropTypes.bool,
//     /**
//      * Whether to show the legend.
//      */
//     legendIsEnabled: PropTypes.bool,
//     /**
//      * Whether to add an "All segments" option to the segmentation menus and have this as the default.
//      */
//     allowAllSegmentsToBePlotted: PropTypes.bool,
//     /**
//      * Whether to hide the axes, for example if you want to make the chart a small spline.
//      */
//     hideAxes: PropTypes.bool,
//     /**
//      * The line width of the series.
//      */
//     lineWidth: PropTypes.number,
//     /**
//      * The text size for all text in the charts.
//      */
//     fontSize: PropTypes.string,
//     /**
//      * Whether to show the tooltip when the points are hovered over.
//      */
//     showTooltip: PropTypes.bool,
//     /**
//      * The header property of the TRAC metadata object for the dataset that is being charted. This is used to name the exported file.
//      */
//     header: PropTypes.object.isRequired,
//     /**
//      * The spacing between bars in a column chart, as a % of the axis units.
//      */
//     pointPadding: PropTypes.number,
//     /**
//      * The size for the inner part of a pie chart used to make a doughnut
//      */
//     pieInnerSize: PropTypes.number,
//
//     circleDoughnutOptions: PropTypes.object,
//     pieOptions: PropTypes.object,
//     semiDoughnutOptions: PropTypes.object,
//     /**
//      * The title to show above the bullet.
//      */
//     title: PropTypes.string,
// }
//
// Chart.defaultProps = {
//
//     chartType: "line",
//     showXVariableSelector: true,
//     showY1VariableSelector: true,
//     showY2VariableSelector: true,
//     hiddenColumns: [],
//     defaultXVariables: "auto",
//     defaultY1Variables: "auto",
//     defaultY2Variables: [],
//     defaultZVariables: [],
//     defaultE1Variables: [],
//     defaultE2Variables: [],
//     defaultSegmentationVariables: [],
//     useuserSetAxisLabelsLineStyles: false,
//     showMarkers: false,
//     markerSize: 6,
//     userSetVariableLabels: {},
//     userSetAxisLabels: {x: null, y1: null, y2: null, z: null},
//     showDataLabels: false,
//     useClientColours: false,
//     chartZoom: "xy",
//     inverted: false,
//     chartHeight: 300,
//     chartHeightIsPercent: false,
//     margin: {top: undefined, bottom: undefined, left: undefined, right: undefined},
//     spacing: {top: 10, bottom: 15, left: 10, right: 10},
//     canExport: true,
//     tooltipIsShared: false,
//     legendIsEnabled: true,
//     allowAllSegmentsToBePlotted: true,
//     hideAxes: false,
//     lineWidth: 4,
//     fontSize: "0.875rem",
//     showTooltip: true,
//     header: {},
//     pointPadding: 0.1,
//     circleDoughnutOptions: {innerSize: "50%", startAngle: 0, endAngle: undefined, size: null, center: [null, null]},
//     pieOptions: {innerSize: 0, startAngle: 0, endAngle: undefined, size: null, center: [null, null]},
//     semiDoughnutOptions: {innerSize: "50%", startAngle: -90, endAngle: 90, size: null, center: ['50%', `62.5%`]},
// }
//
// export default Chart;