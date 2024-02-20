import React, {useMemo} from "react";
import PropTypes from "prop-types";
//import Highcharts from "highcharts/highcharts";
i//mport HighchartsReact from "highcharts-react-official";
import {applyNumberFormat, convertNumberFormatToArray, getThemeColour, isValidNumberFormat} from "../../utils/utils";
import {setHighchartsStyles} from "../../utils/utils_chart";
import {HeaderTitle} from "../HeaderTitle";

// Enable solid gauge chart type
//import solidGauge from "highcharts/modules/solid-gauge";
// Enable arearange and other charts
//import highChartsMore from "highcharts/highcharts-more";

// Needed for arearange and other charts
//highChartsMore(Highcharts)

// Needed for solid gauge charts
//solidGauge(Highcharts)

/**
 * A function passed as part of the chart config that returns the HTML to use for the data label for the
 * gauge. This is the number displayed at the center of the gauge.
 * @param schema - The schema for the value of the gauge. This includes the fontCode property
 * that is used to set how the label is displayed and with what units.
 * @param fontSize - The font size for the units part of the data label.
 * @param value - The number of the gauge.
 * @param fontSizeOverride - After the gauge is rendered the data label size and position are
 * modified using the load event to take account of the size of the gauge container. fontSizeOverride
 * is what the main number in the data label font size will be.
 * @returns {function(): string}
 */
const setHtmlDataLabelFormatter = (schema, fontSize, value, fontSizeOverride = 25) => {

    return function () {

        const {y} = this

        let label = '<div style="text-align:center">'

        let number = y, formatCodeAsArray = null

        if (schema && isValidNumberFormat(schema.formatCode)) {

            number = applyNumberFormat(y, schema.formatCode, false)
            formatCodeAsArray = convertNumberFormatToArray(schema.formatCode)
        }

        if (value) {
            label += `<span style="font-size:${fontSizeOverride}px">${number}</span>`
        }

        if (value && formatCodeAsArray) {
            label += `<br/><span style="font-size:${fontSize};color:var(--tertiary-text)">${formatCodeAsArray[3].toString()}${formatCodeAsArray[4].toString()}</span>`
        } else if (!value) {
            label += `<span style="font-size:${fontSize};color:var(--tertiary-text)">No value set</span>`
        }

        label += '</div>'

        return label
    }
}

/**
 * A function that sets the stops or the boundaries for the colours in the gauge. There are two things
 * that this has to take account of. The first is that the HighCharts API uses fractions of the gauge
 * as the value. So if a gauge goes from 0 to 100 and we won't up to 10 to be green we have to use a
 * value of 0.1%. However we don't know what the gauge goes to until HighCharts decides the max label
 * to add to the gauge - so we can't run this until after the HighCharts component sets the value.
 *
 * The second thing is that HighCharts by default will gradient fill the gauge. To stop that and have
 * a pure red/amber/green colour based on value we add additional stops enforce no gradient fill. See
 * https://www.highcharts.com/forum/viewtopic.php?t=40568
 */
const setStops = (userStops, max = null) => {

    // If RGB stops are not defined then a single info colour dial is shown
    if (!userStops) return [[0, getThemeColour("--info")]]

    let stops = []

    const green = [userStops.green / max, getThemeColour("--success")] // green

    let amberGreenIntermediate, redAmberIntermediate

    // To avoid HighCharts default colour gradient we fix the colour for the entire green range
    if (userStops.amber > userStops.green) {
        amberGreenIntermediate = [(userStops.amber - 0.01) / max, getThemeColour("--success")] // green
    } else {
        amberGreenIntermediate = [(userStops.green - 0.01) / max, getThemeColour("--warning")] // green
    }

    const amber = [userStops.amber / max, getThemeColour("--warning")] // amber
    // To avoid HighCharts default colour gradient we fix the colour for the entire amber range
    if (userStops.red > userStops.amber) {
        redAmberIntermediate = [(userStops.red - 0.01) / max, getThemeColour("--warning")] // red
    } else {
        redAmberIntermediate = [(userStops.amber - 0.01) / max, getThemeColour("--danger")] // red
    }

    const red = [userStops.red / max, getThemeColour("--danger")] // red

    stops.push(green)
    stops.push(amberGreenIntermediate)
    stops.push(amber)
    stops.push(redAmberIntermediate)
    stops.push(red)

    // Sort by the stop % to maks sure it is rendered OK
    stops = stops.sort(function (a, b) {
        return -b[0] + a[0];
    });

    return stops
}

/**
 * A function that sets the maximum for the gauge, the user can define this or let HighCharts set it.
 * @returns -
 */
const setMax = (userMax, userStops, value) => {

    if (userMax) return userMax

    if (!userStops && value) return value

    if (!userStops && !value) return 0

    // The 1.33 is to give the gauge a little room for the red or green bars (if reversed). Otherwise
    // HighCharts could make the last bar colour really short.
    return Math.round(Math.max(1.33 * Math.max(userStops.red, userStops.amber, userStops.green), value || 0))
}

/**
 * A component that shows a gauge with a red amber or green colour depending on the value. Note that scaling of
 * gauge charts in Highcharts is unusual. If you need to change the default props then to make the chart look
 * normal size, chartHeight and outerRadius all need to be updated until the desired look is achieved.
 * @returns {JSX.Element}
 */
const Gauge = props => {

    const {
        schema,
        showDataLabels,
        chartHeight,
        chartHeightIsPercent,
        margin,
        spacing,
        fontSize,
        innerRadius,
        outerRadius,
        value,
        title,
        size,
        stops: userStops,
        max: userMax
    } = props

    // Add in font styles and numeric separators
    setHighchartsStyles(Highcharts)

    const config = useMemo(() => {

        return {

            // By default we do not allow titles, the table can be displayed with a separate title
            title: {text: null},
            chart: {
                type: "solidgauge",
                events: {
                    load: function () {

                        // Modify the data label position and size based on the size of the chart. This is
                        // done by scaling the values based on those that worked nicely for a 350px wide
                        // chart at size 140%. It won't be exact but it will be near enough.
                        const {chartWidth} = this

                        let hasUnits, formatCodeAsArray = null

                        // Is a valid schema format for a number available
                        if (schema && isValidNumberFormat(schema.formatCode)) {

                            formatCodeAsArray = convertNumberFormatToArray(schema.formatCode)
                        }

                        // Does the format include any units that mean we have a unit label in the data label.
                        hasUnits = Boolean(value && formatCodeAsArray && (formatCodeAsArray[3].toString().length > 0 || formatCodeAsArray[4].toString().length > 0))

                        // Center the min and max labels on the arcs, only do the redrawn if there are no data labels, otherwise that
                        // update will do the redraw
                        this.yAxis[0].update({
                            labels: {
                                distance: -this.yAxis[0].center[2] / 10
                            },
                            // We can only set ths stops when we know the maximum in the gauge as
                            // we have to convert the absolute stops provided by the user into
                            // a percentage of the dial.
                            stops: setStops(userStops ,this.yAxis[0].max)

                        }, undefined, !this.series[0].data[0]["dataLabel"])

                        const newFontSize = Math.min(25, Math.round(25 * size * chartWidth / (350 * 140)))
                        const changeInFontSize = value ? 25 - newFontSize : 0

                        // Move and resize the data label
                        // Move label up by its height so the bottom is lever with the gauge, -4 is for the extra
                        // padding when there is a units label. Also take account of the font size change for
                        // labels when the chart is smaller than 350px * 140%
                        if (this.series[0].data[0]["dataLabel"]) {

                            this.series[0].update({
                                dataLabels: {
                                    y: -this.series[0].data[0]["dataLabel"]["absoluteBox"].height + changeInFontSize + (hasUnits || !value ? -3 : 0),
                                    formatter: setHtmlDataLabelFormatter(schema, fontSize, value, newFontSize)
                                }
                            })
                        }
                    }
                },
                height: `${chartHeight}${chartHeightIsPercent ? "%" : ""}`,
                marginTop: margin.top,
                marginBottom: margin.bottom,
                marginLeft: margin.left,
                marginRight: margin.right,
                spacingTop: spacing.top,
                spacingBottom: spacing.bottom,
                spacingLeft: spacing.left,
                spacingRight: spacing.right
            },
            pane: {
                center: ['50%', '70%'],
                size: size + '%',
                startAngle: -90,
                endAngle: 90,
                background: {
                    backgroundColor: "var(--secondary-background)",
                    borderColor: "var(--tertiary-background)",
                    innerRadius: innerRadius,
                    outerRadius: outerRadius,
                    shape: 'arc'
                }
            },
            // Remove the HighCharts credit text
            credits: {
                enabled: false
            },
            // Hide the burger as we use a custom one
            navigation: {
                buttonOptions: {
                    enabled: false
                }
            },
            tooltip: {
                enabled: false
            },
            legend: {
                enabled: false
            },
            plotOptions: {
                series: {
                    dataLabels: {
                        enabled: showDataLabels,
                        style: {
                            fontWeight: "300"
                        },
                        formatter: setHtmlDataLabelFormatter(schema, fontSize, value),
                        borderWidth: 0,
                        useHTML: true
                    },
                    events: {
                        // Put the filtering here
                        click: value ? function () {
                            alert("Click!");
                        } : undefined
                    },
                    crisp: false,
                    cursor: value ? "pointer" : undefined
                }
            },
            yAxis: {
                labels: {
                    style: {
                        color: "var(--tertiary-text)",
                        fontSize: fontSize,
                    },
                    y: 16
                },
                min: 0,
                max: setMax(userMax, userStops, value),
                stops: undefined,
                lineWidth: 0,
                tickWidth: 0,
                minorTickInterval: null,
                tickAmount: userMax ? undefined : 2,
                tickInterval: userMax ? userMax : undefined,
                endOnTick: !userMax
            },
            series: [{
                data: [{
                    y: value || 0,
                    radius: outerRadius,
                    innerRadius: innerRadius
                }]
            }]
        }

    }, [chartHeight, chartHeightIsPercent, fontSize, innerRadius, margin.bottom, margin.left, margin.right, margin.top, outerRadius, schema, showDataLabels, size, spacing.bottom, spacing.left, spacing.right, spacing.top, userMax, userStops, value])

    return (
        <React.Fragment>

            {/*If the chart needs to redrawn because of some external change e.g. the width of its parent*/}
            {/*then add the external parameter as a key to the HighChart. Doing this forces a full redraw.*/}

            {title &&
            <HeaderTitle text={title} type={"h6"} outerClassName={"text-center"}/>
            }

            <HighchartsReact highcharts={Highcharts}
                             constructorType={"chart"}
                             options={config}
            />

        </React.Fragment>
    )
};

Gauge.propTypes = {

    /**
     * The schema for the data, this can be a TRAC schema item for the variable but currently only the
     * formatCode is needed to format the data label.
     */
    schema: PropTypes.shape({
            fieldOrder: PropTypes.number,
            fieldName: PropTypes.string,
            fieldType: PropTypes.string,
            formatCode: PropTypes.string.isRequired,
            categorical: PropTypes.bool,
            businessKey: PropTypes.object
        }
    ),
    /**
     * Whether to show data labels for the points on the chart.
     */
    showDataLabels: PropTypes.bool,
    /**
     * The height if the chart in pixels, the width will always fll the parent. If chartHeightIsPercent
     * is true this number is interpreted as a %.
     */
    chartHeight: PropTypes.number.isRequired,
    /**
     * Whether the height number is actually a % setting, the height should be x% of the width.
     */
    chartHeightIsPercent: PropTypes.bool,
    /**
     * The margins around the chart.
     */
    margin: PropTypes.shape({
        top: PropTypes.number,
        bottom: PropTypes.number,
        left: PropTypes.number,
        right: PropTypes.number
    }),
    /**
     * The spacing around the chart.
     */
    spacing: PropTypes.shape({
        top: PropTypes.number,
        bottom: PropTypes.number,
        left: PropTypes.number,
        right: PropTypes.number
    }),
    /**
     * The text size for all text in the chart.
     */
    fontSize: PropTypes.string,
    /**
     * The inner radius size as a % of the width of the chart. Reducing the difference between
     * the inner outer radii makes for a narrower gauge.
     */
    innerRadius: PropTypes.string,
    /**
     * The outer radius size as a % of the width of the chart. Reducing the difference between
     * the inner outer radii makes for a narrower gauge.
     */
    outerRadius: PropTypes.string,
    /**
     * The scaling of the gauge as a %. This is a very odd prop, the semi circular gauge is actually
     * drawn as a circle and the bottom half is hidden, however this makes the gauge quite small as
     * the height defines the circle that can be draw. This prop then scales this semi-circle up so it
     * better fits the chart container. The pane.center config option positions this scaled image.
     */
    size: PropTypes.number,
    /**
     * The positions to use for the red amber and green colouring. These are absolute values. So if
     * amber is 10 this means if the value is 10 or more (up to the red stop number) then the gauge
     * will be coloured amber. These can be ordered green - amber - red or red - amber - green.
     */
    stops: PropTypes.shape({
        green: PropTypes.number,
        amber: PropTypes.number,
        red: PropTypes.number
    }),
    /**
     * The maximum for the gauge. If this is not set then HighCharts will set the maximum and round up
     * the end point based on the data. This is needed for example if you want to have a dial from 0 - 100%.
     * and don't want HighCharts to round up to 120%.
     */
    max: PropTypes.number,
    /**
     * The title to show above the gauge.
     */
    title: PropTypes.string,
    /**
     * The value of the gauge.
     */
    value: PropTypes.number
}

Gauge.defaultProps = {

    showDataLabels: true,
    chartHeight: 50,
    chartHeightIsPercent: true,
    margin: {top: undefined, bottom: undefined, left: undefined, right: undefined},
    spacing: {top: 20, bottom: 10, left: 10, right: 10},
    fontSize: "0.875rem",
    innerRadius: '60%',
    outerRadius: '100%',
    schema: {formatCode: ",|.|1||"},
    size: 140,
    max: undefined
}

export default Gauge;