import React, {useMemo} from "react";
import PropTypes from "prop-types";
//import Highcharts from "highcharts/highcharts";
//import HighchartsReact from "highcharts-react-official";
import {applyNumberFormat, getThemeColour, isValidNumberFormat, hexToRgb} from "../../utils/utils";
import {setHighchartsStyles, setAxisNumberFormatter} from "../../utils/utils_chart";
import {HeaderTitle} from "../HeaderTitle";

// Enable bullet chart type
//import bullet from "highcharts/modules/bullet";

// Needed for solid bullet charts
//bullet(Highcharts)

/**
 * A function passed as part of the chart config that sets the plot bands or the boundaries for the colours
 * in the bullet. There are two things that this has to take account of.
 * @returns {{color, from: number, to: number}[]|[]}
 */
const setPlotBands = (userStops, target, value, opacity) => {

    // If RGB stops are not defined then a single info colour dial is shown
    if (!userStops) return [{
        from: 0,
        to: Math.max(target || 0, value || 0) * 5,
        color: getThemeColour("--info")
    }]

    const successAsRGB = hexToRgb(getThemeColour("--success"))
    const success = `rgba(${successAsRGB.r}, ${successAsRGB.g}, ${successAsRGB.b}, ${opacity})`

    const warningAsRGB = hexToRgb(getThemeColour("--warning"))
    const warning = `rgba(${warningAsRGB.r}, ${warningAsRGB.g}, ${warningAsRGB.b}, ${opacity})`

    const dangerAsRGB = hexToRgb(getThemeColour("--danger"))
    const danger = `rgba(${dangerAsRGB.r}, ${dangerAsRGB.g}, ${dangerAsRGB.b}, ${opacity})`

    let stops = []

    let green, amber, red

    if (userStops.amber > userStops.green) {
        green = {from: userStops.green, to: userStops.amber, color: success} // green
    } else {
        green = {from: userStops.green, to: Math.max(target || 0, value || 0) * 5, color: success} // green
    }

    if (userStops.red > userStops.amber) {
        amber = {from: userStops.amber, to: userStops.red, color: warning} // amber
    } else {
        amber = {from: userStops.red, to: userStops.amber, color: warning} // amber
    }

    if (userStops.red > userStops.amber) {
        red = {from: userStops.red, to: Math.max(target || 0, value || 0) * 5, color: danger} // red
    } else {
        red = {from: userStops.red, to: userStops.amber, color: danger} // green
    }

    stops.push(green)
    stops.push(amber)
    stops.push(red)

    return stops
}

/**
 * A function passed as part of the chart config that returns the HTML to use for the category label for the
 * bullet. This is the number displayed at the left of the bullet.
 * @param valueSchema - The schema for the value of the bullet. This includes the fontCode property
 * that is used to set how the label is displayed and with what units.
 * @param targetSchema - The schema for the target of the bullet. This includes the fontCode property
 * that is used to set how the label is displayed and with what units. If not available then valueSchema is
 * used.
 * @param value - The number of the bullet.
 * @param target - The target of the bullet.
 * @param fontSize - The font size for the units part of the category label.
 * @returns -
 */
const setHtmlCategory = (valueSchema, targetSchema, value, target, fontSize) => {

    let label = '<span>'

    let numberValue, numberTarget

    if (!targetSchema) targetSchema = {...valueSchema}

    if (valueSchema && isValidNumberFormat(valueSchema.formatCode)) {
        numberValue = value && applyNumberFormat(value, valueSchema.formatCode)
    }

    if (targetSchema && isValidNumberFormat(targetSchema.formatCode)) {
        numberTarget = target && applyNumberFormat(target, targetSchema.formatCode)
    }

    if (value) {
        label += `<span>${numberValue}</span>`
    }

    if (target) {
        label += `<br/><span style="font-size:${fontSize};font-weight:400;color:var(--tertiary-text)">Target: ${numberTarget}</span>`
    }

    label += '</span>'

    return label
}

/**
 * A component that shows a bullet with a red amber or green colour depending on the value. Note that scaling of
 * bullet charts in Highcharts is unusual. If you need to change the default props then to make the chart look
 * normal size, chartHeight and outerRadius all need to be updated until the desired look is achieved.
 * @returns {JSX.Element}
 */
const Bullet = props => {

    const {
        valueSchema,
        targetSchema,
        chartHeight,
        chartHeightIsPercent,
        margin,
        spacing,
        fontSize,
        value,
        target,
        title,
        stops: userStops,
        max: userMax,
        opacity
    } = props

    // Add in font styles and numeric separators
    //setHighchartsStyles(Highcharts)

    const config = useMemo(() => {

        return {

            // By default we do not allow titles, the table can be displayed with a separate title
            title: {text: null},
            chart: {
                type: "bullet",
                inverted: true,
                height: `${chartHeight}${chartHeightIsPercent ? "%" : ""}`,
                marginTop: margin.top,
                marginBottom: margin.bottom,
                marginLeft: margin.left,
                marginRight: margin.right,
                spacingTop: spacing.top,
                spacingBottom: spacing.bottom,
                spacingLeft: spacing.left,
                spacingRight: spacing.right,

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
                    pointPadding: 0.25,
                    borderWidth: 0,
                    borderColor: '#000',
                    color: '#000',
                    targetOptions: {
                        width: '200%'
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
            xAxis: {
                categories: [setHtmlCategory(valueSchema, targetSchema, value, target, fontSize)],
                labels: {
                    style: {
                        fontWeight: "300",
                        fontSize: "1.25rem"
                    }
                }
            },
            yAxis: {
                gridLineWidth: 0,
                plotBands: setPlotBands(userStops, target, value, opacity),
                title: null,
                max: userMax,
                endOnTick: !userMax,
                labels: {
                    style: {
                        color: "var(--tertiary-text)",
                        fontSize: fontSize,
                    },
                    formatter: setAxisNumberFormatter("bullet", [valueSchema]),
                }
            },
            series: [{
                data: [{
                    y: value || 0,
                    target: target
                }]
            }]
        }

    }, [chartHeight, chartHeightIsPercent, fontSize, margin.bottom, margin.left, margin.right, margin.top, opacity, valueSchema, targetSchema, spacing.bottom, spacing.left, spacing.right, spacing.top, target, userMax, userStops, value])

    return (
        <React.Fragment>

            {/*If the chart needs to redrawn because of some external change e.g. the width of its parent*/}
            {/*then add the external parameter as a key to the HighChart. Doing this forces a full redraw.*/}

            {title &&
            <HeaderTitle text={title} type={"h6"} outerClassName={"text-center"}/>
            }

            {value &&
            // <HighchartsReact highcharts={Highcharts}
            //                  constructorType={"chart"}
            //                  options={config}
            // />
            }
            {!value &&
            // TODO move to css ?
            <span className={"mt-4 d-block text-center"} style={{fontWeight: 400, color: "var(--tertiary-text)"}}>
                No value set
            </span>
            }

        </React.Fragment>
    )
};

Bullet.propTypes = {

    /**
     * The schema for the data, this can be a TRAC schema item for the variable but currently only the
     * formatCode and fieldType are needed to format the axis labels and category label.
     */
    valueSchema: PropTypes.shape({
            fieldOrder: PropTypes.number,
            fieldName: PropTypes.string,
            fieldType: PropTypes.string.isRequired,
            formatCode: PropTypes.string.isRequired,
            categorical: PropTypes.bool,
            businessKey: PropTypes.object
        }
    ),
    /**
     * The schema for the target value, this can be a TRAC schema item for the variable but currently only the
     * formatCode are needed to format the category label.
     */
    targetSchema: PropTypes.shape({
            fieldOrder: PropTypes.number,
            fieldName: PropTypes.string,
            fieldType: PropTypes.string.isRequired,
            formatCode: PropTypes.string.isRequired,
            categorical: PropTypes.bool,
            businessKey: PropTypes.object
        }
    ),
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
     * The positions to use for the red amber and green colouring. These are absolute values. So if
     * amber is 10 this means if the value is 10 or more (up to the red stop number) then the bullet
     * will be coloured amber. These can be ordered green - amber - red or red - amber - green.
     */
    stops: PropTypes.shape({
        green: PropTypes.number,
        amber: PropTypes.number,
        red: PropTypes.number
    }),
    /**
     * The maximum for the bullet. If this is not set then HighCharts will set the maximum and round up
     * the end point based on the data. This is needed for example if you want to have a dial from 0 - 100%.
     * and don't want HighCharts to round up to 120%.
     */
    max: PropTypes.number,
    /**
     * The title to show above the bullet.
     */
    title: PropTypes.string,
    /**
     * The value of the bullet.
     */
    value: PropTypes.number,
    /**
     * The target of the bullet (shown as a vertical line).
     */
    target: PropTypes.number,
    /**
     * The rgb opacity to apply to the red, amber and green colours if set
     * as solid colours look too strong without watering down.
     */
    opacity: PropTypes.number
}

Bullet.defaultProps = {

    showDataLabels: true,
    chartHeight: 30,
    chartHeightIsPercent: true,
    margin: {top: undefined, bottom: undefined, left: undefined, right: undefined},
    spacing: {top: 20, bottom: 10, left: 10, right: 10},
    fontSize: "0.875rem",
    valueSchema: {formatCode: ",|.|1||"},
    targetSchema: {formatCode: ",|.|0||"},
    opacity: 0.4
}

export default Bullet;