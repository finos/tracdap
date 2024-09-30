/**
 * Config for charts.
 * @module config_charts
 * @category Config
 */

import {ChartDefinition, ChartType} from "../types/types_general";
import {getThemeColour} from "../react/utils/utils_general";
import {tracdap as trac} from "@finos/tracdap-web-api";

export const GeneralChartConfig = {

    thousandsSeparator: ",",
    colours: {
        google20: [
            getThemeColour("lightTheme", "--info") || "#3366cc", "#dc3912", "#ff9900", "#109618", "#990099",
            "#0099c6", "#dd4477", "#66aa00", "#b82e2e", "#316395",
            "#3366cc", "#994499", "#22aa99", "#aaaa11", "#6633cc",
            "#e67300", "#8b0707", "#651067", "#329262", "#5574a6"
        ],
        client: [
            "#3366cc", "#dc3912", "#ff9900", "#109618", "#990099"
        ]
    },
    lineStyles: ["Solid", "Dash", "DashDot", "Dot", "LongDash", "LongDashDot", "ShortDash", "ShortDash", "ShortDashDot", "ShortDashDotDot", "ShortDot"]
};

export const ChartDefinitions: Record<ChartType, ChartDefinition> = {

    line: {
        xAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        yAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        zAxisFieldTypes: [],
        eAxisFieldTypes: [],
        maxSeriesPerAxis: {x: 1, y1: 10, y2: 10, z: 0, e1: 0, e2: 0},
        defaultNumberOfSeriesSelected: {x: 1, y1: 1, y2: 0, z: 0, e1: 0, e2: 0}
    },
    lineWithError: {
        xAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        yAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        zAxisFieldTypes: [],
        eAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        maxSeriesPerAxis: {x: 1, y1: 1, y2: 0, z: 0, e1: 1, e2: 1},
        defaultNumberOfSeriesSelected: {x: 1, y1: 1, y2: 0, z: 0, e1: 0, e2: 0}
    },
    area: {
        xAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        yAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        zAxisFieldTypes: [],
        maxSeriesPerAxis: {x: 1, y1: 10, y2: 10, z: 0, e1: 0, e2: 0},
        defaultNumberOfSeriesSelected: {x: 1, y1: 1, y2: 0, z: 0, e1: 0, e2: 0}
    },
    scatter: {
        xAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        yAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        zAxisFieldTypes: [],
        maxSeriesPerAxis: {x: 1, y1: 10, y2: 10, z: 0, e1: 0, e2: 0},
        defaultNumberOfSeriesSelected: {x: 1, y1: 1, y2: 0, z: 0, e1: 0, e2: 0}
    },
    arearange: {
        xAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        yAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        zAxisFieldTypes: [trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        maxSeriesPerAxis: {x: 1, y1: 1, y2: 1, z: 0, e1: 0, e2: 0},
        defaultNumberOfSeriesSelected: {x: 1, y1: 1, y2: 1, z: 0, e1: 0, e2: 0},
        selectorLabels: {
            y2: "Select the series for the negative range",
            z: "Select the series for the positive range"
        },
    },
    spline: {
        xAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        yAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        zAxisFieldTypes: [],
        maxSeriesPerAxis: {x: 1, y1: 10, y2: 10, z: 0, e1: 0, e2: 0},
        defaultNumberOfSeriesSelected: {x: 1, y1: 1, y2: 0, z: 0, e1: 0, e2: 0}
    },
    areaspline: {
        xAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        yAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        zAxisFieldTypes: [],
        maxSeriesPerAxis: {x: 1, y1: 10, y2: 10, z: 0, e1: 0, e2: 0},
        defaultNumberOfSeriesSelected: {x: 1, y1: 1, y2: 0, z: 0, e1: 0, e2: 0}
    },
    column: {
        categorical: ["x"],
        xAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL, trac.STRING],
        yAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        zAxisFieldTypes: [],
        eAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        maxSeriesPerAxis: {x: 1, y1: 10, y2: 10, z: 0, e1: 0, e2: 0},
        defaultNumberOfSeriesSelected: {x: 1, y1: 1, y2: 0, z: 0, e1: 0, e2: 0}
    },
    columnWithError: {
        categorical: ["x"],
        xAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL, trac.STRING],
        yAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        zAxisFieldTypes: [],
        eAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        maxSeriesPerAxis: {x: 1, y1: 1, y2: 0, z: 0, e1: 1, e2: 1},
        defaultNumberOfSeriesSelected: {x: 1, y1: 1, y2: 0, z: 0, e1: 1, e2: 1}
    },
    bar: {
        categorical: ["x"],
        xAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL, trac.STRING],
        yAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        zAxisFieldTypes: [],
        maxSeriesPerAxis: {x: 1, y1: 10, y2: 10, z: 0, e1: 0, e2: 0},
        defaultNumberOfSeriesSelected: {x: 1, y1: 1, y2: 0, z: 0, e1: 0, e2: 0}
    },
    pie: {
        categorical: ["x"],
        xAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL, trac.STRING],
        yAxisFieldTypes: [trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        zAxisFieldTypes: [],
        maxSeriesPerAxis: {x: 1, y1: 1, y2: 0, z: 0, e1: 0, e2: 0},
        defaultNumberOfSeriesSelected: {x: 1, y1: 1, y2: 0, z: 0, e1: 0, e2: 0}
    },
    circleDoughnut: {
        categorical: ["x"],
        xAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL, trac.STRING],
        yAxisFieldTypes: [trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        zAxisFieldTypes: [],
        maxSeriesPerAxis: {x: 1, y1: 1, y2: 0, z: 0, e1: 0, e2: 0},
        defaultNumberOfSeriesSelected: {x: 1, y1: 1, y2: 0, z: 0, e1: 0, e2: 0}
    },
    semiDoughnut: {
        categorical: ["x"],
        xAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL, trac.STRING],
        yAxisFieldTypes: [trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        zAxisFieldTypes: [],
        maxSeriesPerAxis: {x: 1, y1: 1, y2: 0, z: 0, e1: 0, e2: 0},
        defaultNumberOfSeriesSelected: {x: 1, y1: 1, y2: 0, z: 0, e1: 0, e2: 0}
    },
    histogram: {
        xAxisFieldTypes: [],
        yAxisFieldTypes: [trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL, trac.STRING],
        zAxisFieldTypes: [],
        maxSeriesPerAxis: {x: 0, y1: 1, y2: 0, z: 0, e1: 0, e2: 0},
        defaultNumberOfSeriesSelected: {x: 0, y1: 1, y2: 0, z: 0, e1: 0, e2: 0}
    },
    bubble: {
        xAxisFieldTypes: [trac.STRING, trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        yAxisFieldTypes: [trac.STRING, trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        zAxisFieldTypes: [trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        maxSeriesPerAxis: {x: 1, y1: 1, y2: 0, z: 1, e1: 0, e2: 0},
        defaultNumberOfSeriesSelected: {x: 1, y1: 1, y2: 0, z: 1, e1: 0, e2: 0}
    },
    sankey: {
        categorical: ["x", "y"],
        // y "CATEGORICAL"
        xAxisFieldTypes: [trac.STRING, trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        yAxisFieldTypes: [trac.STRING, trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        zAxisFieldTypes: [trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        maxSeriesPerAxis: {x: 1, y1: 1, y2: 0, z: 1, e1: 0, e2: 0},
        defaultNumberOfSeriesSelected: {x: 1, y1: 1, y2: 0, z: 1, e1: 0, e2: 0}
    },
    dependencywheel: {
        categorical: ["x", "y"],
        xAxisFieldTypes: [trac.STRING, trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        yAxisFieldTypes: [trac.STRING, trac.DATE, trac.DATETIME, trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        zAxisFieldTypes: [trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        maxSeriesPerAxis: {x: 1, y1: 1, y2: 0, z: 1, e1: 0, e2: 0},
        defaultNumberOfSeriesSelected: {x: 1, y1: 1, y2: 0, z: 1, e1: 0, e2: 0}
    },
    waterfall: {
        xAxisFieldTypes: [trac.STRING],
        yAxisFieldTypes: [trac.FLOAT, trac.INTEGER, trac.DECIMAL],
        zAxisFieldTypes: [trac.BOOLEAN],
        maxSeriesPerAxis: {x: 1, y1: 1, y2: 0, z: 1, e1: 0, e2: 0},
        defaultNumberOfSeriesSelected: {x: 1, y1: 1, y2: 0, z: 1, e1: 0, e2: 0}
    }
};