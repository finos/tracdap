/**
 * Some additional configuration for the QueryBuilder component.
 */

import type {Option, QueryButton} from "../../../types/types_general";
import {tracdap as trac} from "@finos/tracdap-web-api";

type Configuration = {
    maxSelectHistory: number
    aggregationButtons: QueryButton[]
    operatorButtons: QueryButton[]
    textButtons: QueryButton[]
    datetimeButtons: QueryButton[]
    numberButtons: QueryButton[]
    uniqueButtons: QueryButton[]
    orderingOptions: Option<string>[]
}

const configuration: Configuration = {

    maxSelectHistory: 20,
    aggregationButtons: [
        {
            value: "count",
            label: "COUNT",
            tooltip: "Count how many non-missing values there are for the selected variable",
            outputLabelStart: "Count of",
            fieldTypes: [trac.STRING, trac.BOOLEAN, trac.INTEGER, trac.FLOAT, trac.DATE, trac.DATETIME],
            outputType: trac.INTEGER,
            outputFormat: ",|.|0||",
            outputCategorical: true
        },
        {
            value: "count_distinct",
            label: "COUNT DISTINCT",
            tooltip: "Count the number of distinct values for the selected variable",
            outputLabelStart: "Count of distinct values of",
            fieldTypes: [trac.STRING, trac.BOOLEAN, trac.INTEGER, trac.FLOAT, trac.DATE, trac.DATETIME],
            outputType: trac.INTEGER,
            outputFormat: ",|.|0||",
            outputCategorical: true
        },
        {
            value: "sum",
            label: "SUM",
            tooltip: "Total value of the selected variable",
            outputLabelStart: "Sum of",
            fieldTypes: [trac.INTEGER, trac.FLOAT],
            outputType: "INHERIT",
            outputFormat: "INHERIT",
            outputCategorical: "INHERIT"
        },
        {
            value: "avg",
            label: "AVERAGE",
            tooltip: "Average value of the selected variable",
            outputLabelStart: "Mean of",
            fieldTypes: [trac.INTEGER, trac.FLOAT, trac.DATE, trac.DATETIME],
            outputType: trac.FLOAT,
            outputFormat: "INHERIT",
            outputCategorical: "INHERIT"
        },
        {
            value: "min",
            label: "MINIMUM",
            tooltip: "Minimum value of the selected variable",
            outputLabelStart: "Minimum of",
            fieldTypes: [trac.INTEGER, trac.FLOAT, trac.DATE, trac.DATETIME],
            outputType: "INHERIT",
            outputFormat: "INHERIT",
            outputCategorical: "INHERIT"
        },
        {
            value: "max",
            label: "MAXIMUM",
            tooltip: "Maximum value of the selected variable",
            outputLabelStart: "Maximum of",
            fieldTypes: [trac.INTEGER, trac.FLOAT, trac.DATE, trac.DATETIME],
            outputType: "INHERIT",
            outputFormat: "INHERIT",
            outputCategorical: "INHERIT"
        }
    ],
    operatorButtons: [
        {value: "+", icon: 'bi-plus-lg', tooltip: "Add", fieldTypes: [trac.INTEGER, trac.FLOAT]},
        {value: "-", icon: 'bi-dash-lg', tooltip: "Minus", fieldTypes: [trac.INTEGER, trac.FLOAT]},
        {value: "*", icon: 'bi-x-lg', tooltip: "Multiply", fieldTypes: [trac.INTEGER, trac.FLOAT]},
        {value: "/", icon: 'bi-slash-lg', tooltip: "Divide", fieldTypes: [trac.INTEGER, trac.FLOAT]},
        {value: "%", icon: 'bi-percent', tooltip: "Modulus", fieldTypes: [trac.INTEGER, trac.FLOAT]}
    ],
    textButtons: [
        {
            value: "uCase",
            label: "UPPER",
            tooltip: "Uppercase",
            outputLabelEnd: "(upper case)",
            fieldTypes: [trac.STRING],
            outputType: trac.STRING,
            outputFormat: "NONE",
            outputCategorical: "INHERIT"
        },
        {
            value: "lCase",
            label: "LOWER",
            tooltip: "Lowercase",
            outputLabelEnd: "(lower case)",
            fieldTypes: [trac.STRING],
            outputType: trac.STRING,
            outputFormat: "NONE",
            outputCategorical: "INHERIT"
        },
    ],
    datetimeButtons: [
        {
            value: "day", label: "DAY",
            tooltip: "Day e.g. 1/09/2021 returns 1",
            outputLabelStart: "Day of",
            fieldTypes: [trac.DATE, trac.DATETIME],
            outputType: trac.INTEGER,
            outputFormat: "|.|0||",
            outputCategorical: true
        },
        {
            value: "month",
            label: "MONTH",
            tooltip: "Month e.g. 1/09/2021 returns 9",
            outputLabelStart: "Month of",
            fieldTypes: [trac.DATE, trac.DATETIME],
            outputType: trac.INTEGER,
            outputFormat: "|.|0||",
            outputCategorical: true
        },
        {
            value: "year",
            label: "YEAR",
            tooltip: "Year e.g. 1/09/2021 returns 2021",
            outputLabelStart: "Year of",
            fieldTypes: [trac.DATE, trac.DATETIME],
            outputType: trac.INTEGER,
            outputFormat: "|.|0||",
            outputCategorical: true
        }
    ],
    numberButtons: [
        {
            value: "round",
            label: "ROUND",
            tooltip: "Round to nearest integer",
            outputLabelEnd: "(rounded)",
            fieldTypes: [trac.INTEGER, trac.FLOAT],
            outputType: trac.INTEGER,
            outputFormat: ",|.|0||",
            outputCategorical: "INHERIT"
        },
        {
            value: "ceil",
            label: "CEIL",
            tooltip: "Round up to integer",
            outputLabelEnd: "(rounded up)",
            fieldTypes: [trac.INTEGER, trac.FLOAT],
            outputType: trac.INTEGER,
            outputFormat: "INHERIT",
            outputCategorical: "INHERIT"
        },
        {
            value: "floor",
            label: "FLOOR",
            tooltip: "Round down to integer",
            outputLabelEnd: "(rounded down)",
            fieldTypes: [trac.INTEGER, trac.FLOAT],
            outputType: trac.INTEGER,
            outputFormat: "INHERIT",
            outputCategorical: "INHERIT"
        }
    ],
    uniqueButtons: [
        {
            value: "distinct",
            label: "DISTINCT",
            tooltip: "Get unique row values",
            fieldTypes: [trac.STRING, trac.BOOLEAN, trac.INTEGER, trac.FLOAT, trac.DATE, trac.DATETIME]
        }
    ],
    orderingOptions: [
        {value: "ASC", label: "Ascending"},
        {value: "DESC", label: "Descending"}
    ]
}

export {configuration}