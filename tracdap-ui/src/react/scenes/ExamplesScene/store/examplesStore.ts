/**
 * This slice acts as the store for the {@link ExamplesScene}.
 * @module examplesStore
 * @category Redux store
 */

import {createSlice} from '@reduxjs/toolkit'
import {DataRow} from "../../../../types/types_general";
//import {Props as ChartProps} from "../../../components/Chart/Highcharts/HighchartsGeneral";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * An interface for the examplesStore.ts Redux store.
 */
export interface ExamplesStoreState {
    inputs: {
        set1: {
            data: DataRow[],
            fields: trac.metadata.IFieldSchema[]
        }
        set2: {
            data: DataRow[],
            fields: trac.metadata.IFieldSchema[]
        }
        set3: {
            data: DataRow[],
            fields: trac.metadata.IFieldSchema[]
        }
        set4: {
            data: DataRow[],
            fields: trac.metadata.IFieldSchema[]
        }
        set5: {
            data: DataRow[],
            fields: trac.metadata.IFieldSchema[]
        }
        set6: {
            data: DataRow[],
            fields: trac.metadata.IFieldSchema[]
        }
        set7: {
            data: DataRow[],
            fields: trac.metadata.IFieldSchema[]
        }
        set8: {
            data: DataRow[],
            fields: trac.metadata.IFieldSchema[]
        }
    }
    // charts: (Omit<ChartProps, "data" | "fields"> & { input: keyof ExamplesStoreState["inputs"] })[]
}

// This is the initial state of the store.
const initialState: ExamplesStoreState = {

    // TOD depending on how bug the examples get these might be better moved to the TRAC API
    inputs: {
        set1: {
            data: [
                {
                    "OBSERVATION_DATE": "2021-02-01",
                    "DRAWN_BALANCE": 200,
                    "CREDIT_LIMIT": 450.98,
                    "DRAWN_BALANCE_PLUS_SIGMA": 230,
                    "DRAWN_BALANCE_MINUS_SIGMA": 170,
                },
                {
                    "OBSERVATION_DATE": "2021-01-01",
                    "DRAWN_BALANCE": 1000.9,
                    "CREDIT_LIMIT": 2500.6,
                    "DRAWN_BALANCE_PLUS_SIGMA": 1030.9,
                    "DRAWN_BALANCE_MINUS_SIGMA": 970.9,
                },
                {
                    "OBSERVATION_DATE": "2021-03-01",
                    "DRAWN_BALANCE": 300,
                    "CREDIT_LIMIT": 300.7,
                    "DRAWN_BALANCE_PLUS_SIGMA": 340,
                    "DRAWN_BALANCE_MINUS_SIGMA": 250,
                },
                {
                    "OBSERVATION_DATE": "2021-04-01",
                    "DRAWN_BALANCE": 600,
                    "CREDIT_LIMIT": 600.7,
                    "DRAWN_BALANCE_PLUS_SIGMA": 660,
                    "DRAWN_BALANCE_MINUS_SIGMA": 540,
                },
                {
                    "OBSERVATION_DATE": "2021-05-01",
                    "DRAWN_BALANCE": 700,
                    "CREDIT_LIMIT": 750.7,
                    "DRAWN_BALANCE_PLUS_SIGMA": 740,
                    "DRAWN_BALANCE_MINUS_SIGMA": 660
                },
                {
                    "OBSERVATION_DATE": "2021-06-01",
                    "DRAWN_BALANCE": 800,
                    "CREDIT_LIMIT": 8.25,
                    "DRAWN_BALANCE_PLUS_SIGMA": 880,
                    "DRAWN_BALANCE_MINUS_SIGMA": 760
                },
                {
                    "OBSERVATION_DATE": "2021-07-01",
                    "DRAWN_BALANCE": 825,
                    "CREDIT_LIMIT": 835,
                    "DRAWN_BALANCE_PLUS_SIGMA": 900,
                    "DRAWN_BALANCE_MINUS_SIGMA": 750
                },
                {
                    "OBSERVATION_DATE": "2021-08-01",
                    "DRAWN_BALANCE": 800,
                    "CREDIT_LIMIT": 850,
                    "DRAWN_BALANCE_PLUS_SIGMA": 860,
                    "DRAWN_BALANCE_MINUS_SIGMA": 720
                }
            ],
            fields: [
                {fieldName: "OBSERVATION_DATE", formatCode: "QUARTER", label: "Date", fieldType: trac.DATE, categorical: false},
                {fieldName: "DRAWN_BALANCE", fieldType: trac.FLOAT, label: "Balance", categorical: false, formatCode: ",|.|1|£|m"},
                {fieldName: "CREDIT_LIMIT", fieldType: trac.FLOAT, categorical: false, formatCode: ",|.|1|£|m"},
                {
                    fieldName: "DRAWN_BALANCE_PLUS_SIGMA",
                    fieldType: trac.FLOAT,
                    label: "Balance distribution (plus 1 standard deviation)",
                    categorical: false,
                    formatCode: ",|.|1|£|m"
                },
                {
                    fieldName: "DRAWN_BALANCE_MINUS_SIGMA",
                    fieldType: trac.FLOAT,
                    label: "Balance distribution (minus 1 standard deviation)",
                    categorical: false,
                    formatCode: ",|.|1|£|m"
                },
            ]
        },
        set2: {
            data: [
                {
                    "BRAND": "Acme Co.",
                    "REVENUE": 100,
                },
                {
                    "BRAND": "Umbrella Inc.",
                    "REVENUE": 50,
                },
                {
                    "BRAND": "Digital Writes Ltd.",
                    "REVENUE": 25,
                },
                {
                    "BRAND": "Free Press Co.",
                    "REVENUE": 20,
                }
            ],
            fields: [
                {fieldName: "BRAND", label: "Brand", fieldType: trac.STRING, categorical: true},
                {fieldName: "REVENUE", fieldType: trac.FLOAT, label: "Annual revenue", categorical: false, formatCode: ",|.|0|$|bn"}
            ]
        },
        set3: {
            data: [
                {PD: 3.5}, {PD: 3}, {PD: 3.2}, {PD: 3.1}, {PD: 3.6}, {PD: 3.9}, {PD: 3.4}, {PD: 3.4}, {PD: 2.9}, {PD: 3.1}, {PD: 3.7},
                {PD: 3.4}, {PD: 3}, {PD: 3}, {PD: 4}, {PD: 4.4}, {PD: 3.9}, {PD: 3.5}, {PD: 3.8}, {PD: 3.8}, {PD: 3.4}, {PD: 3.7},
                {PD: 3.6}, {PD: 3.3}, {PD: 3.4}, {PD: 3}, {PD: 3.4}, {PD: 3.5}, {PD: 3.4}, {PD: 3.2}, {PD: 3.1}, {PD: 3.4}, {PD: 4.1},
                {PD: 4.2}, {PD: 3.1}, {PD: 3.2}, {PD: 3.5}, {PD: 3.6}, {PD: 3}, {PD: 3.4}, {PD: 3.5}, {PD: 2.3}, {PD: 3.2}, {PD: 3.5},
                {PD: 3.8}, {PD: 3}, {PD: 3.8}, {PD: 3.2}, {PD: 3.7}, {PD: 3.3}, {PD: 3.2}, {PD: 3.2}, {PD: 3.1}, {PD: 2.3}, {PD: 2.8},
                {PD: 2.8}, {PD: 3.3}, {PD: 2.4}, {PD: 2.9}, {PD: 2.7}, {PD: 2}, {PD: 3}, {PD: 2.2}, {PD: 2.9}, {PD: 2.9}, {PD: 3.1},
                {PD: 3}, {PD: 2.7}, {PD: 2.2}, {PD: 2.5}, {PD: 3.2}, {PD: 2.8}, {PD: 2.5}, {PD: 2.8}, {PD: 2.9}, {PD: 3}, {PD: 2.8},
                {PD: 3}, {PD: 2.9}, {PD: 2.6}, {PD: 2.4}, {PD: 2.4}, {PD: 2.7}, {PD: 2.7}, {PD: 3}, {PD: 3.4}, {PD: 3.1}, {PD: 2.3},
                {PD: 3}, {PD: 2.5}, {PD: 2.6}, {PD: 3}, {PD: 2.6}, {PD: 2.3}, {PD: 2.7}, {PD: 3}, {PD: 2.9}, {PD: 2.9}, {PD: 2.5},
                {PD: 2.8}, {PD: 3.3}, {PD: 2.7}, {PD: 3}, {PD: 2.9}, {PD: 3}, {PD: 3}, {PD: 2.5}, {PD: 2.9}, {PD: 2.5}, {PD: 3.6},
                {PD: 3.2}, {PD: 2.7}, {PD: 3}, {PD: 2.5}, {PD: 2.8}, {PD: 3.2}, {PD: 3}, {PD: 3.8}, {PD: 2.6}, {PD: 2.2}, {PD: 3.2},
                {PD: 2.8}, {PD: 2.8}, {PD: 2.7}, {PD: 3.3}, {PD: 3.2}, {PD: 2.8}, {PD: 3}, {PD: 2.8}, {PD: 3}, {PD: 2.8}, {PD: 3.8},
                {PD: 2.8}, {PD: 2.8}, {PD: 2.6}, {PD: 3}, {PD: 3.4}, {PD: 3.1}, {PD: 3}, {PD: 3.1}, {PD: 3.1}, {PD: 3.1}, {PD: 2.7},
                {PD: 3.2}, {PD: 3.3}, {PD: 3}, {PD: 2.5}, {PD: 3}, {PD: 3.4}, {PD: 3}
            ],
            fields: [
                {fieldName: "PD", label: "Probability of default", fieldType: trac.FLOAT, categorical: false, formatCode: ",|.|1||%"}
            ]
        },
        set4: {
            data: [
                {organiser: 'Brazil', recipient: 'France', awards: 1},
                {organiser: 'Brazil', recipient: 'Spain', awards: 1},
                {organiser: 'Brazil', recipient: 'England', awards: 1},
                {organiser: 'Canada', recipient: 'Portugal', awards: 1},
                {organiser: 'Canada', recipient: 'France', awards: 5},
                {organiser: 'Canada', recipient: 'England', awards: 1},
                {organiser: 'Mexico', recipient: 'Portugal', awards: 1},
                {organiser: 'Mexico', recipient: 'France', awards: 1},
                {organiser: 'Mexico', recipient: 'Spain', awards: 5},
                {organiser: 'Mexico', recipient: 'England', awards: 1},
                {organiser: 'USA', recipient: 'Portugal', awards: 1},
                {organiser: 'USA', recipient: 'France', awards: 1},
                {organiser: 'USA', recipient: 'Spain', awards: 1},
                {organiser: 'USA', recipient: 'England', awards: 5},
                {organiser: 'Portugal', recipient: 'Angola', awards: 2},
                {organiser: 'Portugal', recipient: 'Senegal', awards: 1},
                {organiser: 'Portugal', recipient: 'Morocco', awards: 1},
                {organiser: 'Portugal', recipient: 'South Africa', awards: 3},
                {organiser: 'France', recipient: 'Angola', awards: 1},
                {organiser: 'France', recipient: 'Senegal', awards: 3},
                {organiser: 'France', recipient: 'Mali', awards: 3},
                {organiser: 'France', recipient: 'Morocco', awards: 3},
                {organiser: 'France', recipient: 'South Africa', awards: 1},
                {organiser: 'Spain', recipient: 'Senegal', awards: 1},
                {organiser: 'Spain', recipient: 'Morocco', awards: 3},
                {organiser: 'Spain', recipient: 'South Africa', awards: 1},
                {organiser: 'England', recipient: 'Angola', awards: 1},
                {organiser: 'England', recipient: 'Senegal', awards: 1},
                {organiser: 'England', recipient: 'Morocco', awards: 2},
                {organiser: 'England', recipient: 'South Africa', awards: 7},
                {organiser: 'South Africa', recipient: 'China', awards: 5},
                {organiser: 'South Africa', recipient: 'India', awards: 1},
                {organiser: 'South Africa', recipient: 'Japan', awards: 3},
                {organiser: 'Angola', recipient: 'China', awards: 5},
                {organiser: 'Angola', recipient: 'India', awards: 1},
                {organiser: 'Angola', recipient: 'Japan', awards: 3},
                {organiser: 'Senegal', recipient: 'China', awards: 5},
                {organiser: 'Senegal', recipient: 'India', awards: 1},
                {organiser: 'Senegal', recipient: 'Japan', awards: 3},
                {organiser: 'Mali', recipient: 'China', awards: 5},
                {organiser: 'Mali', recipient: 'India', awards: 1},
                {organiser: 'Mali', recipient: 'Japan', awards: 3},
                {organiser: 'Morocco', recipient: 'China', awards: 5},
                {organiser: 'Morocco', recipient: 'India', awards: 1},
                {organiser: 'Morocco', recipient: 'Japan', awards: 3}
            ],
            fields: [
                {fieldName: "organiser", label: "Organising country", fieldType: trac.STRING, categorical: true},
                {fieldName: "recipient", label: "Receiving country", fieldType: trac.STRING, categorical: true},
                {fieldName: "awards", label: "Awards", fieldType: trac.INTEGER, categorical: false, formatCode: "||1|£|bn"}
            ]
        },
        set5: {
            data: [
                {label: 'Last year', change: 120000, isIntermediateSum: true},
                {label: 'Product Revenue', change: 569000, isIntermediateSum: false},
                {label: 'Service Revenue', change: 231000, isIntermediateSum: false},
                {label: 'Variable Costs', change: -233000, isIntermediateSum: false},
                {label: 'Fixed Costs', change: -342000, isIntermediateSum: false},
                {label: 'This year', change: 345000, isIntermediateSum: true}
            ],
            fields: [
                {fieldName: "label", label: "Label", fieldType: trac.STRING, categorical: false},
                {fieldName: "change", label: "Increment", fieldType: trac.INTEGER, formatCode: ",|.|0|£|m"},
                {fieldName: "isIntermediateSum", label: "Is sum", fieldType: trac.BOOLEAN}
            ]
        },
        set6: {
            data: [
                {
                    "OBSERVATION_DATE": "2021-01-01",
                    "DTV": 0.1,
                    "CREDIT_LIMIT": 2500.6,
                },
                {
                    "OBSERVATION_DATE": "2021-02-01",
                    "DTV": 0.2,
                    "CREDIT_LIMIT": 450.98
                },
                {
                    "OBSERVATION_DATE": "2021-03-01",
                    "DTV": 0.3,
                    "CREDIT_LIMIT": 300.7
                },
                {
                    "OBSERVATION_DATE": "2021-04-01",
                    "DTV": 0.35,
                    "CREDIT_LIMIT": 600.7
                },
                {
                    "OBSERVATION_DATE": "2021-05-01",
                    "DTV": 0.3,
                    "CREDIT_LIMIT": 750.7
                },
                {
                    "OBSERVATION_DATE": "2021-06-01",
                    "DTV": 0.5,
                    "CREDIT_LIMIT": 8.25
                },
                {
                    "OBSERVATION_DATE": "2021-07-01",
                    "DTV": 0.55,
                    "CREDIT_LIMIT": 835
                },
                {
                    "OBSERVATION_DATE": "2021-08-01",
                    "DTV": 0.7,
                    "CREDIT_LIMIT": 850
                }
            ],
            fields: [
                {fieldName: "OBSERVATION_DATE", formatCode: "MONTH", label: "Date", fieldType: trac.DATE, categorical: false},
                {fieldName: "DTV", fieldType: trac.FLOAT, label: "Debt to value", categorical: false, formatCode: ",|.|1||%|100"},
                {fieldName: "CREDIT_LIMIT", fieldType: trac.FLOAT, categorical: false, formatCode: ",|.|1|$|bn"}
            ]
        },
        set7: {
            data: [
                {
                    "OBSERVATION_DATE": "2021-01-01",
                    "PORTFOLIO": "MAINSTREAM",
                    "INTEREST_RATE_TYPE": 0,
                    "DEFAULT_RATE": 0.01,
                    "CURE_RATE_RATE": 0.01,
                },
                {
                    "OBSERVATION_DATE": "2021-02-01",
                    "PORTFOLIO": "MAINSTREAM",
                    "INTEREST_RATE_TYPE": 0,
                    "DEFAULT_RATE": 0.015,
                    "CURE_RATE_RATE": 0.015,
                },
                {
                    "OBSERVATION_DATE": "2021-03-01",
                    "PORTFOLIO": "MAINSTREAM",
                    "INTEREST_RATE_TYPE": 0,
                    "DEFAULT_RATE": 0.03,
                    "CURE_RATE_RATE": 0.02,
                },
                {
                    "OBSERVATION_DATE": "2021-04-01",
                    "PORTFOLIO": "MAINSTREAM",
                    "INTEREST_RATE_TYPE": 0,
                    "DEFAULT_RATE": 0.02,
                    "CURE_RATE_RATE": 0.005,
                },
                {
                    "OBSERVATION_DATE": "2021-01-01",
                    "PORTFOLIO": "BTL",
                    "INTEREST_RATE_TYPE": 0,
                    "DEFAULT_RATE": 0.02,
                    "CURE_RATE_RATE": 0.02,
                },
                {
                    "OBSERVATION_DATE": "2021-02-01",
                    "PORTFOLIO": "BTL",
                    "INTEREST_RATE_TYPE": 0,
                    "DEFAULT_RATE": 0.04,
                    "CURE_RATE_RATE": 0.04,
                },
                {
                    "OBSERVATION_DATE": "2021-03-01",
                    "PORTFOLIO": "BTL",
                    "INTEREST_RATE_TYPE": 0,
                    "DEFAULT_RATE": 0.08,
                    "CURE_RATE_RATE": 0.03,
                },
                {
                    "OBSERVATION_DATE": "2021-04-01",
                    "PORTFOLIO": "BTL",
                    "INTEREST_RATE_TYPE": 0,
                    "DEFAULT_RATE": 0.04,
                    "CURE_RATE_RATE": 0.01,
                },
                {
                    "OBSERVATION_DATE": "2021-01-01",
                    "PORTFOLIO": "MAINSTREAM",
                    "INTEREST_RATE_TYPE": 1,
                    "DEFAULT_RATE": 0.03,
                    "CURE_RATE_RATE": 0.03,
                },
                {
                    "OBSERVATION_DATE": "2021-02-01",
                    "PORTFOLIO": "MAINSTREAM",
                    "INTEREST_RATE_TYPE": 1,
                    "DEFAULT_RATE": 0.045,
                    "CURE_RATE_RATE": 0.045,
                },
                {
                    "OBSERVATION_DATE": "2021-03-01",
                    "PORTFOLIO": "MAINSTREAM",
                    "INTEREST_RATE_TYPE": 1,
                    "DEFAULT_RATE": 0.09,
                    "CURE_RATE_RATE": 0.06,
                },
                {
                    "OBSERVATION_DATE": "2021-04-01",
                    "PORTFOLIO": "MAINSTREAM",
                    "INTEREST_RATE_TYPE": 1,
                    "DEFAULT_RATE": 0.06,
                    "CURE_RATE_RATE": 0.015,
                }
            ],
            fields: [
                {fieldName: "OBSERVATION_DATE", formatCode: "QUARTER", label: "Date", fieldType: trac.DATE},
                {fieldName: "PORTFOLIO", label: "Portfolio type", fieldType: trac.STRING, categorical: true},
                {fieldName: "INTEREST_RATE_TYPE", fieldType: trac.INTEGER, label: "Interest rate type", formatCode: ",|.|0|||1"},
                {fieldName: "DEFAULT_RATE", fieldType: trac.FLOAT, label: "Default rate", formatCode: ",|.|3||%|100"},
                {fieldName: "CURE_RATE_RATE", fieldType: trac.FLOAT, label: "Cure rate", formatCode: ",|.|3||%|100"}
            ]
        },
        set8: {
            data: [

                {
                    "OBSERVATION_DATE": "2021-01-01",
                    "DRAWN_BALANCE": 200
                },
                {
                    "OBSERVATION_DATE": "2021-02-01",
                    "DRAWN_BALANCE": null,
                },
                {
                    "OBSERVATION_DATE": "2021-03-01",
                    "DRAWN_BALANCE": 300
                },
                {
                    "OBSERVATION_DATE": "2021-04-01",
                    "DRAWN_BALANCE": 600
                },
                {
                    "OBSERVATION_DATE": "2021-05-01",
                    "DRAWN_BALANCE": null
                },
                {
                    "OBSERVATION_DATE": "2021-06-01",
                    "DRAWN_BALANCE": 800
                },
                {
                    "OBSERVATION_DATE": "2021-07-01",
                    "DRAWN_BALANCE": null
                },
                {
                    "OBSERVATION_DATE": "2021-08-01",
                    "DRAWN_BALANCE": 800
                }
            ],
            fields: [
                {fieldName: "OBSERVATION_DATE", formatCode: "QUARTER", label: "Date", fieldType: trac.DATE, categorical: false},
                {fieldName: "DRAWN_BALANCE", fieldType: trac.FLOAT, label: "Balance", categorical: false, formatCode: ",|.|1|£|m"},
            ]
        },
    },
    // charts: [
    //     // {
    //     //     input: "set1",
    //     //     canExport: true,
    //     //     isTracData: false,
    //     //     savedChartState: {
    //     //         title: "Variables automatically set",
    //     //         chartType: "line",
    //     //         defaultVariables: undefined,
    //     //         defaultSegmentationVariables: [],
    //     //         showMarkers: true,
    //     //         hideAxes: false,
    //     //         chartHeight: 300,
    //     //         chartHeightIsPercent: false,
    //     //         chartZoom: "xy",
    //     //         tooltipIsShared: true,
    //     //         allowAllSegmentsToBePlotted: false,
    //     //         useLineStyles: false
    //     //     }
    //     // },
    //     {
    //         input: "set1",
    //         canExport: true,
    //         isTracData: false,
    //         savedChartState: {
    //             title: "x and y variables set manually",
    //             chartType: "line",
    //             defaultVariables: {x: ["OBSERVATION_DATE"], y1: ["DRAWN_BALANCE", "CREDIT_LIMIT"], y2: ["DRAWN_BALANCE", "CREDIT_LIMIT"]},
    //             defaultSegmentationVariables: [],
    //             tooltipIsShared: true,
    //             showMarkers: true,
    //             useLineStyles: true,
    //             hideAxes: false,
    //             chartHeight: 300,
    //             chartHeightIsPercent: false,
    //             chartZoom: "xy",
    //             allowAllSegmentsToBePlotted: false
    //         }
    //     },
    //     // {
    //     //     input: "set1",
    //     //     canExport: true,
    //     //     isTracData: false,
    //     //     savedChartState: {
    //     //         title: "x and y variables set to be empty",
    //     //         chartType: "line",
    //     //         defaultVariables: {x: [], y1: [], y2: []},
    //     //         defaultSegmentationVariables: [],
    //     //         tooltipIsShared: true,
    //     //         showMarkers: true,
    //     //         useLineStyles: true,
    //     //         hideAxes: false,
    //     //         chartHeight: 300,
    //     //         chartHeightIsPercent: false,
    //     //         chartZoom: "xy",
    //     //         allowAllSegmentsToBePlotted: false
    //     //     }
    //     // },
    //     // {
    //     //     input: "set6",
    //     //     canExport: true,
    //     //     isTracData: false,
    //     //     savedChartState: {
    //     //         title: "Line chart, x and y variables set manually, % on y1",
    //     //         chartType: "line",
    //     //         defaultVariables: {x: ["OBSERVATION_DATE"], y1: ["DTV"]},
    //     //         defaultSegmentationVariables: [],
    //     //         tooltipIsShared: true,
    //     //         showMarkers: true,
    //     //         useLineStyles: true,
    //     //         hideAxes: false,
    //     //         chartHeight: 300,
    //     //         chartHeightIsPercent: false,
    //     //         chartZoom: "xy",
    //     //         allowAllSegmentsToBePlotted: false
    //     //     }
    //     // },
    //     // {
    //     //     input: "set6",
    //     //     canExport: true,
    //     //     isTracData: false,
    //     //     savedChartState: {
    //     //         title: "Line chart, x and y variables set manually, % and $ units on y1",
    //     //         chartType: "line",
    //     //         defaultVariables: {x: ["OBSERVATION_DATE"], y1: ["DTV", "CREDIT_LIMIT"]},
    //     //         defaultSegmentationVariables: [],
    //     //         tooltipIsShared: true,
    //     //         showMarkers: true,
    //     //         useLineStyles: true,
    //     //         hideAxes: false,
    //     //         chartHeight: 600,
    //     //         chartHeightIsPercent: false,
    //     //         chartZoom: "xy",
    //     //         allowAllSegmentsToBePlotted: false
    //     //     }
    //     // },
    //     //         {
    //     //     input: "set8",
    //     //     canExport: true,
    //     //     isTracData: false,
    //     //     savedChartState: {
    //     //         title: "Check on identification of isolated points",
    //     //         chartType: "line",
    //     //         defaultVariables: {x: ["OBSERVATION_DATE"], y1: ["DRAWN_BALANCE"]},
    //     //         tooltipIsShared: true,
    //     //         showMarkers: false,
    //     //         useLineStyles: true,
    //     //         hideAxes: false,
    //     //         chartHeight: 300,
    //     //         chartHeightIsPercent: false,
    //     //         chartZoom: "xy",
    //     //         allowAllSegmentsToBePlotted: false
    //     //     }
    //     // },
    //     // {
    //     //     input: "set7",
    //     //     canExport: true,
    //     //     isTracData: false,
    //     //     savedChartState: {
    //     //         title: "Line chart with segment options, start with automatic filtering",
    //     //         chartType: "line",
    //     //         defaultVariables: {x: ["OBSERVATION_DATE"], y1: ["DEFAULT_RATE", "CURE_RATE"]},
    //     //         //defaultSegmentationVariables: [],
    //     //         tooltipIsShared: true,
    //     //         showMarkers: true,
    //     //         useLineStyles: true,
    //     //         hideAxes: false,
    //     //         chartHeight: 300,
    //     //         chartHeightIsPercent: false,
    //     //         chartZoom: "xy",
    //     //         allowAllSegmentsToBePlotted: false
    //     //     }
    //     // },
    //     // {
    //     //     input: "set7",
    //     //     canExport: true,
    //     //     isTracData: false,
    //     //     savedChartState: {
    //     //         title: "Line chart with segment options, start by showing all",
    //     //         chartType: "line",
    //     //         defaultVariables: {x: ["OBSERVATION_DATE"], y1: ["DEFAULT_RATE", "CURE_RATE"]},
    //     //         //defaultSegmentationVariables: [],
    //     //         tooltipIsShared: true,
    //     //         showMarkers: true,
    //     //         useLineStyles: true,
    //     //         hideAxes: false,
    //     //         chartHeight: 300,
    //     //         chartHeightIsPercent: false,
    //     //         chartZoom: "xy",
    //     //         allowAllSegmentsToBePlotted: true
    //     //     }
    //     // },
    //
    //     // Multiple date series on y with different formats
    //     // Data with isolated points
    //     // {
    //     //     title: "Chart chart with error bar",
    //     //     input: "set1",
    //     //     chartType: "lineWithError",
    //     //     defaultXVariables: ["OBSERVATION_DATE"],
    //     //     defaultY1Variables: ["DRAWN_BALANCE"],
    //     //     defaultY2Variables: [],
    //     //     defaultE1Variables: ["DRAWN_BALANCE"],
    //     //     defaultE2Variables: ["CREDIT_LIMIT"],
    //     //     defaultSegmentationVariables: [],
    //     //     tooltipIsShared: true,
    //     //     showMarkers: true,
    //     //     useLineStyles: true,
    //     //             isTracData: false
    //     // },
    //     // {
    //     //     title: "Column chart with error bar",
    //     //     input: "set1",
    //     //     chartType: "columnWithError",
    //     //     defaultXVariables: ["OBSERVATION_DATE"],
    //     //     defaultY1Variables: ["DRAWN_BALANCE"],
    //     //     defaultY2Variables: [],
    //     //     defaultE1Variables: ["DRAWN_BALANCE"],
    //     //     defaultE2Variables: ["CREDIT_LIMIT"],
    //     //     defaultSegmentationVariables: [],
    //     //     tooltipIsShared: true,
    //     //     showMarkers: true,
    //     //     useLineStyles: true,
    //     //             isTracData: false
    //     // },
    //     // {
    //     //     title: "Basic line chart, x and y variables the same ",
    //     //     input: "set1",
    //     //     chartType: "line",
    //     //     defaultXVariables: ["CREDIT_LIMIT"],
    //     //     defaultY1Variables: ["CREDIT_LIMIT"],
    //     //     defaultY2Variables: [],
    //     //     defaultSegmentationVariables: [],
    //     //     tooltipIsShared: true,
    //     //     showMarkers: true,
    //     //     useLineStyles: true,
    //     //             isTracData: false
    //     // },
    //     // {
    //     //     title: "Basic area chart, x and y variables set manually, shared tooltip, markers on",
    //     //     input: "set1",
    //     //     chartType: "area",
    //     //     defaultXVariables: ["OBSERVATION_DATE"],
    //     //     defaultY1Variables: ["DRAWN_BALANCE"],
    //     //     defaultY2Variables: [],
    //     //     defaultSegmentationVariables: [],
    //     //     tooltipIsShared: true,
    //     //     showMarkers: true,
    //     //     useLineStyles: false,
    //     //             isTracData: false
    //     // },
    //     // {
    //     //     title: "Basic area spline chart, x and y variables set manually, shared tooltip, markers off",
    //     //     input: "set1",
    //     //     chartType: "areaspline",
    //     //     defaultXVariables: ["OBSERVATION_DATE"],
    //     //     defaultY1Variables: ["DRAWN_BALANCE"],
    //     //     defaultY2Variables: ["CREDIT_LIMIT"],
    //     //     defaultSegmentationVariables: [],
    //     //     tooltipIsShared: true,
    //     //     showMarkers: false,
    //     //     useLineStyles: false,
    //     //             isTracData: false
    //     // },
    //     // {
    //     //     title: "Basic spline chart, x and y variables set manually, shared tooltip, markers on",
    //     //     input: "set1",
    //     //     chartType: "spline",
    //     //     defaultXVariables: ["OBSERVATION_DATE"],
    //     //     defaultY1Variables: ["DRAWN_BALANCE"],
    //     //     defaultY2Variables: ["CREDIT_LIMIT"],
    //     //     defaultSegmentationVariables: [],
    //     //
    //     //     tooltipIsShared: true,
    //     //     showMarkers: false,
    //     //     useLineStyles: false,
    //     //             isTracData: false
    //     // },
    //     //
    //     {
    //         input: "set1",
    //         canExport: true,
    //         isTracData: false,
    //         savedChartState: {
    //             title: "x and y variables set manually",
    //             chartType: "scatter",
    //             defaultVariables: {x: ["OBSERVATION_DATE"], y1: ["DRAWN_BALANCE", "CREDIT_LIMIT"]},
    //             defaultSegmentationVariables: [],
    //             tooltipIsShared: true,
    //             showMarkers: true,
    //             useLineStyles: true,
    //             hideAxes: false,
    //             chartHeight: 300,
    //             chartHeightIsPercent: false,
    //             chartZoom: "xy",
    //             allowAllSegmentsToBePlotted: false
    //         }
    //     },
    //     {
    //         input: "set1",
    //         canExport: true,
    //         isTracData: false,
    //         savedChartState: {
    //             title: "x and y variables set manually",
    //             chartType: "area",
    //             defaultVariables: {x: ["OBSERVATION_DATE"], y1: ["DRAWN_BALANCE", "CREDIT_LIMIT"]},
    //             defaultSegmentationVariables: [],
    //             tooltipIsShared: true,
    //             showMarkers: true,
    //             useLineStyles: true,
    //             hideAxes: false,
    //             chartHeight: 300,
    //             chartHeightIsPercent: false,
    //             chartZoom: "xy",
    //             allowAllSegmentsToBePlotted: false
    //         }
    //     },
    //     {
    //         input: "set1",
    //         canExport: true,
    //         isTracData: false,
    //         savedChartState: {
    //             title: "x and y variables set manually",
    //             chartType: "spline",
    //             defaultVariables: {x: ["OBSERVATION_DATE"], y1: ["DRAWN_BALANCE", "CREDIT_LIMIT"]},
    //             defaultSegmentationVariables: [],
    //             tooltipIsShared: true,
    //             showMarkers: true,
    //             useLineStyles: true,
    //             hideAxes: false,
    //             chartHeight: 300,
    //             chartHeightIsPercent: false,
    //             chartZoom: "xy",
    //             allowAllSegmentsToBePlotted: false
    //         }
    //     },
    //     // {
    //     //     title: "Area chart with a range set",
    //     //     input: "set1",
    //     //     chartType: "arearange",
    //     //     defaultXVariables: ["OBSERVATION_DATE"],
    //     //     defaultY1Variables: ["DRAWN_BALANCE_MINUS_SIGMA"],
    //     //     defaultY2Variables: ["DRAWN_BALANCE_PLUS_SIGMA"],
    //     //     defaultZVariables: [],
    //     //     defaultSegmentationVariables: [],
    //     //
    //     //     tooltipIsShared: false,
    //     //     showMarkers: false,
    //     //     useLineStyles: true,
    //     //             isTracData: false
    //     // },
    //     // {
    //     //     title: "Basic column chart",
    //     //     input: "set1",
    //     //     chartType: "column",
    //     //     defaultXVariables: ["OBSERVATION_DATE"],
    //     //     defaultY1Variables: ["DRAWN_BALANCE"],
    //     //     defaultY2Variables: ["CREDIT_LIMIT", "DRAWN_BALANCE"],
    //     //     defaultZVariables: [],
    //     //     defaultSegmentationVariables: [],
    //     //     tooltipIsShared: false,
    //     //     showMarkers: true,
    //     //     useLineStyles: true,
    //     //             isTracData: false
    //     // },
    //     // {
    //     //     title: "Basic bar chart",
    //     //     input: "set1",
    //     //     chartType: "bar",
    //     //     defaultXVariables: ["CREDIT_LIMIT"],
    //     //     defaultY1Variables: ["OBSERVATION_DATE"],
    //     //     defaultY2Variables: ["CREDIT_LIMIT"],
    //     //     defaultZVariables: [],
    //     //     defaultSegmentationVariables: [],
    //     //     tooltipIsShared: false,
    //     //     showMarkers: true,
    //     //     useLineStyles: true,
    //     //             isTracData: false
    //     // },
    //     // {
    //     //     title: "Basic bar chart",
    //     //     input: "set1",
    //     //     chartType: "bar",
    //     //     defaultXVariables: ["OBSERVATION_DATE"],
    //     //     defaultY1Variables: ["DRAWN_BALANCE", "CREDIT_LIMIT"],
    //     //     defaultY2Variables: ["CREDIT_LIMIT", "DRAWN_BALANCE"],
    //     //     defaultZVariables: [],
    //     //     defaultSegmentationVariables: [],
    //     //     tooltipIsShared: false,
    //     //     showMarkers: true,
    //     //     useLineStyles: true,
    //     //     pointPadding: 0,
    //     //             isTracData: false
    //     // },
    //     // {
    //     //     title: "Column chart with date as y-axis",
    //     //     input: "set1",
    //     //     chartType: "column",
    //     //     defaultXVariables: ["CREDIT_LIMIT"],
    //     //     defaultY1Variables: ["OBSERVATION_DATE"],
    //     //     defaultY2Variables: [],
    //     //     defaultZVariables: [],
    //     //     defaultSegmentationVariables: [],
    //     //     tooltipIsShared: false,
    //     //     showMarkers: true,
    //     //     useLineStyles: true,
    //     //             isTracData: false
    //     // },
    //     // {
    //     //     title: "Column chart",
    //     //     input: "set1",
    //     //     chartType: "column",
    //     //     defaultXVariables: ["CREDIT_LIMIT"],
    //     //     defaultY1Variables: ["DRAWN_BALANCE"],
    //     //     defaultY2Variables: [],
    //     //     defaultZVariables: [],
    //     //     defaultSegmentationVariables: [],
    //     //     tooltipIsShared: false,
    //     //     showMarkers: true,
    //     //     useLineStyles: true,
    //     //             isTracData: false
    //     // },
    //     // {
    //     //     title: "Basic pie chart",
    //     //     input: "set2",
    //     //     chartType: "pie",
    //     //     defaultXVariables: ["BRAND"],
    //     //     defaultY1Variables: ["REVENUE"],
    //     //     defaultY2Variables: [],
    //     //     defaultZVariables: [],
    //     //     defaultSegmentationVariables: [],
    //     //     tooltipIsShared: false,
    //     //     showMarkers: true,
    //     //     useLineStyles: true,
    //     //             isTracData: false
    //     // },
    //     // {
    //     //     title: "Basic histogram",
    //     //     input: "set3",
    //     //     chartType: "histogram",
    //     //     defaultXVariables: [],
    //     //     defaultY1Variables: ["PD"],
    //     //     defaultY2Variables: [],
    //     //     defaultZVariables: [],
    //     //     defaultSegmentationVariables: [],
    //     //     tooltipIsShared: false,
    //     //     showMarkers: true,
    //     //     useLineStyles: true,
    //     //             isTracData: false
    //     // },
    //     // {
    //     //     title: "Basic bubble with date as x-axis",
    //     //     input: "set1",
    //     //     chartType: "bubble",
    //     //     defaultXVariables: ["OBSERVATION_DATE"],
    //     //     defaultY1Variables: ["CREDIT_LIMIT"],
    //     //     defaultY2Variables: [],
    //     //     defaultZVariables: ["DRAWN_BALANCE"],
    //     //     defaultSegmentationVariables: [],
    //     //     tooltipIsShared: false,
    //     //     showMarkers: true,
    //     //     useLineStyles: true,
    //     //             isTracData: false
    //     // },
    //     // {
    //     //     title: "Basic waterfall chart",
    //     //     input: "set5",
    //     //     chartType: "waterfall",
    //     //     defaultXVariables: ["label"],
    //     //     defaultY1Variables: ["change"],
    //     //     defaultY2Variables: [],
    //     //     defaultZVariables: ["isIntermediateSum"],
    //     //     defaultSegmentationVariables: [],
    //     //     tooltipIsShared: false,
    //     //     showMarkers: true,
    //     //     useLineStyles: true,
    //     //     showDataLabels: true,
    //     //     showLegend: true,
    //     //             isTracData: false
    //     //
    //     // },
    //     // {
    //     //     title: "Basic circle doughnut  with date as y-axis",
    //     //     input: "set1",
    //     //     chartType: "circleDoughnut",
    //     //     defaultXVariables: ["OBSERVATION_DATE"],
    //     //     defaultY1Variables: ["DRAWN_BALANCE"],
    //     //     defaultY2Variables: [],
    //     //     defaultZVariables: [],
    //     //     defaultSegmentationVariables: [],
    //     //     tooltipIsShared: false,
    //     //     showMarkers: true,
    //     //     useLineStyles: true,
    //     //             isTracData: false
    //     // },
    //     // {
    //     //     title: "Basic semi doughnut with date as y-axis",
    //     //     input: "set1",
    //     //     chartType: "semiDoughnut",
    //     //     defaultXVariables: ["OBSERVATION_DATE"],
    //     //     defaultY1Variables: ["DRAWN_BALANCE"],
    //     //     defaultY2Variables: [],
    //     //     defaultZVariables: [],
    //     //     defaultSegmentationVariables: [],
    //     //     tooltipIsShared: false,
    //     //     showMarkers: true,
    //     //     useLineStyles: true,
    //     //             isTracData: false
    //     // },
    //     // {
    //     //     title: "Basic sankey chart",
    //     //     input: "set4",
    //     //     chartType: "sankey",
    //     //     defaultXVariables: ["organiser"],
    //     //     defaultY1Variables: ["recipient"],
    //     //     defaultY2Variables: [],
    //     //     defaultZVariables: ["awards"],
    //     //     defaultSegmentationVariables: [],
    //     //     tooltipIsShared: false,
    //     //     showMarkers: true,
    //     //     useLineStyles: true,
    //     //     showDataLabels: false,
    //     //     showLegend: true,
    //     //             isTracData: false
    //     // },
    //     // {
    //     //     title: "Basic dependency wheel chart",
    //     //     input: "set4",
    //     //     chartType: "dependencywheel",
    //     //     defaultXVariables: ["organiser"],
    //     //     defaultY1Variables: ["recipient"],
    //     //     defaultY2Variables: [],
    //     //     defaultZVariables: ["awards"],
    //     //     defaultSegmentationVariables: [],
    //     //     tooltipIsShared: false,
    //     //     showMarkers: true,
    //     //     useLineStyles: true,
    //     //     showDataLabels: false,
    //     //     showLegend: true,
    //     //             isTracData: false
    //     //
    //     // }
    // ]
}

export const examplesStore = createSlice({
    name: 'examplesStore',
    initialState: initialState,
    reducers: {}
})

// Action creators are generated for each case reducer function
export const {} = examplesStore.actions

export default examplesStore.reducer