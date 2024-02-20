import {AsString, ColourOptions, DateFormat, DatetimeFormat, Multiplier, Option, Position, ThemesList} from "../types/types_general";

/* GeneralChartConfig config */
const General: {

    charts: {
        exportFileTypes: ("svg" | "png")[]
        numberOfRowsForWorkers: number
        numberOfRowsToSampleDownTo: number
        boostThreshold: number
    },
    publicPath: string,
    environments: {
        production: string | null
        preProduction: string | null
        test: string | null
        dev: string | null
        sandbox: string | null
    },
    numberOfDataDownloadRows: number
    files: {
        importFileTypes: ("csv" | "xlsx" | "pdf" | "pptx" | "docx")[],
    },
    dates: {
        /**
         * Some clients like to store dates as 2022/12/01 for Dec 2022 and some would like 2022/12/31.
         * In places where a date needs to be set by the UI there is a config for which approach is taken.
         */
        endOrStartOfPeriod: "start" | "end"
    },
    tables: {
        exportFileTypes: ("csv" | "xlsx")[]
        importFileTypes: ("csv" | "xlsx")[]
        numberOfRowsForVirtualization: number
        numberOfRowsForFunctionalityRestriction: number
        truncateTextLimit: number
        displayTableLimit: number
    }
    defaultLanguage: string
    defaultTheme: ThemesList
    defaultFormats: {
        integer: string
        float: string
        decimal: string
        date: DateFormat
        datetime: DatetimeFormat
    },
    loading: {
        allowCopies: { model: boolean, data: boolean, flow: boolean, schema: boolean }
    },
    options: {
        themeOptions: Option<ThemesList>[],
        languageOptions: Option<string>[]
    }
    // When a model is loaded into TRAC the language type is set as one of its attributes. However, when
    // we want to create a link to the file in GitHub say we need to know the extension to use e.g. a
    // python file has the extension '.py'. This object is a lookup between the two.
    languageExtensions: Record<string, string>

} = {
    // The path that the app is served from e.g. for locally http://localhost:8080/demo it would be "demo". This
    // must match the TRAC gateway service config routes
    publicPath: "/app/",
    charts: {
        exportFileTypes: ["svg", "png"],
        // If a dataset has more than this number of rows then the processing of the data for the chart is
        // moved to worker threads so the browser does not become non-responsive.
        numberOfRowsForWorkers: 1000,
        // Turn on the boost plugin if the number of rows in a series after filtering gets bigger than this. There is some
        // loss of visuals and features in lieu of performance.
        // 0: off, 1: always on > 1 is a threshold of when to apply it.
        boostThreshold: 10000,
        // No chart can ever have more than this number of points, if after filter a series has more than this number
        // of points it is sampled down to this number.
        numberOfRowsToSampleDownTo: 100000
    },
    dates: {
        endOrStartOfPeriod: "end"
    },
    environments: {
        production: null,
        preProduction: null,
        test: null,
        dev: null,
        sandbox: null
    },
    numberOfDataDownloadRows: 2000,
    tables: {
        exportFileTypes: ["csv"],
        importFileTypes: ["csv", "xlsx"],
        numberOfRowsForVirtualization: 500,
        numberOfRowsForFunctionalityRestriction: 10000,
        /**
         * String values greater than this length with be truncated, the user can click to show the full text.
         */
        truncateTextLimit: 250,
        /**
         * The maximum number of rows that can be shown in a table.
         */
        displayTableLimit: 100000
    },
    files: {
        importFileTypes: ["csv", "xlsx", "pdf", "pptx", "docx"],
    },
    defaultLanguage: "en-gb",
    defaultTheme: "lightTheme",
    defaultFormats: {
        integer: ",|.|0|||1",
        float: ",|.|2|||1",
        decimal: ",|.|2|||1",
        date: "DAY",
        datetime: "DATETIME"
    },
    loading: {
        allowCopies: {
            model: false,
            data: false,
            flow: false,
            schema: false,
        }
    },
    options: {
        themeOptions: [
            {value: "lightTheme", label: "Light"},
            // {value: "darkTheme", label: "Dark"},
            // {value: "clientTheme", label: "Client", disabled: true}
        ],
        languageOptions:
            [
                {value: "en-gb", label: "English"}
            ]
    },
    languageExtensions: {javascript: "js", python: "py"}
}

/**
 * https://date-fns.org/v2.25.0/docs/format
 * @type {{datetime: string, week: string, filename: string, month: string, year: string, half_year: string, time: string, day: string, quarter: string}}
 */
const DateFormats: { [key in DateFormat | DatetimeFormat]: string } = {
    DAY: "do MMMM yyyy",
    WEEK: "'Week' I yyyy",
    MONTH: "MMM yyyy",
    QUARTER: "QQQ yyyy",
    // dateFns does not have a native half year format, so we create one from the quarterly format. First set it to
    // a quarterly one then this is converted in the formatDateObject util.
    HALF_YEAR: "QQQ yyyy",
    YEAR: "yyyy",
    ISO: "yyyy-MM-dd",
    TIME: "HH:mm",
    DATETIME: "do MMM yyyy HH:mm:ss",
    FILENAME: "yyyy_MM_dd_HHmmss"
}

const Units: Option<string, Position>[] =
    [
        {value: "", label: "None", disabled: false, details: {position: "both"}},
        {value: "kr", label: "kr", disabled: false, details: {position: "pre"}},
        {value: "£", label: "£", disabled: false, details: {position: "pre"}},
        {value: "$", label: "$", disabled: false, details: {position: "pre"}},
        {value: "€", label: "€", disabled: false, details: {position: "pre"}},
        {value: "%", label: "%", disabled: false, details: {position: "post"}},
        {value: "bp", label: "bp", disabled: false, details: {position: "post"}}
    ]

const DecimalPlaces: Option<number>[] = [
    {value: 0, label: "0", disabled: false},
    {value: 1, label: "1", disabled: false},
    {value: 2, label: "2", disabled: false},
    {value: 3, label: "3", disabled: false},
    {value: 4, label: "4", disabled: false},
    {value: 5, label: "5", disabled: false}
]

/**
 * Options for the string to add at the end of a number in a format to signify the size
 * of the number e.g. "5k". The multiplier is used in the {@link Chart} component to
 * scale down large values to be readily shown on the axis 10,000,000 -> 10m,
 */
const Sizes: Option<string, Multiplier>[] = [
    {value: "", label: "None", disabled: false, details: {multiplier: 1}},
    {value: "k", label: "thousand", disabled: false, details: {multiplier: 1000}},
    {value: "m", label: "million", disabled: false, details: {multiplier: 1000000}},
    {value: "bn", label: "billion", disabled: false, details: {multiplier: 1000000000}},
    {value: "tr", label: "trillion", disabled: false, details: {multiplier: 1000000000000}}
]

const ThousandsSeparators: Option<string>[] = [
    {value: "", label: "None", disabled: false},
    {value: ",", label: ", (comma)", disabled: false},
    {value: ".", label: ". (decimal)", disabled: false}
]

const BooleanOptions: Option<null | boolean, AsString>[] = [
    {value: null, label: "None", disabled: false, details: {asString: null}},
    {value: true, label: "True", disabled: false, details: {asString: "TRUE"}},
    {value: false, label: "False", disabled: false, details: {asString: "FALSE"}}
]

const MultiplierOptions: Option<number>[] = [
    {value: 1000000, label: "1,000,000", disabled: false},
    {value: 1000, label: "1,000", disabled: false},
    {value: 100, label: "100", disabled: false},
    {value: 10, label: "10", disabled: false},
    {value: 1, label: "1", disabled: false},
    {value: 0.1, label: "0.1", disabled: false},
    {value: 0.01, label: "0.01", disabled: false},
    {value: 0.001, label: "0.001", disabled: false},
    {value: 0.000001, label: "0.000001", disabled: false}
]

/**
 * These colours are used to colour table cells when it is in traffic light or heatmap mode.
 */
const TableColumnColours: Record<ColourOptions, string> = {

    lowHeatmapColour: "#f7f7f7",
    highHeatmapColour: "#EF241C",
    lowTrafficLightColour: "#EF241C",
    highTrafficLightColour: "#198754",
    transitionTrafficLightColour: "#ffffff",
}

export {General, DateFormats, Units, Sizes, ThousandsSeparators, DecimalPlaces, BooleanOptions, TableColumnColours, MultiplierOptions}

