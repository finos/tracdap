/**
 * A component that shows a table with a menu that allows various options to be set.
 *
 * @module Table
 * @category Component
 */

import type {ActionCreatorWithPayload, AsyncThunk} from "@reduxjs/toolkit";
import {arraysOfPrimitiveValuesEqual, arraysOfPrimitiveValuesEqualInOrder, getMinAndMaxValueFromArrayOfObjects, sortArrayBy} from "../../utils/utils_arrays";
import {Button} from "../Button";
import type {ButtonPayload, ColumnColourDefinitions, FullTableState, Option, TableRow, TableValues, TracUiTableState, Variants} from "../../../types/types_general";
import {convertHexToRgb, getColumnColourTransitionValue, getYiq} from "../../utils/utils_general";
import {Cell} from "./Cell";
import {
    type ColumnDef,
    createColumnHelper,
    type FilterFn,
    flexRender,
    getCoreRowModel,
    getFacetedMinMaxValues,
    getFacetedRowModel,
    getFacetedUniqueValues,
    getFilteredRowModel,
    getPaginationRowModel,
    getSortedRowModel,
    type InitialTableState,
    type OnChangeFn,
    type Row as RowType,
    type RowData,
    type RowSelectionState,
    type SortingState,
    type Updater,
    useReactTable,
    type VisibilityState
} from "@tanstack/react-table";
import {ColumnFilters} from "./ColumnFilters";
import {downloadDataAsCsvFromJson, downloadDataAsCsvFromStream} from "../../utils/utils_downloads";
import {downloadDataAsXlsxSimple} from "../../utils/utils_excel";
import {extractValueFromTracValueObject} from "../../utils/utils_trac_metadata";
import {Footer} from "./Footer";
import {General, TableColumnColours} from "../../../config/config_general";
import {Header} from "./Header";
import {Icon} from "../Icon";
import {isDateObject, isDefined, isOption, isTracBoolean, isTracDateOrDatetime, isTracNumber, isTracString} from "../../utils/utils_trac_type_chckers";
import parseISO from "date-fns/parseISO";
import PropTypes from "prop-types";
import React, {useCallback, useEffect, useMemo, useRef, useState} from "react";
import {type RootState} from "../../../storeController";
import {Row} from "./Row";
import {type SingleValue} from "react-select";
import {Toolbar} from "./Toolbar";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useVirtualizer} from "@tanstack/react-virtual";
import {useAppDispatch, useAppSelector} from "../../../types/types_hooks";
import {getUniqueKeyFromObject, objectsEqual} from "../../utils/utils_object";
import {setAnyFormat} from "../../utils/utils_formats";

/**
 * An interface that extends the column meta property type to include the schema object that we add as
 * well as whether a string column should be treated as an array of strings (which will show it inside a badge).
 */
declare module '@tanstack/react-table' {
    interface ColumnMeta<TData extends RowData, TValue> {
        schema: trac.metadata.IFieldSchema,
        treatAsArray: boolean
    }
}

/**
 * An interface that extends the FilterFns available to the table to use to filter columns, in this case
 * we are registering a bespoke filter for use on date and datetime columns as well as strings which can include
 * nulls, categorical and array types.
 */
declare module '@tanstack/react-table' {
    interface FilterFns {
        customBooleanFilter: FilterFn<TableRow>
        customDateFilter: FilterFn<TableRow>
        customStringFilter: FilterFn<TableRow>
    }
}

// An object that is used as a helper (it checks what we do) when building the column structure
// See https://tanstack.com/table/v8/docs/guide/column-defs#column-helpers
const columnHelper = createColumnHelper<TableRow>()

/**
 * An object that defines some default props to apply to all columns in the table. This means we don't have to define
 * these parameters on each column.
 */
const defaultColumn: Partial<ColumnDef<TableRow, any>> = {

    // multi sort is the ability to sort by two or more columns at the same time
    enableMultiSort: true,
    enableGlobalFilter: true,
    enableColumnFilter: true
}

/**
 * An object that sets styles for the header 'th' elements, we define this out of the component so that it does not
 * trigger a rerender.
 */
const headerStyle = {
    // Min width is needed to allow the column filtering widgets to show property
    // This is so the placeholder label in the widgets show property, otherwise they get clipped
    minWidth: "120px",
}

/**
 * A function that returns the class to align the text in a cell based on type.
 * @param fieldType {integer} The TRAC type to get the class for.
 */
const getTextAlignment = (fieldType: trac.BasicType | undefined | null) => {

    return isTracNumber(fieldType) || isTracDateOrDatetime(fieldType) ? "text-end" : isTracBoolean(fieldType) ? "text-center" : "text-start"
}

/**
 * A function that returns the class to align the div in a cell based on type.
 * @param fieldType {integer} The TRAC type to get the class for.
 */
const getDivAlignment = (fieldType: trac.BasicType | undefined | null) => {

    return isTracNumber(fieldType) || isTracDateOrDatetime(fieldType) ? "float-end" : isTracBoolean(fieldType) ? "float-center" : "float-start"
}

/**
 * A function that runs when the user has column filters showing, and they filter a date or datetime column. By default,
 * there is no native react-table filter function for dates so this custom version is needed.
 * @param row - The row context object from the table.
 * @param columnId - The ID of the date or datetime column.
 * @param filterValue {[string, string]} The minimum and maximum dates to use for the filter as ISO strings.
 */
const customDateFilter: FilterFn<any> = (row: RowType<TableRow>, columnId: string, filterValue: [string, string]): boolean => {

    // The filter value stored for the date columns mirrors how react-table handles number columns internally by default.
    // In this case the filter is an array of two dates
    let [minDate, maxDate] = filterValue

    // Get the value in the row for the column we are filtering.
    const rowValue = row.getValue<string | null>(columnId)

    // Convert the ISO string date to a Javascript date object, handle null values too
    const rowValueAsDateObject = rowValue != null ? parseISO(rowValue) : null

    // Should we show the row, true here means keep it visible...

    // Don't filter anything if there are no filter dates set
    if (!minDate && !maxDate) {
        return false
    } else if (rowValueAsDateObject !== null && minDate && !maxDate) {
        return rowValueAsDateObject >= parseISO(minDate)
    } else if (rowValueAsDateObject !== null && !minDate && maxDate) {
        return rowValueAsDateObject <= parseISO(maxDate)
    } else if (rowValueAsDateObject !== null && minDate && maxDate) {
        return rowValueAsDateObject >= parseISO(minDate) && rowValueAsDateObject <= parseISO(maxDate)
    } else {
        // This should be for when there is a filter set but the value is null, we remove it
        return true
    }
}

/**
 * A function that runs when the user has column filters showing, and they filter a boolean column. By default,
 * there is no native react-table filter function for dates so this custom version is needed.
 * @param row - The row context object from the table.
 * @param columnId - The ID of the date or datetime column.
 * @param filterValue - The boolean value to filter.
 */
const customBooleanFilter: FilterFn<any> = (row: RowType<TableRow>, columnId: string, filterValue: SingleValue<Option<boolean>>): boolean => {

    // Get the value in the row for the column we are filtering.
    const rowValue = row.getValue<boolean | null>(columnId)

    // If no option is set then do not filter
    if (filterValue === null) {
        return true
    } else {
        // Passing an option to the filter allows us to have separate logic for null values in the dataset
        // We can't do this with values as if something is null is that no filter or find me the empty values.
        return rowValue === filterValue.value
    }
}

/**
 * A function that sets up the definitions for heatmap and traffic light columns, which columns to use is defined in
 * the dataset's attributes as an array of column names. This gets the minimum and maximum values in the column for
 * heat maps as well as an estimate for the transition value for traffic lights and returns an object that is stored
 * in the local component state (not the state of the react-table plugin). The colour definitions can be edited in the
 * ColumnColoursModal component and are used by the Row component to set the values.
 *
 * @param data - The data for the table.
 * @param schema - The schema for the dataset.
 * @param tag - The TRAC tag for the dataset that contains the heatmap and traffic light attributes.
 */
const setInitialHeatmapAndTrafficLightDefinitions = (data: TableRow[], schema: trac.metadata.IFieldSchema[], tag: undefined | trac.metadata.ITag): ColumnColourDefinitions => {

    // Which columns should have heatmap colours applied, they get the default colours and transition value
    // applied as these are not specified in an attribute
    const heatmapColumns = extractValueFromTracValueObject(tag?.attrs?.heatmap_columns).value
    // Which columns should have traffic light colours applied, they get the default colours and transition value
    // applied as these are not specified in an attribute
    const trafficLightColumns = extractValueFromTracValueObject(tag?.attrs?.trafficlight_columns).value

    // The attributes that define the heat map and traffic light columns needs to be an array. If someone changes the
    // definitions we check that it is the right type rather than error
    if (!Array.isArray(trafficLightColumns) || !Array.isArray(heatmapColumns)) return {}

    // The object that we are going to return containing all the details about which columns are
    // heatmap or traffic light columns and what the parameters are
    let tempColumnColours: ColumnColourDefinitions = {}

    // A list of all the columns in the table
    const columnNames: string[] = schema.map(variable => variable.fieldName).filter(isDefined)

    let uniqueHeatmapAndTrafficLightColumns = [...new Set([...trafficLightColumns, ...heatmapColumns])]

    uniqueHeatmapAndTrafficLightColumns.forEach(column => {

        if (typeof column === "string" && columnNames.includes(column)) {

            // Get the fieldType from the schema, string variables can not be heatmap or traffic light columns
            const fieldType = schema.find(variable => variable.fieldName === column)?.fieldType || trac.BasicType.BASIC_TYPE_NOT_SET

            const type = heatmapColumns.includes(column) ? "heatmap" : "trafficlight"

            // Set the colour based on the colour mapping method
            const lowColour = heatmapColumns.includes(column) ? TableColumnColours.lowHeatmapColour : TableColumnColours.lowTrafficLightColour
            const highColour = heatmapColumns.includes(column) ? TableColumnColours.highHeatmapColour : TableColumnColours.highTrafficLightColour

            // Get the hexadecimal colours as rgb
            const lowColourAsRgb = convertHexToRgb(lowColour)
            const highColourAsRgb = convertHexToRgb(highColour)
            const transitionColourAsRgb = convertHexToRgb(TableColumnColours.transitionTrafficLightColour)

            // Add the required object
            tempColumnColours[column] = {
                type,
                basicType: fieldType,
                // Min and max are needed for heatmaps to set a cells relative position and colour
                minimumValue: undefined,
                maximumValue: undefined,
                // The transition value is used in traffic lights as the boundary between the two colours
                transitionValue: null,
                // Note that the next two lines mean that if a column it in both lists then heat maps take precedence
                lowColour,
                highColour,
                transitionColour: TableColumnColours.transitionTrafficLightColour,
                // Calculate the colours for the text, using YIQ to decide which will give the higher contrast
                lowTextColour: type === "trafficlight" ? getYiq(lowColourAsRgb.r, lowColourAsRgb.g, lowColourAsRgb.b) >= 128 ? 'black' : 'white' : undefined,
                highTextColour: type === "trafficlight" ? getYiq(highColourAsRgb.r, highColourAsRgb.g, highColourAsRgb.b) >= 128 ? 'black' : 'white' : undefined,
                transitionTextColour: getYiq(transitionColourAsRgb.r, transitionColourAsRgb.g, transitionColourAsRgb.b) >= 128 ? 'black' : 'white'
            }

            // Calculate the minimum and maximum values for the heat map, this is expensive, so we
            // always retain the calculated values in the colour definition state, this is so if the user turns it on
            // then off and then on again then we don't have to recalculate the min and max. Instead the values from the
            // first calculation will be available.
            const {minimum, maximum} = getMinAndMaxValueFromArrayOfObjects(data, column)
            tempColumnColours[column].minimumValue = isDateObject(minimum) ? undefined : minimum
            tempColumnColours[column].maximumValue = isDateObject(maximum) ? undefined : maximum
            // We set the transition value for heatmaps, although it is not needed, this is so if the user swaps
            // between a heatmap to a traffic light for a specific column the value is already set.
            tempColumnColours[column].transitionValue = getColumnColourTransitionValue(tempColumnColours[column].minimumValue, tempColumnColours[column].maximumValue, fieldType)
        }
    })

    return tempColumnColours
}

/**
 * A function that sets up the definitions for which columns are not visible by using an attribute in the dataset's tag.
 * This object is put into the react-table plugin's initial state which manages it internally.
 * @param schema - The schema for the dataset.
 * @param tag - The TRAC tag for the dataset that contains the heatmap and traffic light attributes.
 */
const setInitialHiddenColumn = (schema: trac.metadata.IFieldSchema[], tag: undefined | trac.metadata.ITag): VisibilityState => {

    // Which columns are hidden by default
    const hiddenColumns = extractValueFromTracValueObject(tag?.attrs?.hidden_columns).value

    // The attribute that defines the hidden columns needs to be an array. If someone changes the
    // definition we check that it is the right type rather than error
    if (!Array.isArray(hiddenColumns)) return {}

    // The object that we are going to return containing all the details about which columns are
    // heatmap or traffic light columns and what the parameters are
    let tempHiddenColumns: VisibilityState = {}

    // A list of all the columns in the table
    const columnNames: string[] = schema.map(variable => variable.fieldName).filter(isDefined)

    hiddenColumns.forEach(column => {

        if (typeof column === "string" && columnNames.includes(column)) {

            // The visibility is set to false, if absent visibility is true
            tempHiddenColumns[column] = false
        }
    })

    return tempHiddenColumns
}

/**
 * A function that runs when the component mounts and sets the initial state for the table component. Note that this is
 * the internal state for this component and not the react-table plugin state, there is a separate object created for
 * the initial state of the plugin.
 */
const setInitialTracUiState = (props: Props): TracUiTableState => {

    const {initialHeight = "65vh", initialPaginate = false, initialShowGlobalFilter = true, initialShowInformation = true} = props

    // Note that we set the individual state using individual props and then if a saved version of the state is
    // passed in we merge this on top. This means that we are able to save the user set view of the component in a
    // store and then restore that view if they move away and then come back to the table.
    return ({
        ...{
            // When true the global filter box shows in the toolbar
            showGlobalFilter: initialShowGlobalFilter,
            // The height of the table either in pixels or the percent of the vertical height of the screen
            height: initialHeight,
            // When true the footer shows row and selection information
            showInformation: initialShowInformation,
            // Whether to paginate the table, if false the table has a fixed height and is scrollable
            paginate: initialPaginate,
            // Definitions for which columns are heat maps and traffic lights.
            // The user is able to set columns as heatmap or traffic light columns, so they are coloured according to their values.
            // The columns to include can be defined in tag attributes that are passed to the
            // setInitialHeatmapAndTrafficLightDefinitions function to create the required definitions.
            columnColours: setInitialHeatmapAndTrafficLightDefinitions(props.data, props.schema, props.tag),
            // When true a row of widgets shows that allows column filters to be changed, this is not retained across
            // the table re-mounting
            showColumnFilters: false,
            // When true a row of widgets shows that allows the column order to be changed, this is not retained across
            // the table re-mounting
            showColumnOrder: false,
            // The heights of some rows in the table, used to make the sticky classes work. The three
            // rows are from the three refs below
            rowHeights: {head: 0, columnOrder: 0, columnFilter: 0}

        }, ...props.savedTableState?.tracUiState || {}
    })
};

/**
 * A function that runs when the component mounts and sets the initial state for the react-table component. Note that
 * this is the internal state for the react-table plugin and not this component, there is a separate object
 * created for the initial state of this component.
 */
const setInitialReactTableState = (props: Props): InitialTableState => {

    const {
        data,
        initialPageSize = 10,
        initialPaginate = false,
        initialSort = defaultObjectProps.initialSort,
        schema
    } = props

    // Note that we set the individual state using individual props and then if a saved version of the state is
    // passed in we merge this on top. This means that we are able to save the user set view of the component in a
    // store and then restore that view if they move away and then come back to the table.
    return ({
        ...{
            columnFilters: [],
            columnOrder: schema.map(variable => variable.fieldName).filter(isDefined),
            columnVisibility: setInitialHiddenColumn(schema, props.tag),
            sorting: data.length > General.tables.numberOfRowsForFunctionalityRestriction ? [] : initialSort.filter(sort => schema.map(variable => variable.fieldName).includes(sort.id)),
            globalFilter: "",
            pagination: initialPaginate ? {pageSize: initialPageSize, pageIndex: 0} : undefined,
            rowSelection: {}

        }, ...props.savedTableState?.reactTableState || {}
    })
};

// TODO Editing
// TODO Table row selection logic
// TODO add colour selector
// TODO Add typescript checking
// TODO look again at editing

/**
 * An interface for the props of the Table component.
 */
export interface Props {

    /**
     * The columns that should be shown in a Badge component, array values will be shown as badges by default. This is
     * not a public attribute, this is used solely by the UI when needed.
     */
    badgeColumns?: string[]
    /**
     * A lookup for the values in the array of badgeColumns and the colour that they should be shown in. This means
     * that we can have red amber green badges based on their string value. If other colourings for specific values are
     * needed then these can be set via this prop.
     */
    badgeColours?: Record<string, Variants>
    /**
     * The css class to apply to the table, this allows additional styles to be added to the component.
     * @defaultValue = 'my-3'
     */
    className?: string
    /**
     *  The dataset to show. This is a TRAC dataset with the addition of string arrays.
     */
    data: TableRow[]
    /**
     * A component that is shown in the footer along with the pagination buttons. This is usually a set of buttons that
     * perform tasks such as load something related to the table. This component will be shown next to the pagination
     * buttons and the table information so that the layout is nice and neat.
     */
    footerComponent?: React.ReactElement<any, any> | React.ReactNode[]
    /**
     * A function that returns the user selected rows to the parent, this is used to update a store with the selection. It is an
     * async thunk in order to allow the function to have additional async logic such as getting data related to the selected rows
     * from TRAC. The Typescript interface is for a Redux AsyncThunk with the returned interface, the argument interface and the
     * store interfaces defined.
     */
    getSelectedRowsFunction?: AsyncThunk<void | { storeKey?: string, tags?: trac.metadata.ITag[], id?: string }, { storeKey?: string, selectedRows?: TableRow[], id?: string }, { state: RootState }>
    /**
     * A component that is shown in the header along with the toolbar buttons. This is usually a set of buttons that
     * perform tasks such as load something related to the table. This component will be shown on the left of the
     * global search and toolbar icon so that the layout is nice and neat.
     */
    headerComponent?: React.ReactElement<any, any> | React.ReactNode[]
    /**
     * An ID for the table, this is used when you are trying to save the state of the table or the selected rows, and
     * you need to know which table the information being sent back is for.
     */
    id?: string
    /**
     * A function that updates the data for the table, this is assumed to be in a Redux store so the function needs to
     * be dispatched. This is used when uploading data and editing data. When this is prop is used an "Import from file"
     * option will appear in the toolbar.
     */
    importDataFunction?: Function
    /**
     * The table height to use if not paginated, this will mean the table may become scrollable if the records do not
     * fit the height. TODO Make editable
     * @defaultValue '65vh'
     */
    initialHeight?: string
    /**
     * The number of rows to show per page if paginated.
     * @defaultValue 10
     */
    initialPageSize?: number
    /**
     * Whether to paginate the table.
     * @defaultValue false
     */
    initialPaginate?: boolean
    /**
     * Whether to show an input that allows a global search to be done on the table.
     * @defaultValue true
     */
    initialShowGlobalFilter?: boolean,
    /**
     * Whether to show information about the table data.
     * @defaultValue true
     */
    initialShowInformation?: boolean
    /**
     * The sort applied to the table on mounting. This is an array of the form
     * [{id: 'creationTime', desc: false}] which allows for multiple sorts.
     * @defaultValue []
     */
    initialSort?: SortingState
    /**
     * Whether the data is editable by the user. The importDataFunction is also needed to be able to saved changes to
     * the data.
     * @defaultValue false
     */
    isEditable?: boolean
    /**
     * Whether the data is actually a TRAC dataset with a real tag. If this component is used to show data that is not
     * from a real dataset (such as search results) then certain things like showing the metadata tag modal should be
     * turned off.
     */
    isTracData: boolean
    /**
     * The message to show if there is no data.
     * defaultValue 'There is no data for this table'
     */
    noDataMessage?: string
    /**
     * The number of rows that can be selected when clicked on.
     * defaultValue 0
     */
    noOfRowsSelectable?: number
    /**
     * String columns that are not allowed to word wrap. This is not a public attribute, this is used
     * solely by the UI when needed.
     * @defaultValue []
     */
    noWrapColumns?: string[]
    /**
     * The options for the page length, shown if pagination is being used.
     * @defaultValue [10, 50, 100, 200, 500, 1000]
     */
    pageSizeOptions?: number[]
    /**
     * The table state that has previously been saved to a store, this is used to set the initial state of the table, so
     * we can mount it and make it look like it did before the user moved to a different page.
     */
    savedTableState?: FullTableState
    /**
     * A function that runs when the table unmounts that saves the state of the table to a store, meaning that it can
     * be reloaded with the same state.
     */
    saveTableStateFunction?: ActionCreatorWithPayload<Pick<Props, "storeKey" | "id"> & { tableState: FullTableState }>
    /**
     * The schema for the dataset.
     */
    schema: trac.metadata.IFieldSchema[]
    /**
     * The data for the selected rows. This is needed in case you need to change the rows that the table shows as
     * selected from outside the component. For example, the react-table state stores the rows selected by their
     * row number e.g. {"3": true}, if the data refreshes the table will always show the fourth row as selected
     * unless you have the ability to update that
     */
    selectedRows?: TableRow[]
    /**
     * Whether to allow the data to be exported to csv or excel.
     * @defaultValue true
     */
    showExportButtons?: boolean
    /**
     * Whether to show the header and the toolbar.
     * @defaultValue true
     */
    showHeader?: boolean
    /**
     * The key in a store to use to save the data to/get the data from.
     */
    storeKey?: string
    /**
     * The TRAC metadata for the data. Note that the TRAC tag for the dataset is used to extract attributes that
     * impact on how the table is viewed. These are not separate props. For example if the tag includes a
     * "hidden_columns" attribute the list of values will be used to set column visibility.
     */
    tag?: trac.metadata.ITag
    /**
     * An array of the field names in the data that when concatenated together as a string form a unique row identifier,
     * this is used when trying to set the selected rows from outside this component.
     */
    uniqueRowIdentifier?: string[]
}

/**
 * These are default prop values, but we don't put them directly into the destructuring inside the
 * component as that would cause re-rendering.
 */
const defaultObjectProps: Required<Pick<Props, "initialSort" | "noWrapColumns" | "pageSizeOptions">> = {

    initialSort: [],
    noWrapColumns: [],
    pageSizeOptions: [10, 50, 100, 200, 500, 1000]
};

export const Table = (props: Props) => {

    console.log("Rendering Table")

    const {
        badgeColumns,
        badgeColours,
        className = "my-3",
        data,
        footerComponent,
        getSelectedRowsFunction,
        headerComponent,
        id,
        importDataFunction,
        initialSort = defaultObjectProps.initialSort,
        isEditable = false,
        isTracData,
        noDataMessage = "There is no data for this table",
        noOfRowsSelectable = 0,
        noWrapColumns = defaultObjectProps.noWrapColumns,
        pageSizeOptions = defaultObjectProps.pageSizeOptions,
        saveTableStateFunction,
        schema,
        selectedRows,
        showExportButtons = true,
        showHeader = true,
        storeKey,
        tag,
        uniqueRowIdentifier
    } = props

    // Some config that defines when we move to virtualized tables and when we will restrict
    // table functionality based on the size of the data. Since we know that the larger a dataset is the
    // worse in browser performance will be, and we don't know what the laptop spec the application is running on we
    // set limits to reduce the impact of this that we can tune in the config.
    const {numberOfRowsForFunctionalityRestriction, numberOfRowsForVirtualization, truncateTextLimit} = General.tables

    const restrictedFunctionality = Boolean(data.length > numberOfRowsForFunctionalityRestriction)

    // Get what we need from the store, used for setting the download name
    const {userId, userName} = useAppSelector(state => state["applicationStore"].login)
    const {"trac-tenant": tenant} = useAppSelector(state => state["applicationStore"].cookies)

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Which columns are editable by default, table needs to be set as editable for this to do anything
    // TODO enable per cell editing
    //const editableColumnsFromAttr = useMemo(() => extractValueFromTracValueObject(tag?.attrs?.editable_columns).value, [tag])

    // The state of the table excluding the state used internally by the react-table plugin
    const [tracUiState, setTracUiState] = useState<TracUiTableState>(setInitialTracUiState(props))

    // In order to make the widgets rows sticky at the top of the table when it is not paginated, we need to know
    // the height of the table header row, the column ordering widget row and the column filter widget row.
    const tableHeaderRow = useRef<HTMLTableRowElement | null>(null)
    const tableColumnOrderingRow = useRef<HTMLTableRowElement | null>(null)
    const tableColumnFilterRow = useRef<HTMLTableRowElement | null>(null)

    // This ref is the div wrapper to the table, this is used when we need to virtualize the table. The react-virtual
    // plugin attaches to the ref to help manage the rendering. Note that when the ref is set on the div then the
    // virtualization will be 'on' irrespective of if we actually want it to be. So you can see that the ref is
    // actually passed as an optional prop to the div.
    const tableContainerRef = useRef<HTMLDivElement>(null)

    /**
     * A hook that runs after the page has loaded and that stores the height of some table rows in
     * the state. This is needed to be able to set the widgets row in the table as sticky using this offset.
     */
    useEffect(() => {

        if (tracUiState.showColumnOrder || tracUiState.showColumnFilters) {

            const head = tableHeaderRow.current ? Math.round(tableHeaderRow.current.clientHeight) : 0
            const columnOrder = tableColumnOrderingRow.current ? Math.round(tableColumnOrderingRow.current.clientHeight) : 0
            const columnFilter = tableColumnFilterRow.current ? Math.round(tableColumnFilterRow.current.clientHeight) : 0

            if (tracUiState.rowHeights.head !== head || tracUiState.rowHeights.columnOrder !== columnOrder || tracUiState.rowHeights.columnFilter !== columnFilter) {

                console.log("LOG :: Setting widgets row height")
                setTracUiState(prevState => (
                    {...prevState, ...{rowHeights: {head, columnOrder, columnFilter}}}
                ))
            }
        }

    }, [tracUiState.rowHeights.columnFilter, tracUiState.rowHeights.columnOrder, tracUiState.rowHeights.head, tracUiState.showColumnFilters, tracUiState.showColumnOrder])

    // An array that contains the definition of all the columns in the table
    const columns: ColumnDef<TableRow, TableValues>[] = useMemo(() =>

        // The sort by function means that we always show the table columns in the field order defined in the schema
        sortArrayBy(schema, "fieldOrder").map((variable) => {

            if (variable.fieldName == null) throw new Error(`Variable ${variable.fieldName} does not have a fieldType set, this is not allowed`)

            // The array contains the returned value from the columnHelper.accessor function

            return columnHelper.accessor(variable.fieldName, {

                // The ID for the column
                id: variable.fieldName,
                // The label for the column
                header: variable.label || variable.fieldName,
                // The cell render function
                cell: info => <Cell info={info} truncateTextLimit={truncateTextLimit} badgeColours={badgeColours}/>,
                // We attach the schema to the metadata for the column definition, meaning we can access it when
                // provided the column definition in other functions
                meta: {
                    schema: variable,
                    treatAsArray: Boolean(badgeColumns && badgeColumns.includes(variable.fieldName))
                },
                // The default filters used by react-table for boolean, and numbers are all fine but a custom
                // filter for date and datetime columns is needed as otherwise these will be treated as strings.
                // We also need a custom filter for strings which can be categorical or not and also can include
                // arrays of strings
                filterFn: isTracDateOrDatetime(variable.fieldType) ? 'customDateFilter' : isTracNumber(variable.fieldType) ? "inNumberRange" : isTracString(variable.fieldType) ? "customStringFilter" : isTracBoolean(variable.fieldType) ? "customBooleanFilter" : undefined
            })

        }), [badgeColours, badgeColumns, schema, truncateTextLimit])

    /**
     * A function that runs when the user has the global filter showing, and they change the value. This replaces the
     * default react table functions as they don't cope with different data types. Note that the
     * getColumnCanGlobalFilter function passed to the table is used to determine which columns are searchable.
     * @param row - The row to be checked for filtering.
     * @param columnId - The ID of the column to check for filtering.
     * @param filterValue {string|null} The filter value to check against.
     */
    const customGlobalFilterFunction = (row: RowType<TableRow>, columnId: string, filterValue: string): boolean => {

        // Do not filter anything if the filter value is not set
        if (filterValue === "") return true

        // So now we know that the column is visible and there is a filter value to apply, so we need to do a little
        // work. Get the column details, this contains the meta we stored in it and the column visibility
        const schema = table.getColumn(columnId)?.columnDef.meta?.schema

        // If there is no schema we error, this is a required prop
        if (!schema || !schema.fieldType) throw new Error(`Column ${columnId} does not have a schema or fieldType set, this is not allowed.`)

        // The table data types (in the TableValues interface) can be a data row from the TRAC data API or an array of
        // strings which the user interface uses specifically to list items in a table cell. The TRAC data API can not
        // return arrays. We get the value for the column and then format them, the format is needed so if the column
        // shows "JAN" for a formatted date that the user can search for "JAN".
        const value = row.getValue<TableValues>(columnId)
        const formattedValue = Array.isArray(value) ? value : setAnyFormat(schema?.fieldType, schema?.formatCode || null, value)

        // We already know filterValue is not null, so if value is null we filter it out
        if (formattedValue == null) {

            return false

        } else if (Array.isArray(formattedValue)) {

            return formattedValue.some(item => item.toLowerCase().includes(filterValue.toLowerCase()))

        } else {

            return formattedValue.toLowerCase().includes(filterValue.toLowerCase())
        }
    }

    /**
     * A function that runs when the user has column filters showing, and they filter a string column. There is a default
     * function available from the react-table component, but it can't handle nulls, also we have categorical strings
     * and arrays of strings, so we need to handle those.
     * @param row - The row context object from the table.
     * @param columnId - The ID of the date or datetime column.
     * @param filterValue - The string to filter on.
     */
    const customStringFilter: FilterFn<any> = (row: RowType<TableRow>, columnId: string, filterValue: null | string | Option<string>): boolean => {

        // Do not filter anything if the filter value is not set
        if (filterValue === null || filterValue === "") return true

        // Get the value in the row for the column we are filtering.
        const value = row.getValue<string[] | string | null>(columnId)

        // Get the column details, this contains the meta we stored in it and the column visibility
        const schema = table.getColumn(columnId)?.columnDef.meta?.schema

        // We already know filterValue is not null, so if value is null we filter it out
        if (value == null) {

            return false

        } else if (typeof value === "string" && typeof filterValue === "string" && !schema?.categorical) {

            // None categorical strings need to match any part of the filter string
            return value.toLowerCase().includes(filterValue.toLowerCase())

        } else if (schema?.categorical && isOption(filterValue) && filterValue.value === null) {

            // Categorical strings will be selected via an option dropdown, when the option is for null we take this as find the null values
            return true

        } else if (typeof value === "string" && schema?.categorical && isOption(filterValue)) {

            // Categorical strings will be selected via an option dropdown, the whole value must match
            return value.toLowerCase() === filterValue.value.toLowerCase()

        } else if (Array.isArray(value) && Array.isArray(filterValue)) {

            // Arrays must contain all the values of the filter value
            return filterValue.every(item => value.includes(item))

        } else {

            return false
        }
    }

    /**
     * A function that removes a column filter being applied to a column by removing its entry in the array of filter
     * values.
     * @param payload - The payload from the button used to clear the filter.
     */
    const handleClearColumnFilter = (payload: ButtonPayload) => {

        const newFilters = [...table.getState().columnFilters.filter(item => item.id !== payload.id)]

        table.setColumnFilters(newFilters)
    }

    /**
     * A function that initiates a download of the table data as a csv, this adds in the additional information
     * needed to set the file name.
     */
    const handleCsvExport = useCallback(() => {

        // For real TRAC datasets we get the full dataset from a streaming download, this is so if we have truncated the data in the
        // browser because it's very big then we still allow the user to download the whole dataset.
        tenant && isTracData && tag ? downloadDataAsCsvFromStream(tag, tenant, userId) : downloadDataAsCsvFromJson(data, tag, userId)

    }, [data, isTracData, tag, tenant, userId])

    /**
     * A function that initiates a download of the table data as an Excel file, this adds in the additional information
     * needed to set the file name.
     */
    const handleXlsxExport = useCallback(() => {

        downloadDataAsXlsxSimple(data, schema, tag, userId, userName, tenant)

    }, [data, schema, tag, tenant, userId, userName])

    /**
     * The row selection is stored in local state, rather than the state of the react-table plugin so that we can have some additional
     * logic to keep it in sync with the selected rows in the store holding information about the table.
     */
    const [rowSelection, setRowSelection] = useState<RowSelectionState>(props.savedTableState?.reactTableState?.rowSelection || {})

    /**
     * A function that handles additional logic for handling the selection of rows in the table. If the table gets new data (for example
     * if the data comes from a search API call) then the selected rows may not be available or have changed position. There might be
     * occasions where the store has the selected rows changed and that needs to be reflected here. This function handles that logic.
     * @param rowSelectionUpdater {Updater<RowSelectionState>} The function from the react-table plugin to use to update the row selection state.
     */
    const handleRowSelectionChange: OnChangeFn<RowSelectionState> = (rowSelectionUpdater: Updater<RowSelectionState>) => {

        if (typeof rowSelectionUpdater === "function") {

            const greg = rowSelectionUpdater(table.getState().rowSelection)
            setRowSelection(rowSelectionUpdater(table.getState().rowSelection))

            if (getSelectedRowsFunction) {

                console.log("LOG :: Saving selected rows in store")

                const selectedRows: TableRow[] = []

                Object.keys(greg).forEach(id => {
                    selectedRows.push(data[Number(id)])
                })

                dispatch(getSelectedRowsFunction({
                    storeKey,
                    id,
                    // Need to generate this as this is not updated yet
                    selectedRows
                }))
            }
        }
    }

    // This is the full table definition using the react-table plugin
    const table = useReactTable({

        // If there is no data show a custom table saying 'No data'
        data: data,
        // If there is no data show a custom table schema
        columns: columns,
        // Register additional custom filters for handling date/datetime and string column filters
        filterFns: {
            customBooleanFilter: customBooleanFilter,
            customDateFilter: customDateFilter,
            customStringFilter: customStringFilter
        },
        // The initial sorting set when the table mounts
        initialState: setInitialReactTableState(props),
        // The only state we have in this component rather than managed by the react-table plugin is
        // which rows are selected as this needs to be able to be updated from outside this Table component.
        state: {
            rowSelection
        },
        // Set default options for all columns using the defaultColumn object
        defaultColumn: defaultColumn,
        // Set whether the table is sortable using the isSortable prop
        enableSorting: !restrictedFunctionality && data.length > 0,
        // All tables with sorting enabled can be multi-sorted
        enableMultiSort: !restrictedFunctionality && data.length > 0,
        // Function to assess if the sorting action is a multi sort, it is always true as we always
        // allow multi sorting if isSortable is true
        isMultiSortEvent: () => true,
        // Set whether rows can be selected using the noOfRowsSelectable prop
        enableRowSelection: Boolean(noOfRowsSelectable > 0 && data.length > 0),
        // Set whether multiple rows can be selected using the noOfRowsSelectable prop
        enableMultiRowSelection: Boolean(noOfRowsSelectable > 1 && data.length > 0),
        // Set whether global filtering is possible
        enableGlobalFilter: !restrictedFunctionality && data.length > 0,
        // Whether a column can be included in global filtering is set based on visibility, columns named objectID are
        // always searchable meaning that someone can past an ID and always see it irrespective
        getColumnCanGlobalFilter: (column) => !restrictedFunctionality && (column.getIsVisible() || column.id === "objectId"),
        // The function to run to assess which rows to show when the global filter value is changes
        globalFilterFn: customGlobalFilterFunction,
        // Various functions used for the table functionality, you have to be a bit careful here as just but calling
        // the function here you set up that function to be used. e.g. getPaginationRowModel will render a page of
        // rows even though none of the other stuff is saying that the table should be paginated.
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: !restrictedFunctionality && data.length > 0 ? getSortedRowModel() : undefined,
        getFilteredRowModel: !restrictedFunctionality && data.length > 0 ? getFilteredRowModel() : undefined,
        getFacetedRowModel: !restrictedFunctionality && data.length > 0 ? getFacetedRowModel() : undefined,
        getFacetedUniqueValues: !restrictedFunctionality && data.length > 0 ? getFacetedUniqueValues() : undefined,
        getFacetedMinMaxValues: !restrictedFunctionality && data.length > 0 ? getFacetedMinMaxValues() : undefined,
        getPaginationRowModel: tracUiState.paginate && data.length > 0 ? getPaginationRowModel() : undefined,
        onRowSelectionChange: handleRowSelectionChange
    })

    // Get the rows from the table
    const {rows} = table.getRowModel()

    // This is a hook from the react-virtual plugin that allows us to virtualize the rows in the table, we only need
    // to do this if the number of rows is above the limit set in the config.
    const rowVirtualizer = useVirtualizer({
        count: rows.length,
        getScrollElement: () => !tracUiState.paginate && rows.length > numberOfRowsForVirtualization ? tableContainerRef.current : null,
        // 37px is a row in the table with one line of text per cell
        estimateSize: () => 38
    })

    // Get some details about the rows out of the react-virtual hook
    const totalSize = rowVirtualizer.getTotalSize()
    const virtualRows = rowVirtualizer.getVirtualItems()

    // The react-virtual hook requires some padding to be applied to the table, I think that this is to make the
    // scroll bar look right as you scroll down.
    const paddingTop = virtualRows.length > 0 ? virtualRows?.[0]?.start || 0 : 0

    const paddingBottom =
        virtualRows.length > 0
            ? totalSize - (virtualRows?.[virtualRows.length - 1]?.end || 0)
            : 0

    // Get the selected rows from the table
    const {rows: selectedRowsFromTable} = table.getSelectedRowModel()

    /**
     * A hook that checks that the selected rows in the store holding information about the table is in sync
     * with the selected rows in the state of the table. Usually this would not be a problem except when the
     * user can change the data. For example, if the user is showing search results from TRAC or swapping
     * between datasets then any selected rows might no longer be present or might be in a different row
     * to the index held in the react-table state. So we need to sync the two. Where is it straight forward
     * to we try and see if we can find the rows in the table after a change has been detected.
     */
    useEffect(() => {

        if (uniqueRowIdentifier && selectedRows && noOfRowsSelectable > 0 && getSelectedRowsFunction) {

            // What are the unique keys for the selected rows according to the Redux store
            const uniqueKeysForRowsInStore: string[] = selectedRows.map(row => getUniqueKeyFromObject(row, uniqueRowIdentifier))

            // What are the unique keys for the selected rows according to the table state
            const uniqueKeysForRowsInTable = selectedRowsFromTable.map(row => getUniqueKeyFromObject(row.original, uniqueRowIdentifier))

            // Are the store and the state in sync
            if (!arraysOfPrimitiveValuesEqual(uniqueKeysForRowsInStore, uniqueKeysForRowsInTable)) {

                console.log("LOG :: Selected rows in store and state are out of sync")

                let updateStoreToNull = false
                let updateStateToNull = false

                // If the store has been updated with zero selected rows then remove all selections from the table
                if (uniqueKeysForRowsInStore.length === 0) {

                    updateStateToNull = true

                } else if (noOfRowsSelectable === 1 && uniqueKeysForRowsInStore.length === 1) {
                    // If there is one selectable row but the store and state disagree then try to find the right row index

                    // Try and find the index of the row with the key that's in the store, it could have just moved position.
                    // findIndex stops after the first item has been found
                    const index = data.findIndex(row => getUniqueKeyFromObject(row, uniqueRowIdentifier) === uniqueKeysForRowsInStore[0])

                    // If the row is not in the table anymore, perhaps a new search was done, then remove selections from the store and the
                    // table state
                    if (index < 0) {

                        updateStateToNull = true
                        updateStoreToNull = true

                    } else {

                        // If the expected row was found in the table then update the table state
                        setRowSelection({[index]: true})
                    }

                } else if (noOfRowsSelectable > 1 && !restrictedFunctionality) {

                    // If there is more than one selectable row and the store and state disagree then try to find the right row indices. Only do this
                    // if the table is not huge and parsing the table may cause performance issues.

                    // Try and find the indices of the rows with the keys that's in the store, they could have just moved position.
                    // forEach parses the whole table so is slower
                    const indices: RowSelectionState = {}
                    const foundRows: TableRow[] = []

                    data.forEach((row, i) => {

                        const uniqueKeyForRow = getUniqueKeyFromObject(row, uniqueRowIdentifier)

                        if (uniqueKeysForRowsInStore.includes(uniqueKeyForRow)) {
                            indices[i] = true
                            foundRows.push(row)
                        }
                    })

                    // Update the store and the state with the new selections
                    setRowSelection(indices)

                    dispatch(getSelectedRowsFunction({
                        storeKey: storeKey,
                        id: id,
                        // Need to generate this as this is not updated yet
                        selectedRows: foundRows
                    }))

                } else if (noOfRowsSelectable > 1 && restrictedFunctionality) {

                    // If there is more than one selectable row and the store and state disagree then just reset the selections if the table is
                    // under functionality restrictions.

                    updateStateToNull = true
                    updateStoreToNull = true
                }

                // Perform the updates identified
                if (updateStateToNull) {

                    console.log("LOG :: Setting table state selected rows to none")
                    setRowSelection({})
                }
                if (updateStoreToNull) {

                    console.log("LOG :: Setting table store selected rows to none")
                    dispatch(getSelectedRowsFunction({
                        storeKey: storeKey,
                        id: id,
                        // Need to generate this as this is not updated yet
                        selectedRows: []
                    }))
                }
            }
        }

    }, [data, dispatch, getSelectedRowsFunction, id, noOfRowsSelectable, restrictedFunctionality, selectedRows, selectedRowsFromTable, storeKey, uniqueRowIdentifier])

    /**
     * Awesomeness. A hook that runs when the table unmounts and that sends the state of the table and the other
     * pieces of state held by the component to a Redux store. This allows the table to re-mounted with the same state
     * and the same view.
     */
    useEffect(() => {

        // TODO Check this vs other on unmount approaches
        return () => {
            if (saveTableStateFunction) {

                console.log("LOG :: Saving table state")

                dispatch(saveTableStateFunction({
                    storeKey: storeKey,
                    id: id,
                    tableState: {tracUiState, reactTableState: table.getState()}
                }))
            }
        }
    }, [dispatch, id, saveTableStateFunction, storeKey, table, tracUiState])

    /**
     * A function that runs when the column ordering widget is shown, and they change the ordering.
     * @param payload - The payload from the button click.
     */
    const handleOrderChange = useCallback((payload: ButtonPayload) => {

        const {id: variableToMove, name} = payload

        // Get the position of the edited variable in the columns, this is for all columns not just those visible
        const columnOrder = table.getAllLeafColumns().map(column => column.id)

        // The table provides the columns in their current order
        const oldColumnOrderIndex = columnOrder.findIndex(variable => variable === variableToMove)

        // Button id props can be numbers, so we need to check that we did indeed get a fieldName string
        if (oldColumnOrderIndex >= 0 && typeof variableToMove === "string") {

            // If moving up the list the fieldOrder will go down by one
            const upOrDownStep = name === "left" ? -1 : 1

            // Not all columns have to be in the columnVisibility object, you can list only the hidden ones and the
            // rest will be assumed as visible.
            const visibleColumns = table.getVisibleLeafColumns().map(column => column.id)
            const indexOfVariableToInMoveVisibleColumns = visibleColumns.findIndex(variable => variable === variableToMove)
            const fieldNameAtNewPositionInVisibleColumns = visibleColumns[indexOfVariableToInMoveVisibleColumns + upOrDownStep]
            const indexOfFieldNameAtNewPositionInAllColumns = columnOrder.findIndex(variable => variable === fieldNameAtNewPositionInVisibleColumns)

            // Now swap the fieldOrders
            if (indexOfFieldNameAtNewPositionInAllColumns > 0 && indexOfFieldNameAtNewPositionInAllColumns > 0) {

                const newColumnOrder = columnOrder.map((fieldName, i) => {

                    if (i === oldColumnOrderIndex) {
                        return fieldNameAtNewPositionInVisibleColumns
                    } else if (i === indexOfFieldNameAtNewPositionInAllColumns) {
                        return variableToMove
                    } else {
                        return fieldName
                    }
                })

                table.setColumnOrder(newColumnOrder)
            }

        } else {
            throw new TypeError(`The selected variable index was not found or variableToMove was not a string, this is not allowed`)
        }

    }, [table])

    /**
     * A hook that runs when the user changes the order of the columns outside the component, this can occur when the user is
     * editing a schema in the SchemaEditor component, and they change the order, we need the table and the schema editor to
     * be in sync. This also checks if the hidden columns needs updating, if the user changes the schema then they may
     * have hidden columns specified that now in the data that need to be hidden. This happens for example when changing
     * between searching for jobs to searching for models in the FindInTrac component.
     */
    useEffect(() => {

        // The names of the fields in their order to show
        const schemaOrder = sortArrayBy(schema, "fieldOrder").map(field => field.fieldName).filter(isDefined)

        // Get the position of the edited variable in the columns, this is for all columns not just those visible
        const columnOrder = table.getAllLeafColumns().map(column => column.id)

        // If the intended order and the current order are different update the table
        if (!arraysOfPrimitiveValuesEqualInOrder(schemaOrder, columnOrder)) {
            console.log("LOG :: Updating table column order")
            table.setColumnOrder(schemaOrder)
        }

        // Get the initial set of hidden columns as if the schema had been the one used from the start
        const initialHiddenColumns = setInitialHiddenColumn(schema, tag)

        // But if the user had changed any of the visibility statuses recapture those changes
        const currentColumnVisibility = table.getState().columnVisibility

        // Merge the intended column visibility with the current value, this means that if
        // the user has changed the visibility since the table loaded we retain their new
        // settings
        const newHiddenColumns = {...initialHiddenColumns, ...currentColumnVisibility}

        // However, the merge means we may have columns from currentColumnVisibility that are not
        // in the schema, so delete those that we have that are not in the schema
        Object.keys(newHiddenColumns).forEach(column => {

            if (!columnOrder.includes(column)) {
                delete newHiddenColumns[column]
            }
        })

        // Check whether we need to update the column visibility due a change being identified
        if (!objectsEqual(currentColumnVisibility, newHiddenColumns)) {
            console.log("LOG :: Updating table column visibility")
            table.setColumnVisibility(newHiddenColumns)
        }

    }, [schema, table, tag])

    return (

        <div className={className}>

            {showHeader &&
                <Header headerComponent={headerComponent}
                        globalFilterValue={table.getState().globalFilter}
                        pageSizeOptions={pageSizeOptions}
                        paginate={tracUiState.paginate}
                        showGlobalFilter={Boolean(!restrictedFunctionality && tracUiState.showGlobalFilter)}
                        table={table}
                >
                    {data.length > 0 ?
                        <Toolbar columnVisibility={table.getState().columnVisibility}
                                 data={data}
                                 disabled={false}
                                 handleCsvExport={handleCsvExport}
                                 handleXlsxExport={handleXlsxExport}
                                 initialSort={!restrictedFunctionality ? initialSort.filter(sort => schema.map(variable => variable.fieldName).includes(sort.id)) : undefined}
                                 setTracUiState={setTracUiState}
                                 showColumnFilters={tracUiState.showColumnFilters}
                                 showColumnOrder={tracUiState.showColumnOrder}
                                 showExportButtons={showExportButtons}
                                 showGlobalFilter={tracUiState.showGlobalFilter}
                                 table={table}
                                 columnColours={tracUiState.columnColours}
                                 isEditable={isEditable}
                                 importDataFunction={importDataFunction}
                                 restrictedFunctionality={restrictedFunctionality}
                                 schema={schema}
                                 storeKey={storeKey}
                            // Even if we pass in a tag as a prop this could be made up as in the FindInTrac scene
                            // we only want to show the metadata for real tags
                                 tag={isTracData ? tag : undefined}
                        /> : null}
                </Header>
            }

            {/*The table-top-and-bottom-border class is needed to add a border to the table at the top & bottom when it is sticky and*/}
            {/*scrollable. Also, We only have to add the tableContainerRef ref when we want to use virtualization, otherwise it will always
            be on.*/}
            <div ref={!tracUiState.paginate && rows.length > numberOfRowsForVirtualization ? tableContainerRef : undefined}
                 className={`mt-3 ${!tracUiState.paginate ? "table-top-and-bottom-border" : ""} mw-100 overflow-x-auto ${!tracUiState.paginate ? "overflow-y-auto" : ""}`}
                 style={{maxHeight: !tracUiState.paginate ? tracUiState.height : ""}}
            >
                <table className={`dataHtmlTable w-100 ${!tracUiState.paginate ? "dataHtmlTable-sticky-top" : ""}`}>
                    <thead className={`${!tracUiState.paginate ? "sticky-top" : ""}`}>
                    {table.getHeaderGroups().map(headerGroup => (
                        <tr key={headerGroup.id} ref={tableHeaderRow}>
                            {headerGroup.headers.map(header => {
                                return (
                                    <th key={header.id} {...{
                                        // Numbers are right aligned, strings are left aligned
                                        className: `${getTextAlignment(header.column.columnDef.meta?.schema.fieldType)} ${header.column.getCanSort() ? 'pointer user-select-none' : ""}`,
                                        onClick: header.column.getToggleSortingHandler(),
                                        style: {
                                            ...headerStyle,
                                            height: tableHeaderRow ? tracUiState.rowHeights.head : undefined
                                        }
                                    }}>
                                        {header.isPlaceholder ? null : (
                                            // To make the up and down arrow show when not sorted replace null with 'none'
                                            <div className={`sort ${header.column.getIsSorted() || null}`}>
                                                {/*This is the value for the column as a component*/}
                                                {flexRender(header.column.columnDef.header, header.getContext())}
                                            </div>
                                        )}
                                    </th>
                                )
                            })}
                        </tr>
                    ))}
                    </thead>
                    <tbody>
                    {/*This maps over all the visible leaf columns, i.e. the lowest level of the column groups*/}
                    {tracUiState.showColumnOrder &&
                        <tr ref={tableColumnOrderingRow}
                            className={`widgets ${!tracUiState.paginate ? "sticky-top" : ""}`} {...{
                            style: {
                                top: tracUiState.rowHeights.head,
                                height: tableColumnOrderingRow ? tracUiState.rowHeights.columnOrder : undefined
                            }
                        }}>
                            {table.getVisibleLeafColumns().map(leafColumn => {
                                return (
                                    <td key={leafColumn.id}>
                                        <div
                                            className={`order ${getDivAlignment(leafColumn.columnDef.meta?.schema.fieldType)}`}>
                                            <Button ariaLabel={"Move column left"}
                                                    className={`m-0 p-0 fs-5 link-secondary no-halo`}
                                                    id={leafColumn.id}
                                                    name={"left"}
                                                    variant={"link"}
                                                    onClick={handleOrderChange}
                                                    isDispatched={false}
                                            >
                                                <Icon ariaLabel={false}
                                                      icon={"bi-arrow-left-square-fill"}
                                                />
                                            </Button>

                                            <Button ariaLabel={"Move column right"}
                                                    className={"m-0 p-0 fs-5 link-secondary no-halo"}
                                                    id={leafColumn.id}
                                                    name={"right"}
                                                    variant={"link"}
                                                    onClick={handleOrderChange}
                                                    isDispatched={false}
                                            >
                                                <Icon ariaLabel={false}
                                                      icon={"bi-arrow-right-square-fill"}
                                                />
                                            </Button>
                                        </div>
                                    </td>)
                            })}
                        </tr>
                    }

                    {tracUiState.showColumnFilters &&
                        <tr ref={tableColumnFilterRow}
                            className={`widgets ${!tracUiState.paginate ? "sticky-top" : ""}`} {...{
                            style: {
                                top: tracUiState.rowHeights.head + tracUiState.rowHeights.columnOrder,
                                height: tableColumnFilterRow ? tracUiState.rowHeights.columnFilter : undefined
                            }
                        }}>
                            {table.getVisibleLeafColumns().map(leafColumn =>
                                // Note that the css has default padding and borders defined for table but here we override it
                                <td key={leafColumn.id} className={"border-bottom-0 pb-0"}>
                                    {leafColumn.getCanFilter() && (
                                        <ColumnFilters table={table} column={leafColumn}/>
                                    )}
                                </td>
                            )}
                        </tr>
                    }

                    {tracUiState.showColumnFilters &&
                        <tr className={`widgets ${!tracUiState.paginate ? "sticky-top" : ""}`} {...{
                            style: {
                                top: tracUiState.rowHeights.head + tracUiState.rowHeights.columnOrder + tracUiState.rowHeights.columnFilter,
                            }
                        }}>
                            {table.getVisibleLeafColumns().map(leafColumn => {
                                return (
                                    // Note that the css has default padding defined for table but here we override it
                                    <td key={leafColumn.id}
                                        className={`${table.getState().columnFilters.length > 0 ? "pt-1" : "pt-0"}`}>
                                        <div className={"d-flex flex-column"}>
                                            {leafColumn.getIsFiltered() &&
                                                <Button ariaLabel={"Clear filter"}
                                                        className={"px-4 p-0 align-self-center"}
                                                        size={"sm"}
                                                        id={leafColumn.id}
                                                        onClick={handleClearColumnFilter}
                                                        isDispatched={false}
                                                >
                                                    Clear filter
                                                </Button>
                                            }
                                        </div>
                                    </td>
                                )
                            })}
                        </tr>
                    }
                    {/*Show a message if there are no rows*/}
                    {data.length === 0 && <tr>
                        <td colSpan={table.getVisibleLeafColumns().length}>{noDataMessage}</td>
                    </tr>}

                    {rows.length <= numberOfRowsForVirtualization &&
                        table.getRowModel().rows.map(row =>

                            <Row columnColours={tracUiState.columnColours}
                                 getTextAlignment={getTextAlignment}
                                 getToggleSelectedHandler={row.getToggleSelectedHandler}
                                 visibleCells={row.getVisibleCells()}
                                 index={row.index}
                                 isSelected={row.getIsSelected()}
                                 key={row.id}
                                 noWrapColumns={noWrapColumns}
                            />
                        )
                    }

                    {rows.length > numberOfRowsForVirtualization &&
                        <React.Fragment>
                            {paddingTop > 0 && (
                                <tr>
                                    <td style={{height: `${paddingTop}px`}}/>
                                </tr>
                            )}

                            {virtualRows.map(virtualRow => {

                                const row = rows[virtualRow.index]
                                return (
                                    <Row columnColours={tracUiState.columnColours}
                                         getTextAlignment={getTextAlignment}
                                         getToggleSelectedHandler={row.getToggleSelectedHandler}
                                         visibleCells={row.getVisibleCells()}
                                         isSelected={row.getIsSelected()}
                                         key={row.id}
                                         index={virtualRow.index}
                                         noWrapColumns={noWrapColumns}
                                         measureElement={rowVirtualizer.measureElement}
                                    />
                                )
                            })}

                            {paddingBottom > 0 && (
                                <tr>
                                    <td style={{height: `${paddingBottom}px`}}/>
                                </tr>
                            )}

                        </React.Fragment>
                    }
                    </tbody>
                </table>
            </div>

            <Footer footerComponent={footerComponent}
                    paginate={tracUiState.paginate}
                    showInformation={tracUiState.showInformation}
                    table={table}
            />
        </div>
    )
};

Table.propTypes = {
    badgeColumns: PropTypes.arrayOf(PropTypes.string),
    badgeColours: PropTypes.objectOf(PropTypes.string),
    className: PropTypes.string,
    data: PropTypes.arrayOf(PropTypes.objectOf(PropTypes.oneOfType([PropTypes.number, PropTypes.string, PropTypes.bool, PropTypes.arrayOf(PropTypes.string)]))).isRequired,
    footerComponent: PropTypes.element,
    getSelectedRowsFunction: PropTypes.func,
    headerComponent: PropTypes.element,
    id: PropTypes.string,
    importDataFunction: PropTypes.func,
    initialHeight: PropTypes.string,
    initialPageSize: PropTypes.number,
    initialPaginate: PropTypes.bool,
    initialShowGlobalFilter: PropTypes.bool,
    initialShowInformation: PropTypes.bool,
    initialSort: PropTypes.arrayOf(PropTypes.shape({
        id: PropTypes.string.isRequired,
        desc: PropTypes.bool.isRequired
    })),
    isEditable: PropTypes.bool,
    isTracData: PropTypes.bool.isRequired,
    noDataMessage: PropTypes.string,
    noOfRowsSelectable: PropTypes.number,
    noWrapColumns: PropTypes.arrayOf(PropTypes.string),
    pageSizeOptions: PropTypes.arrayOf(PropTypes.number),
    schema: PropTypes.arrayOf(PropTypes.shape({
        fieldName: PropTypes.string.isRequired,
        fieldType: PropTypes.number.isRequired,
        fieldOrder: PropTypes.number,
        label: PropTypes.string,
        categorical: PropTypes.bool,
        businessKey: PropTypes.bool,
    })).isRequired,
    showExportButtons: PropTypes.bool,
    storeKey: PropTypes.string,
    tag: PropTypes.shape({
        header: PropTypes.object.isRequired,
        attrs: PropTypes.object.isRequired
    }),
    savedTableState: PropTypes.shape({
        tracUiState: PropTypes.object,
        reactTableState: PropTypes.object
    }),
    saveTableStateFunction: PropTypes.func
};